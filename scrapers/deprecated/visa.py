"""
scrapers/visa.py — Visa Careers Scraper
=======================================
Scrapes Bangalore jobs from the backend API that powers Visa's careers page.

How it works:
  - Public URL: https://corporate.visa.com/en/jobs/?cities=Bangalore
  - Backend API: https://search.visa.com/CAREERS/careers/jobs?q=

The API accepts POST requests with a JSON body and returns paginated job
listings with descriptions inline. This first version is intentionally
Bangalore-only and sends the city filter in the POST body.
"""

import html
import logging
import re
from datetime import datetime, timezone

import httpx

from scrapers.base import BaseScraper, JobPosting, generate_fallback_id

logger = logging.getLogger(__name__)

_API_URL = "https://search.visa.com/CAREERS/careers/jobs?q="
_CITY = "Bangalore"
_PAGE_SIZE = 1000


def _strip_html(raw_html: str) -> str:
    """Convert HTML fragments to plain text."""
    clean = re.sub(r"<[^>]+>", " ", raw_html)
    clean = html.unescape(clean)
    clean = re.sub(r"\s+", " ", clean)
    return clean.strip()


def _parse_age_days(posted_date: str, now: datetime) -> int | None:
    """Parse Visa's ISO timestamp into number of days ago."""
    if not posted_date:
        return None

    normalized = posted_date.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (now - dt.astimezone(timezone.utc)).days


class VisaScraper(BaseScraper):
    """Scrapes Visa careers using the backend jobs API."""

    def __init__(
        self,
        max_age_days: int | None = None,
        page_size: int = _PAGE_SIZE,
        client: httpx.Client | None = None,
        **kwargs,
    ):
        self._max_age_days = max_age_days
        self._page_size = page_size
        self._client = client or httpx.Client(
            timeout=30,
            verify=False,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/125.0.0.0 Safari/537.36"
                ),
            },
            follow_redirects=True,
        )

    def scrape(self, url: str) -> list[JobPosting]:
        """Fetch all Bangalore jobs from Visa's backend API."""
        offset = 0
        all_jobs: list[JobPosting] = []
        records_matched = 0

        while True:
            jobs_batch, records_matched, page_size = self._fetch_page(offset)
            if not jobs_batch:
                break

            all_jobs.extend(jobs_batch)
            offset += page_size
            if offset >= records_matched:
                break

        before_filter = len(all_jobs)
        if self._max_age_days is not None:
            now = datetime.now(timezone.utc)
            filtered = []
            for job in all_jobs:
                days_ago = _parse_age_days(job.posted_date, now)
                if days_ago is None or days_ago <= self._max_age_days:
                    filtered.append(job)
            all_jobs = filtered
            skipped = before_filter - len(all_jobs)
            logger.info(
                "Visa: %d total hits, %d within %d days, %d older (skipped)",
                records_matched,
                len(all_jobs),
                self._max_age_days,
                skipped,
            )
        else:
            logger.info("Visa: %d total hits, fetched %d", records_matched, len(all_jobs))

        return all_jobs

    def _fetch_page(self, offset: int) -> tuple[list[JobPosting], int, int]:
        payload = {
            "city": [_CITY],
            "from": offset,
            "size": self._page_size,
        }
        try:
            resp = self._client.post(_API_URL, json=payload)
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPError as e:
            logger.error("Visa API request failed: %s", e)
            return [], 0, self._page_size
        except ValueError:
            logger.error("Visa API returned non-JSON response (status %d)", resp.status_code)
            return [], 0, self._page_size

        records_matched = data.get("recordsMatched", 0)
        raw_jobs = data.get("jobDetails", [])
        page_size = data.get("pageSize") or len(raw_jobs) or self._page_size
        try:
            page_size = int(page_size)
        except (TypeError, ValueError):
            page_size = len(raw_jobs) or self._page_size
        jobs = [self._build_job(item) for item in raw_jobs]
        return jobs, records_matched, page_size

    def _build_job(self, item: dict) -> JobPosting:
        ref_number = item.get("refNumber", "")
        posting_id = item.get("postingId", "")
        title = item.get("jobTitle", "").strip()
        city = item.get("city", "")
        region = item.get("region", "")
        country = item.get("country", "")
        location = ", ".join(part for part in [city, region, country] if part)
        posted_date = item.get("createdOn", "")

        description_parts = [
            item.get("jobDescription", ""),
            item.get("qualifications", ""),
            item.get("additionalInformation", ""),
        ]
        description = _strip_html(" ".join(part for part in description_parts if part))
        job_id = str(
            ref_number
            or posting_id
            or generate_fallback_id(f"Visa:{title}:{item.get('applyUrl', '')}")
        )

        return JobPosting(
            job_id=job_id,
            title=title,
            company="Visa",
            location=location,
            description=description,
            url=item.get("applyUrl", ""),
            posted_date=posted_date,
            job_num=str(ref_number or ""),
        )

    def close(self):
        """Close the httpx client."""
        self._client.close()
