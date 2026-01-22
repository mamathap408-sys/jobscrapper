"""
scrapers/amazon.py — Amazon Jobs Scraper
=========================================
Scrapes job postings from Amazon's public career site (amazon.jobs) using their
JSON search API.

How it works:
  - Public URL: https://www.amazon.jobs/en/search?...
  - JSON API:   https://www.amazon.jobs/en/search.json?...

  The API accepts GET requests with query params and returns paginated job listings
  with full descriptions inline. No authentication needed.

Approach:
  1. Parse the URL's query params — user controls filters (country, location, radius)
     directly in urls.txt
  2. Paginate via `offset` param, using `result_limit` from URL params (default 100)
  3. Build JobPosting per job with inline descriptions (no separate detail fetch needed)
  4. Optionally filter by age using `posted_date` field

Example:
  URL: https://www.amazon.jobs/en/search.json?normalized_country_code[]=IND&loc_query=Bangalore+India&radius=24km&sort=recent&result_limit=100
"""

import logging
import re
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import httpx

from scrapers.base import BaseScraper, JobPosting

logger = logging.getLogger(__name__)


def _strip_html(html: str) -> str:
    """Remove HTML tags from a string."""
    clean = re.sub(r"<[^>]+>", " ", html)
    clean = re.sub(r"\s+", " ", clean)
    return clean.strip()


class AmazonScraper(BaseScraper):
    """Scrapes Amazon Jobs via their public JSON search API (no browser needed)."""

    def __init__(self, max_age_days: int | None = None, **kwargs):
        """Initialize with optional age filter.

        Args:
            max_age_days: Only include jobs posted within this many days.
                          None = no limit.
        """
        self._max_age_days = max_age_days
        self._client = httpx.Client(
            timeout=30,
            verify=False,
            headers={
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/125.0.0.0 Safari/537.36",
            },
            follow_redirects=True,
        )

    def scrape(self, url: str) -> list[JobPosting]:
        """Scrape all job postings from Amazon Jobs.

        Paginates through the JSON API using offset until all results are fetched.
        Descriptions come inline — no separate detail fetch needed.

        Args:
            url: The Amazon Jobs search.json URL with query params.

        Returns:
            List of JobPosting objects with full descriptions.
        """
        parsed = urlparse(url)
        params = parse_qs(parsed.query)

        result_limit = int(params.get("result_limit", [100])[0])
        offset = 0
        all_jobs = []

        while True:
            params["offset"] = [str(offset)]
            page_url = urlunparse(parsed._replace(query=urlencode(params, doseq=True)))

            jobs_batch, hits = self._fetch_page(page_url)
            all_jobs.extend(jobs_batch)

            offset += result_limit
            if offset >= hits or not jobs_batch:
                break

        # Filter by age
        before_filter = len(all_jobs)
        if self._max_age_days is not None:
            now = datetime.now(timezone.utc)
            filtered = []
            for job in all_jobs:
                days_ago = self._parse_age(job.posted_date, now)
                if days_ago is not None and days_ago <= self._max_age_days:
                    filtered.append(job)
                elif days_ago is None:
                    filtered.append(job)  # Keep jobs with unparseable dates
            all_jobs = filtered
            skipped = before_filter - len(all_jobs)
            logger.info(
                "Amazon: %d total hits, %d within %d days, %d older (skipped)",
                hits, len(all_jobs), self._max_age_days, skipped,
            )
        else:
            logger.info("Amazon: %d total hits, fetched %d", hits, len(all_jobs))

        return all_jobs

    def _fetch_page(self, url: str) -> tuple[list[JobPosting], int]:
        """Fetch one page of Amazon job results.

        Returns:
            Tuple of (jobs, total_hits).
        """
        try:
            resp = self._client.get(url)
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPError as e:
            logger.error("Amazon API request failed: %s", e)
            return [], 0
        except ValueError:
            logger.error("Amazon API returned non-JSON response (status %d)", resp.status_code)
            return [], 0

        hits = data.get("hits", 0)
        raw_jobs = data.get("jobs", [])
        jobs = []

        for item in raw_jobs:
            job_id = item.get("id_icims", "") or item.get("id", "")
            title = item.get("title", "")
            company = item.get("company_name", "Amazon")
            location = item.get("normalized_location", "") or item.get("location", "")
            posted_date = item.get("posted_date", "")

            # Combine description fields and strip HTML
            desc_parts = [
                item.get("description", ""),
                item.get("basic_qualifications", ""),
                item.get("preferred_qualifications", ""),
            ]
            description = _strip_html(" ".join(part for part in desc_parts if part))

            job_path = item.get("job_path", "")
            job_url = f"https://www.amazon.jobs{job_path}" if job_path else ""

            jobs.append(JobPosting(
                job_id=str(job_id),
                title=title,
                company=company,
                location=location,
                description=description,
                url=job_url,
                posted_date=posted_date,
            ))

        return jobs, hits

    @staticmethod
    def _parse_age(posted_date: str, now: datetime) -> int | None:
        """Parse Amazon's posted_date string and return days ago.

        Amazon uses format like "April 10, 2026".

        Returns:
            Number of days ago, or None if unparseable.
        """
        if not posted_date:
            return None
        try:
            dt = datetime.strptime(posted_date, "%B %d, %Y").replace(tzinfo=timezone.utc)
            return (now - dt).days
        except ValueError:
            return None

    def close(self):
        """Close the httpx client."""
        self._client.close()
