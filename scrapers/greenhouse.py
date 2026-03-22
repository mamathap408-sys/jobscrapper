"""
scrapers/greenhouse.py — Greenhouse Job Board Scraper
=====================================================
Scrapes public Greenhouse job boards via the Job Board API.

How it works:
  - Public boards live at boards.greenhouse.io or job-boards.greenhouse.io
  - API: https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs?content=true
  - No authentication needed for public boards

The scraper accepts a board URL in urls.yaml and optional query params that are
used as client-side filters:
  - location=bangalore
  - department=engineering
  - keyword=python
"""

import logging
import re
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

import httpx

from scrapers.base import BaseScraper, JobPosting

logger = logging.getLogger(__name__)

_API_BASE = "https://boards-api.greenhouse.io/v1/boards"


def _strip_html(html: str) -> str:
    """Remove HTML tags from a string."""
    clean = re.sub(r"<[^>]+>", " ", html or "")
    clean = re.sub(r"\s+", " ", clean)
    return clean.strip()


def _normalize_company(token: str) -> str:
    """Turn a board token into a readable fallback company name."""
    text = token.replace("-", " ").replace("_", " ").strip()
    return " ".join(part.capitalize() for part in text.split()) or token


class GreenhouseScraper(BaseScraper):
    """Scrapes Greenhouse job boards via the public Job Board API."""

    def __init__(self, max_age_days: int | None = None, **kwargs):
        self._max_age_days = max_age_days
        self._client = httpx.Client(
            timeout=30,
            verify=False,
            headers={
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
        """Fetch all public jobs for a Greenhouse board URL."""
        board_token = self._extract_board_token(url)
        filters = self._build_filters(url)
        api_url = f"{_API_BASE}/{board_token}/jobs?content=true"

        try:
            resp = self._client.get(api_url)
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPError as e:
            logger.error("Greenhouse API request failed for %s: %s", api_url, e)
            return []
        except ValueError:
            logger.error("Greenhouse API returned non-JSON response (status %d)", resp.status_code)
            return []

        items = data.get("jobs", []) if isinstance(data, dict) else []
        company = self._extract_company(data, board_token)

        jobs = []
        now = datetime.now(timezone.utc)
        for item in items:
            job = self._parse_job(item, company)
            if not job:
                continue
            if not self._matches_filters(job, item, filters):
                continue
            if self._max_age_days is not None and not self._within_age(job.posted_date, now):
                continue
            jobs.append(job)

        logger.info("Greenhouse/%s: %d jobs", board_token, len(jobs))
        return jobs

    @staticmethod
    def _extract_board_token(url: str) -> str:
        """Extract the Greenhouse board token from a board URL."""
        parsed = urlparse(url)
        parts = [part for part in parsed.path.split("/") if part]
        if not parts:
            raise ValueError(f"Could not extract Greenhouse board token from {url}")
        return parts[0]

    @staticmethod
    def _build_filters(url: str) -> dict[str, str]:
        """Extract supported client-side filters from the URL query string."""
        params = parse_qs(urlparse(url).query)
        return {
            "location": params.get("location", [""])[0].strip().lower(),
            "department": params.get("department", [""])[0].strip().lower(),
            "keyword": params.get("keyword", [""])[0].strip().lower(),
        }

    @staticmethod
    def _extract_company(data: dict, board_token: str) -> str:
        """Prefer API metadata for the company name, else fall back to the token."""
        board = data.get("board") if isinstance(data, dict) else None
        if isinstance(board, dict):
            for key in ("name", "company_name", "title"):
                value = board.get(key)
                if value:
                    return str(value)
        meta = data.get("meta") if isinstance(data, dict) else None
        if isinstance(meta, dict):
            for key in ("board_name", "company_name"):
                value = meta.get(key)
                if value:
                    return str(value)
        return _normalize_company(board_token)

    def _parse_job(self, item: dict, company: str) -> JobPosting | None:
        """Parse a Greenhouse API job item into a JobPosting."""
        job_id = str(item.get("id", "")).strip()
        title = str(item.get("title", "")).strip()
        if not job_id or not title:
            return None

        location = ""
        raw_location = item.get("location")
        if isinstance(raw_location, dict):
            location = str(raw_location.get("name", "")).strip()
        elif raw_location:
            location = str(raw_location).strip()

        description = _strip_html(str(item.get("content", "")))
        job_url = str(item.get("absolute_url", "")).strip()
        posted_date = self._parse_iso_date(
            str(item.get("updated_at") or item.get("created_at") or "")
        )

        job_num = ""
        metadata = item.get("metadata")
        if isinstance(metadata, list):
            for entry in metadata:
                if not isinstance(entry, dict):
                    continue
                name = str(entry.get("name", "")).lower()
                value = entry.get("value")
                if value and any(term in name for term in ("req", "requisition", "job id")):
                    job_num = str(value).strip()
                    break

        return JobPosting(
            job_id=job_id,
            title=title,
            company=company,
            location=location,
            description=description,
            url=job_url,
            posted_date=posted_date,
            job_num=job_num,
        )

    @staticmethod
    def _parse_iso_date(value: str) -> str:
        """Parse an ISO date/time string to YYYY-MM-DD."""
        if not value:
            return ""
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return ""

    def _matches_filters(self, job: JobPosting, raw_item: dict, filters: dict[str, str]) -> bool:
        """Apply location/department/keyword filters from the board URL."""
        if filters["location"] and filters["location"] not in (job.location or "").lower():
            return False

        if filters["department"]:
            departments = raw_item.get("departments") or []
            department_text = " ".join(
                str(dept.get("name", ""))
                for dept in departments
                if isinstance(dept, dict)
            ).lower()
            if filters["department"] not in department_text:
                return False

        if filters["keyword"]:
            haystack = " ".join(
                part for part in (job.title, job.location, job.description) if part
            ).lower()
            if filters["keyword"] not in haystack:
                return False

        return True

    def _within_age(self, posted_date: str, now: datetime) -> bool:
        """Check if a posted date is within max_age_days."""
        if not posted_date or self._max_age_days is None:
            return True
        try:
            dt = datetime.strptime(posted_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            return (now - dt).days <= self._max_age_days
        except ValueError:
            return True

    def close(self):
        """Close the shared HTTP client."""
        self._client.close()
