"""
scrapers/ashby.py — Ashby Job Board Scraper
===========================================
Scrapes public Ashby job boards via Ashby's job posting API.

How it works:
  - Public boards live at jobs.ashbyhq.com/{organization}
  - API: https://api.ashbyhq.com/posting-api/job-board/{organization}
  - No authentication needed for public job boards

The scraper filters internally to target India cities by default:
  - Bengaluru / Bangalore
  - Hyderabad

Optional client-side filters can be provided in the board URL query string:
  - team=engineering
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

_API_BASE = "https://api.ashbyhq.com/posting-api/job-board"
_DEFAULT_CITY_ALIASES = {
    "bengaluru",
    "bangalore",
    "hyderabad",
}


def _strip_html(html: str) -> str:
    """Remove HTML tags from a string."""
    clean = re.sub(r"<[^>]+>", " ", html or "")
    clean = re.sub(r"\s+", " ", clean)
    return clean.strip()


def _normalize_company(token: str) -> str:
    """Turn an Ashby organization token into a readable fallback company name."""
    text = token.replace("-", " ").replace("_", " ").strip()
    return " ".join(part.capitalize() for part in text.split()) or token


class AshbyScraper(BaseScraper):
    """Scrapes Ashby job boards via the public posting API."""

    def __init__(self, cities: list[str] | None = None, max_age_days: int | None = None, **kwargs):
        self._cities = {city.strip().lower() for city in (cities or sorted(_DEFAULT_CITY_ALIASES)) if city.strip()}
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
        """Fetch all public jobs for an Ashby board URL."""
        organization = self._extract_organization(url)
        filters = self._build_filters(url)
        api_url = f"{_API_BASE}/{organization}"

        try:
            resp = self._client.get(api_url)
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPError as e:
            logger.error("Ashby API request failed for %s: %s", api_url, e)
            return []
        except ValueError:
            logger.error("Ashby API returned non-JSON response (status %d)", resp.status_code)
            return []

        jobs_data = []
        if isinstance(data, dict):
            jobs_data = data.get("jobs") or data.get("jobPostings") or []

        company = self._extract_company(data, organization)
        jobs = []
        now = datetime.now(timezone.utc)
        for item in jobs_data:
            job = self._parse_job(item, company)
            if not job:
                continue
            if not self._matches_target_cities(job):
                continue
            if not self._matches_filters(job, item, filters):
                continue
            if self._max_age_days is not None and not self._within_age(job.posted_date, now):
                continue
            jobs.append(job)

        logger.info("Ashby/%s: %d jobs", organization, len(jobs))
        return jobs

    @staticmethod
    def _extract_organization(url: str) -> str:
        """Extract the Ashby organization token from a board URL."""
        parsed = urlparse(url)
        parts = [part for part in parsed.path.split("/") if part]
        if not parts:
            raise ValueError(f"Could not extract Ashby organization from {url}")
        return parts[0]

    @staticmethod
    def _build_filters(url: str) -> dict[str, str]:
        """Extract supported client-side filters from the URL query string."""
        params = parse_qs(urlparse(url).query)
        return {
            "team": params.get("team", [""])[0].strip().lower(),
            "department": params.get("department", [""])[0].strip().lower(),
            "keyword": params.get("keyword", [""])[0].strip().lower(),
        }

    @staticmethod
    def _extract_company(data: dict, organization: str) -> str:
        """Prefer board metadata for the company name, else fall back to the token."""
        if isinstance(data, dict):
            for key in ("companyName", "name"):
                value = data.get(key)
                if value:
                    return str(value)
            board = data.get("jobBoard")
            if isinstance(board, dict):
                for key in ("companyName", "name", "title"):
                    value = board.get(key)
                    if value:
                        return str(value)
        return _normalize_company(organization)

    def _parse_job(self, item: dict, company: str) -> JobPosting | None:
        """Parse an Ashby API job item into a JobPosting."""
        job_id = str(
            item.get("id")
            or item.get("jobPostingId")
            or item.get("slug")
            or ""
        ).strip()
        title = str(item.get("title", "")).strip()
        if not job_id or not title:
            return None

        location = self._extract_location(item)
        description = self._extract_description(item)
        posted_date = self._parse_iso_date(
            str(
                item.get("publishedDate")
                or item.get("postedDate")
                or item.get("updatedAt")
                or item.get("createdAt")
                or ""
            )
        )
        job_url = str(
            item.get("jobUrl")
            or item.get("jobPostingUrl")
            or item.get("url")
            or ""
        ).strip()

        team = self._extract_nested_name(item.get("team"))
        department = self._extract_nested_name(item.get("department")) or str(item.get("departmentName", "")).strip()
        job_num = str(item.get("jobPostingId") or "").strip()

        if not description:
            metadata_bits = []
            if team:
                metadata_bits.append(f"Team: {team}")
            if department:
                metadata_bits.append(f"Department: {department}")
            employment_type = str(item.get("employmentType", "")).strip()
            if employment_type:
                metadata_bits.append(f"Type: {employment_type}")
            description = "\n".join(metadata_bits)

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
    def _extract_nested_name(value) -> str:
        """Extract a name/title string from nested Ashby objects."""
        if isinstance(value, dict):
            for key in ("name", "title", "label"):
                nested = value.get(key)
                if nested:
                    return str(nested).strip()
        elif value:
            return str(value).strip()
        return ""

    def _extract_location(self, item: dict) -> str:
        """Extract the best available location string from an Ashby job item."""
        location = self._extract_nested_name(item.get("location"))
        if location:
            secondary = item.get("secondaryLocations") or []
            if isinstance(secondary, list) and secondary:
                extras = [
                    self._extract_nested_name(entry)
                    for entry in secondary
                    if self._extract_nested_name(entry)
                ]
                if extras:
                    return " | ".join([location, *extras])
            return location
        return str(item.get("locationName", "")).strip()

    def _extract_description(self, item: dict) -> str:
        """Extract description text from the most useful Ashby field."""
        for key in ("descriptionPlain", "description", "descriptionHtml", "content"):
            value = item.get(key)
            if value:
                text = str(value).strip()
                return _strip_html(text) if "<" in text else text
        return ""

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
        """Apply team/department/keyword filters from the board URL."""
        if filters["team"]:
            team = self._extract_nested_name(raw_item.get("team")).lower()
            if filters["team"] not in team:
                return False

        if filters["department"]:
            department = (
                self._extract_nested_name(raw_item.get("department"))
                or str(raw_item.get("departmentName", ""))
            ).lower()
            if filters["department"] not in department:
                return False

        if filters["keyword"]:
            haystack = " ".join(
                part for part in (job.title, job.location, job.description) if part
            ).lower()
            if filters["keyword"] not in haystack:
                return False

        return True

    def _matches_target_cities(self, job: JobPosting) -> bool:
        """Keep target-city jobs plus remote/unspecified jobs."""
        haystack = (job.location or "").lower()
        if not haystack.strip():
            return True
        return any(city in haystack for city in self._cities) or "remote" in haystack

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
