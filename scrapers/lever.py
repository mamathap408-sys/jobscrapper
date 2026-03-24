"""
scrapers/lever.py — Lever Job Board Scraper
===========================================
Scrapes public Lever job boards via Lever's postings API.

How it works:
  - Public boards live at jobs.lever.co/{site}
  - API: https://api.lever.co/v0/postings/{site}?mode=json
  - No authentication needed for public postings

The scraper filters internally to target India cities by default:
  - Bengaluru / Bangalore
  - Hyderabad

Optional client-side filters can be provided in the board URL query string:
  - team=engineering
  - commitment=full-time
  - keyword=python
"""

import logging
import re
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

import httpx

from scrapers.base import BaseScraper, JobPosting

logger = logging.getLogger(__name__)

_API_BASE = "https://api.lever.co/v0/postings"
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
    """Turn a Lever site token into a readable fallback company name."""
    text = token.replace("-", " ").replace("_", " ").strip()
    return " ".join(part.capitalize() for part in text.split()) or token


class LeverScraper(BaseScraper):
    """Scrapes Lever job boards via the public postings API."""

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
        """Fetch all public jobs for a Lever board URL."""
        site = self._extract_site(url)
        filters = self._build_filters(url)
        api_url = f"{_API_BASE}/{site}?mode=json"

        try:
            resp = self._client.get(api_url)
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPError as e:
            logger.error("Lever API request failed for %s: %s", api_url, e)
            return []
        except ValueError:
            logger.error("Lever API returned non-JSON response (status %d)", resp.status_code)
            return []

        company = _normalize_company(site)
        jobs = []
        now = datetime.now(timezone.utc)
        for item in data if isinstance(data, list) else []:
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

        logger.info("Lever/%s: %d jobs", site, len(jobs))
        return jobs

    @staticmethod
    def _extract_site(url: str) -> str:
        """Extract the Lever site token from a board URL."""
        parsed = urlparse(url)
        parts = [part for part in parsed.path.split("/") if part]
        if not parts:
            raise ValueError(f"Could not extract Lever site from {url}")
        return parts[0]

    @staticmethod
    def _build_filters(url: str) -> dict[str, str]:
        """Extract supported client-side filters from the URL query string."""
        params = parse_qs(urlparse(url).query)
        return {
            "team": params.get("team", [""])[0].strip().lower(),
            "commitment": params.get("commitment", [""])[0].strip().lower(),
            "keyword": params.get("keyword", [""])[0].strip().lower(),
        }

    def _parse_job(self, item: dict, company: str) -> JobPosting | None:
        """Parse a Lever API job item into a JobPosting."""
        job_id = str(item.get("id", "")).strip()
        title = str(item.get("text", "")).strip()
        if not job_id or not title:
            return None

        categories = item.get("categories") if isinstance(item.get("categories"), dict) else {}
        all_locations = categories.get("allLocations")
        if isinstance(all_locations, list) and all_locations:
            location = " | ".join(str(value).strip() for value in all_locations if value)
        else:
            location = str(categories.get("location", "")).strip()

        desc_parts = []
        plain = str(item.get("descriptionPlain", "")).strip()
        html = str(item.get("description", "")).strip()
        if plain:
            desc_parts.append(plain)
        elif html:
            desc_parts.append(_strip_html(html))

        for section in item.get("lists") or []:
            if not isinstance(section, dict):
                continue
            heading = str(section.get("text", "")).strip()
            content = _strip_html(str(section.get("content", "")).strip())
            if heading and content:
                desc_parts.append(f"{heading}: {content}")
            elif content:
                desc_parts.append(content)

        description = "\n".join(part for part in desc_parts if part)
        job_url = str(item.get("hostedUrl") or item.get("applyUrl") or "").strip()
        posted_date = self._parse_timestamp(item.get("createdAt") or item.get("updatedAt"))

        return JobPosting(
            job_id=job_id,
            title=title,
            company=company,
            location=location,
            description=description,
            url=job_url,
            posted_date=posted_date,
            job_num=job_id,
        )

    @staticmethod
    def _parse_timestamp(value) -> str:
        """Parse Lever timestamps (usually milliseconds since epoch) to YYYY-MM-DD."""
        if value in (None, ""):
            return ""
        try:
            ts = int(value)
            if ts > 10_000_000_000:
                ts = ts / 1000
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            return dt.strftime("%Y-%m-%d")
        except (TypeError, ValueError, OSError):
            return ""

    def _matches_filters(self, job: JobPosting, raw_item: dict, filters: dict[str, str]) -> bool:
        """Apply team/commitment/keyword filters from the board URL."""
        categories = raw_item.get("categories") if isinstance(raw_item.get("categories"), dict) else {}

        if filters["team"]:
            if filters["team"] not in str(categories.get("team", "")).lower():
                return False

        if filters["commitment"]:
            if filters["commitment"] not in str(categories.get("commitment", "")).lower():
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
