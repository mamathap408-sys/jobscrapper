"""
scrapers/base.py — Base Scraper Interface
==========================================
Defines the standard data format (JobPosting) and the abstract interface (BaseScraper)
that all scrapers must implement.

Every scraper (Workday, generic, or custom) must:
  1. Inherit from BaseScraper
  2. Implement the scrape(url) method
  3. Return a list of JobPosting objects

This ensures all scrapers produce consistent output that the matcher, database,
and notifier can work with regardless of where the jobs came from.
"""

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx

_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)


@dataclass
class JobPosting:
    """Represents a single job posting scraped from a career portal.

    Attributes:
        job_id:      Unique identifier for deduplication.
                     From the site's API (e.g., Workday externalPath) or a hash of the URL.
        title:       Job title, e.g., "Senior Backend Engineer"
        company:     Company name, extracted from the portal URL or API response.
        location:    Job location, e.g., "Bangalore, India" or "Remote". Empty if unknown.
        description: Full job description text (HTML tags stripped).
                     Used by the GenAI matcher to score relevance.
                     May be empty initially and filled in later (e.g., Workday detail fetch).
        url:         Direct link to the job posting page (for the email digest).
        posted_date: When the job was posted, e.g., "2024-01-15". Empty if unavailable.
        job_num:     Job number / requisition ID from the portal (e.g., "R1308276"). Empty if unavailable.
        scraper:     The scraper type this job came from (e.g. "workday"). Tagged by
                     the watcher after scraping; used to gate per-source behavior
                     such as resume generation. Empty if not set.
    """
    job_id: str
    title: str
    company: str
    location: str
    description: str
    url: str
    posted_date: str
    job_num: str = ""
    scraper: str = ""


def generate_fallback_id(text: str) -> str:
    """Generate a stable fallback job ID from arbitrary text using MD5."""
    return hashlib.md5(text.encode()).hexdigest()


def strip_html(html: str) -> str:
    """Remove HTML tags from a string and collapse whitespace."""
    clean = re.sub(r"<[^>]+>", " ", html or "")
    clean = re.sub(r"\s+", " ", clean)
    return clean.strip()


def normalize_company(token: str) -> str:
    """Turn a board/site token (e.g. "acme-corp") into a readable company name."""
    text = token.replace("-", " ").replace("_", " ").strip()
    return " ".join(part.capitalize() for part in text.split()) or token


class BaseScraper:
    """Abstract base class for all site scrapers.

    Owns the shared httpx client and the max_age_days filter.

    To add a new scraper:
      1. Create a new file in scrapers/ (e.g., scrapers/greenhouse.py)
      2. Create a class that inherits from BaseScraper and calls super().__init__()
      3. Implement the scrape() method
      4. Register it in scrapers/__init__.py SCRAPER_REGISTRY
      5. Use the new type label in urls.yaml (e.g., "greenhouse | https://...")
    """

    def __init__(self, max_age_days: int | None = None, headers: dict | None = None,
                 timeout: int = 30, **kwargs):
        """Initialize the shared HTTP client.

        Args:
            max_age_days: Only include jobs posted within this many days. None = no limit.
            headers:      Extra headers merged over the default User-Agent.
            timeout:      HTTP timeout in seconds.
        """
        self._max_age_days = max_age_days
        # httpx client reused across API calls for connection pooling
        self._client = httpx.Client(
            timeout=timeout,
            verify=False,  # Corporate Zscaler proxy intercepts HTTPS
            headers={"User-Agent": _USER_AGENT, **(headers or {})},
            follow_redirects=True,
        )

    def scrape(self, url: str) -> list[JobPosting]:
        """Scrape the given career portal URL and return job postings.

        Args:
            url: The career portal URL to scrape.

        Returns:
            List of JobPosting objects found on the page.
        """
        raise NotImplementedError

    def _within_age(self, posted_date: str, now: datetime) -> bool:
        """Check if a YYYY-MM-DD posted date is within max_age_days.

        Jobs with missing or unparseable dates are kept.
        """
        if not posted_date or self._max_age_days is None:
            return True
        try:
            dt = datetime.strptime(posted_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            return (now - dt).days <= self._max_age_days
        except ValueError:
            return True

    def close(self):
        """Close the shared httpx client."""
        self._client.close()
