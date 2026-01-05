"""
scrapers/workday.py — Workday Career Portal Scraper
=====================================================
Scrapes job postings from Workday-powered career sites using their hidden JSON API.

How Workday portals work:
  - Public URL: https://company1.wd5.myworkdayjobs.com/en-US/External
  - Hidden API: https://company1.wd5.myworkdayjobs.com/wday/cxs/company1/External/jobs

  The API accepts POST requests with JSON body and returns paginated job listings.
  No authentication needed — it's the same API the browser uses behind the scenes.

Approach:
  1. Parse the portal URL to extract company name and site identifier
  2. Build the API URL from those parts
  3. POST to /jobs with pagination (20 jobs per page)
  4. Each job comes with title, location, posted date, and a path to its detail page
  5. Optionally fetch full descriptions via GET /jobs/{path} (done in main.py before matching)

Example:
  URL: https://netflix.wd5.myworkdayjobs.com/en-US/External
  API: https://netflix.wd5.myworkdayjobs.com/wday/cxs/netflix/External/jobs
"""

import hashlib
import logging
import re
from urllib.parse import urlparse

import httpx

from scrapers.base import BaseScraper, JobPosting

logger = logging.getLogger(__name__)

# Workday returns up to this many jobs per API call
_PAGE_SIZE = 20


def _parse_workday_url(url: str) -> tuple[str, str]:
    """Extract company name and site identifier from a Workday portal URL.

    Args:
        url: e.g., "https://company1.wd5.myworkdayjobs.com/en-US/External"

    Returns:
        Tuple of (company, site).
        Example: ("company1", "External")
    """
    parsed = urlparse(url)
    # Hostname: "company1.wd5.myworkdayjobs.com" → company = "company1"
    company = parsed.hostname.split(".")[0]
    # Path: "/en-US/External" → site = "External" (last segment)
    path_parts = [p for p in parsed.path.strip("/").split("/") if p]
    site = path_parts[-1] if path_parts else "External"
    return company, site


def _build_api_base(url: str) -> str:
    """Convert a Workday portal URL into its API base URL.

    Args:
        url: e.g., "https://company1.wd5.myworkdayjobs.com/en-US/External"

    Returns:
        API base URL, e.g., "https://company1.wd5.myworkdayjobs.com/wday/cxs/company1/External"
    """
    parsed = urlparse(url)
    company, site = _parse_workday_url(url)
    return f"{parsed.scheme}://{parsed.hostname}/wday/cxs/{company}/{site}"


class WorkdayScraper(BaseScraper):
    """Scrapes Workday career portals via their JSON API (no browser needed)."""

    def __init__(self, **kwargs):
        # httpx client reused across API calls for connection pooling
        self._client = httpx.Client(
            timeout=30,
            verify=False,  # Corporate Zscaler proxy intercepts HTTPS
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            follow_redirects=True,
        )

    def scrape(self, url: str) -> list[JobPosting]:
        """Scrape all job postings from a Workday portal.

        Paginates through the API until all jobs are fetched or an error occurs.
        Note: descriptions are NOT fetched here (too many API calls).
              main.py calls fetch_job_detail() separately for new jobs only.

        Args:
            url: The Workday career portal URL.

        Returns:
            List of JobPosting objects (with empty descriptions).
        """
        api_base = _build_api_base(url)
        company, _ = _parse_workday_url(url)
        logger.info("Scraping Workday portal: %s (API: %s)", url, api_base)

        all_jobs = []
        offset = 0

        # Paginate: keep fetching until we get fewer jobs than a full page
        while True:
            jobs_batch = self._fetch_job_list(api_base, company, url, offset)
            if not jobs_batch:
                break
            all_jobs.extend(jobs_batch)
            offset += _PAGE_SIZE
            if len(jobs_batch) < _PAGE_SIZE:
                break  # Last page — no more jobs

        logger.info("Found %d jobs from %s", len(all_jobs), url)
        return all_jobs

    def _fetch_job_list(
        self, api_base: str, company: str, portal_url: str, offset: int
    ) -> list[JobPosting]:
        """Fetch one page of job listings from the Workday API.

        Args:
            api_base:   The API base URL (from _build_api_base).
            company:    Company name (for the JobPosting.company field).
            portal_url: Original portal URL (to build direct job links).
            offset:     Pagination offset (0, 20, 40, ...).

        Returns:
            List of JobPosting objects for this page. Empty list on error.
        """
        payload = {
            "appliedFacets": {},   # No filters — get all jobs
            "limit": _PAGE_SIZE,
            "offset": offset,
            "searchText": "",      # No search — get everything
        }
        try:
            resp = self._client.post(f"{api_base}/jobs", json=payload)
            resp.raise_for_status()
        except httpx.HTTPError as e:
            logger.error("Workday API request failed for %s: %s", api_base, e)
            return []

        data = resp.json()
        postings = data.get("jobPostings", [])
        jobs = []

        for item in postings:
            title = item.get("title", "")
            external_path = item.get("externalPath", "")  # e.g., "/job/Senior-Engineer/JR12345"
            bullet_fields = item.get("bulletFields", [])   # [location, posted_date, ...]
            location = bullet_fields[0] if bullet_fields else ""
            posted_date = bullet_fields[1] if len(bullet_fields) > 1 else ""

            # Build the direct URL a user can click to see the full posting
            parsed = urlparse(portal_url)
            job_url = f"{parsed.scheme}://{parsed.hostname}{external_path}"

            # Use externalPath as the unique ID (stable across scrapes)
            # Fallback to MD5 hash if no path is available
            job_id = external_path or hashlib.md5(
                f"{company}:{title}".encode()
            ).hexdigest()

            jobs.append(JobPosting(
                job_id=job_id,
                title=title,
                company=company,
                location=location,
                description="",  # Filled later by fetch_job_detail()
                url=job_url,
                posted_date=posted_date,
            ))

        return jobs

    def fetch_job_detail(self, api_base: str, external_path: str) -> str:
        """Fetch the full job description for a single posting.

        Called by main.py for NEW jobs only (to avoid unnecessary API calls).
        The description is used by the GenAI matcher to score relevance.

        Args:
            api_base:      The API base URL.
            external_path: The job's externalPath (also its job_id).

        Returns:
            Plain text job description (HTML tags stripped). Empty string on error.
        """
        try:
            resp = self._client.get(f"{api_base}{external_path}")
            resp.raise_for_status()
            data = resp.json()
            posting_info = data.get("jobPostingInfo", {})
            raw_desc = posting_info.get("jobDescription", "")
            return _strip_html(raw_desc)
        except httpx.HTTPError as e:
            logger.warning("Failed to fetch job detail %s: %s", external_path, e)
            return ""

    def close(self):
        """Close the httpx client (releases connections)."""
        self._client.close()


def _strip_html(html: str) -> str:
    """Remove HTML tags from a string. Used to clean Workday job descriptions."""
    clean = re.sub(r"<[^>]+>", " ", html)  # Replace tags with spaces
    clean = re.sub(r"\s+", " ", clean)      # Collapse multiple spaces
    return clean.strip()
