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

import logging
import re
from urllib.parse import urlparse

import httpx

from scrapers.base import BaseScraper, JobPosting, generate_fallback_id

logger = logging.getLogger(__name__)

# Workday returns up to this many jobs per API call
_PAGE_SIZE = 20


def _parse_posted_days(posted_on: str) -> int | None:
    """Parse Workday's relative date string into number of days ago.

    Examples:
        "Posted Today"        → 0
        "Posted Yesterday"    → 1
        "Posted 2 Days Ago"   → 2
        "Posted 30+ Days Ago" → 30

    Returns:
        Number of days ago, or None if the format is unrecognized.
    """
    text = posted_on.lower().strip()
    if "today" in text:
        return 0
    if "yesterday" in text:
        return 1
    match = re.search(r"(\d+)\+?\s*days?\s*ago", text)
    if match:
        return int(match.group(1))
    return None


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
    company = parsed.hostname.split(".")[0]
    path_parts = [p for p in parsed.path.strip("/").split("/") if p]
    site = path_parts[-1] if path_parts else "External"
    return f"{parsed.scheme}://{parsed.hostname}/wday/cxs/{company}/{site}"


class WorkdayScraper(BaseScraper):
    """Scrapes Workday career portals via their JSON API (no browser needed)."""

    def __init__(self, facets: dict | None = None, max_age_days: int | None = None, **kwargs):
        """Initialize with optional API facet filters and age limit.

        Args:
            facets:       Optional dict of Workday facet filters to narrow results.
                          e.g., {"locationCountry": ["c4f78be1a8f14da0ab49ce1162348a5e"]}
            max_age_days: Only include jobs posted within this many days.
                          e.g., 10 = skip anything older than 10 days. None = no limit.
        """
        self._facets = facets or {}
        self._max_age_days = max_age_days
        # httpx client reused across API calls for connection pooling
        self._client = httpx.Client(
            timeout=30,
            verify=False,  # Corporate Zscaler proxy intercepts HTTPS
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            },
            follow_redirects=True,
        )

    def scrape(self, url: str) -> list[JobPosting]:
        """Scrape all job postings from a Workday portal.

        Paginates through the API until all jobs are fetched or an error occurs.
        Note: descriptions are NOT fetched here (too many API calls).
              Call enrich_descriptions() separately for new jobs only.

        Args:
            url: The Workday career portal URL.

        Returns:
            List of JobPosting objects (with empty descriptions).
        """
        self._portal_url = url
        api_base = _build_api_base(url)
        company, _ = _parse_workday_url(url)
        logger.info("Scraping Workday portal: %s (API: %s)", url, api_base)

        all_jobs = []
        offset = 0

        # First call to get total count + facet names for logging
        jobs_batch, total, facets_data = self._fetch_job_list(api_base, company, url, offset)
        all_jobs.extend(jobs_batch)
        offset += _PAGE_SIZE

        # Resolve applied facet IDs to human-readable names for logging
        region_label = self._resolve_facet_names(facets_data)

        # Fetch remaining pages based on total from API
        while offset < total:
            jobs_batch, _, _ = self._fetch_job_list(api_base, company, url, offset)
            all_jobs.extend(jobs_batch)
            offset += _PAGE_SIZE

        # Filter out old jobs
        before_filter = len(all_jobs)
        if self._max_age_days is not None:
            all_jobs = [
                j for j in all_jobs
                if (_parse_posted_days(j.posted_date) or 0) <= self._max_age_days
            ]
            filtered_out = before_filter - len(all_jobs)
            logger.info(
                "%s: %d jobs in region, %d within %d days, %d older (skipped)",
                region_label, total, len(all_jobs), self._max_age_days, filtered_out,
            )
        else:
            logger.info("%s: %d jobs in region, fetched all %d", region_label, total, len(all_jobs))
        return all_jobs

    def _resolve_facet_names(self, facets_data: list[dict]) -> str:
        """Resolve applied facet IDs to readable names using API response facets.

        Returns a string like "India > Karnataka, Telangana, Maharashtra".
        """
        if not self._facets or not facets_data:
            return "All regions"

        # Build a lookup: facet_id -> descriptor from the API response
        id_to_name = {}
        for facet in facets_data:
            for val in facet.get("values", []):
                if "id" in val:
                    id_to_name[val["id"]] = val.get("descriptor", val["id"])
                # Handle nested facets (e.g., locationMainGroup > locationCountry)
                for sub_val in val.get("values", []):
                    if "id" in sub_val:
                        id_to_name[sub_val["id"]] = sub_val.get("descriptor", sub_val["id"])

        parts = []
        for param, ids in self._facets.items():
            names = [id_to_name.get(fid, fid) for fid in ids]
            parts.append(", ".join(names))
        return " > ".join(parts) if parts else "All regions"

    def _fetch_job_list(
        self, api_base: str, company: str, portal_url: str, offset: int
    ) -> tuple[list[JobPosting], int, list[dict]]:
        """Fetch one page of job listings from the Workday API.

        Args:
            api_base:   The API base URL (from _build_api_base).
            company:    Company name (for the JobPosting.company field).
            portal_url: Original portal URL (to build direct job links).
            offset:     Pagination offset (0, 20, 40, ...).

        Returns:
            Tuple of (jobs, total, facets). total is the API's reported total job count.
            facets is the raw facets list from the response (used for resolving names).
        """
        payload = {
            "appliedFacets": self._facets,
            "limit": _PAGE_SIZE,
            "offset": offset,
            "searchText": "",
        }
        try:
            resp = self._client.post(f"{api_base}/jobs", json=payload)
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPError as e:
            logger.error("Workday API request failed for %s: %s", api_base, e)
            return [], 0, []
        except ValueError:
            logger.error("Workday API returned non-JSON response for %s (status %d): %s",
                         api_base, resp.status_code, resp.text[:500])
            return [], 0, []
        total = data.get("total", 0)
        facets_data = data.get("facets", [])
        postings = data.get("jobPostings", [])
        jobs = []
        parsed = urlparse(portal_url)
        url_prefix = f"{parsed.scheme}://{parsed.hostname}"

        for item in postings:
            title = item.get("title", "")
            external_path = item.get("externalPath", "")
            bullet_fields = item.get("bulletFields", [])
            job_num = bullet_fields[0] if bullet_fields else ""
            location = item.get("locationsText", "")
            posted_date = item.get("postedOn", "")

            # Use subsidiary name (e.g., "Pall", "Cytiva") if available, else fallback
            sub_company = bullet_fields[1] if len(bullet_fields) > 1 else company

            job_url = f"{url_prefix}{external_path}"

            # Use externalPath as the unique ID (stable across scrapes)
            # Fallback to MD5 hash if no path is available
            job_id = external_path or generate_fallback_id(f"{company}:{title}")

            jobs.append(JobPosting(
                job_id=job_id,
                title=title,
                company=sub_company,
                location=location,
                description="",  # Filled later by fetch_job_detail()
                url=job_url,
                posted_date=posted_date,
                job_num=job_num,
            ))

        return jobs, total, facets_data

    def fetch_job_detail(self, api_base: str, external_path: str) -> tuple[str, str]:
        """Fetch the full job description and exact location for a single posting.

        Called by main.py for NEW jobs only (to avoid unnecessary API calls).
        The description is used by the GenAI matcher to score relevance.

        Args:
            api_base:      The API base URL.
            external_path: The job's externalPath (also its job_id).

        Returns:
            Tuple of (description, location). Description is plain text (HTML stripped).
            Location includes all locations joined by " | ".
            Returns ("", "") on error.
        """
        try:
            resp = self._client.get(f"{api_base}{external_path}")
            resp.raise_for_status()
            data = resp.json()
            posting_info = data.get("jobPostingInfo", {})
            raw_desc = posting_info.get("jobDescription", "")
            # Build exact location from primary + additional locations
            primary_loc = posting_info.get("location", "")
            additional = posting_info.get("additionalLocations", [])
            all_locations = [primary_loc] + additional if additional else [primary_loc]
            location = " | ".join(loc for loc in all_locations if loc)
            return _strip_html(raw_desc), location
        except (httpx.HTTPError, ValueError) as e:
            logger.warning("Failed to fetch job detail %s: %s", external_path, e)
            return "", ""

    def enrich_descriptions(self, jobs: list[JobPosting]):
        """Fetch full descriptions for jobs that are missing them.

        Args:
            jobs: List of JobPosting objects to enrich in-place.
        """
        api_base = _build_api_base(self._portal_url)
        for job in jobs:
            if not job.description and job.job_id:
                desc, location = self.fetch_job_detail(api_base, job.job_id)
                job.description = desc
                if location:
                    job.location = location

    def close(self):
        """Close the httpx client (releases connections)."""
        self._client.close()


def _strip_html(html: str) -> str:
    """Remove HTML tags from a string. Used to clean Workday job descriptions."""
    clean = re.sub(r"<[^>]+>", " ", html)  # Replace tags with spaces
    clean = re.sub(r"\s+", " ", clean)      # Collapse multiple spaces
    return clean.strip()
