"""
scrapers/hcl.py — HCLTech Careers Scraper
==========================================
Scrapes job postings from HCLTech's career site (careers.hcltech.com) using their
SuccessFactors recruiting JSON API.

Why a dedicated scraper instead of the generic SuccessFactors RSS scraper:
  - HCL's SF instance has locationSearchableFields: [] (empty), so the RSS
    locationSearch:(City) syntax returns zero results.
  - The RSS feed has no structured city data — titles just show "(IN)" for India.
  - The JSON API at /services/recruiting/v1/jobs supports:
      • facetFilters: {"custCountryRegion": ["India"]} — proper country filtering
      • sortBy: "date" — newest first
      • Structured custprimecity field per job
  - Page size is fixed at 10 (server-enforced, cannot be changed).

Approach:
  1. POST to /services/recruiting/v1/jobs with India facet filter, sorted by date
  2. Paginate through results (10 per page)
  3. Filter client-side by custprimecity matching target cities
  4. Stop early once jobs are older than max_age_days (since results are date-sorted)
  5. Fetch full description from the job detail page for matched jobs

City name variants found in HCL's data:
  Bangalore area: Bengaluru, Bangalore, Bangalore South, Bangaloresouth, Banglore
  Hyderabad area: Hyderabad, Serilingampally
"""

import logging
import re
import time
from datetime import datetime, timezone

import httpx

from scrapers.base import BaseScraper, JobPosting

logger = logging.getLogger(__name__)

_API_URL = "https://careers.hcltech.com/services/recruiting/v1/jobs"
_JOB_BASE_URL = "https://careers.hcltech.com/job"

_DEFAULT_CITIES = {
    "Bengaluru",
    "Bangalore",
    "Bangalore South",
    "Bangaloresouth",
    "Banglore",
    "Hyderabad",
    "Serilingampally",
}

# Max pages to scan before giving up (safety limit: 500 pages = 5000 jobs)
_MAX_PAGES = 500

# How many consecutive pages with zero target-city matches before stopping early
_EMPTY_PAGE_LIMIT = 30


def _parse_sf_date(date_str: str) -> str:
    """Parse SuccessFactors date like '4/20/26' or '3/2/26' to YYYY-MM-DD.

    SF uses M/D/YY format (2-digit year).
    """
    if not date_str:
        return ""
    try:
        dt = datetime.strptime(date_str.strip(), "%m/%d/%y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return ""


def _strip_html(html: str) -> str:
    """Remove HTML tags from a string."""
    clean = re.sub(r"<[^>]+>", " ", html)
    clean = re.sub(r"\s+", " ", clean)
    return clean.strip()


class HCLScraper(BaseScraper):
    """Scrapes HCLTech careers via their SuccessFactors recruiting JSON API."""

    def __init__(
        self,
        cities: set[str] | None = None,
        max_age_days: int | None = None,
        max_pages: int = _MAX_PAGES,
        **kwargs,
    ):
        """Initialize with target cities and optional limits.

        Args:
            cities:       Set of custprimecity values to match.
                          Defaults to Bengaluru/Bangalore/Hyderabad variants.
            max_age_days: Only include jobs posted within this many days. None = no limit.
            max_pages:    Maximum API pages to fetch (10 results/page). Safety limit.
        """
        self._cities = {c.lower() for c in (cities or _DEFAULT_CITIES)}
        self._max_age_days = max_age_days
        self._max_pages = max_pages
        self._client = httpx.Client(
            timeout=30,
            verify=False,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/125.0.0.0 Safari/537.36",
                "Content-Type": "application/json",
            },
            follow_redirects=True,
        )

    def scrape(self, url: str) -> list[JobPosting]:
        """Scrape HCL jobs for target cities from the recruiting API.

        Args:
            url: The base career site URL (e.g., https://careers.hcltech.com).
                 Used for logging; the scraper always hits the API directly.

        Returns:
            List of JobPosting objects in target cities.
        """
        logger.info("Scraping HCLTech API (cities: %s, max_age_days: %s)",
                     self._cities, self._max_age_days)

        matched_jobs = []
        total_scanned = 0
        consecutive_empty = 0
        now = datetime.now(timezone.utc)

        for page in range(self._max_pages):
            results, total_jobs = self._fetch_page(page)

            if results is None:
                logger.warning("HCL: API error on page %d, stopping", page)
                break

            if not results:
                break

            page_matches = 0
            stop_due_to_age = False

            for job_data in results:
                total_scanned += 1
                resp = job_data.get("response", {})
                city = resp.get("custprimecity", "")
                posted = _parse_sf_date(resp.get("unifiedStandardStart", ""))

                # Check age — since results are sorted by date, once we hit
                # a job older than our cutoff we can stop entirely
                if self._max_age_days is not None and posted:
                    try:
                        dt = datetime.strptime(posted, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                        if (now - dt).days > self._max_age_days:
                            stop_due_to_age = True
                            break
                    except ValueError:
                        pass

                # Check city match
                if city.lower() in self._cities:
                    job = self._parse_job(resp, posted)
                    if job:
                        matched_jobs.append(job)
                        page_matches += 1

            if stop_due_to_age:
                logger.info("HCL: Reached max_age_days=%d cutoff at page %d", self._max_age_days, page)
                break

            # Early stop if we keep getting zero matches (we're past the target cities)
            if page_matches == 0:
                consecutive_empty += 1
                if consecutive_empty >= _EMPTY_PAGE_LIMIT:
                    logger.info("HCL: %d consecutive empty pages, stopping at page %d",
                                _EMPTY_PAGE_LIMIT, page)
                    break
            else:
                consecutive_empty = 0

            # Progress logging every 50 pages
            if (page + 1) % 50 == 0:
                logger.info("HCL: page %d/%d, scanned %d, matched %d so far",
                             page + 1, self._max_pages, total_scanned, len(matched_jobs))

            if len(results) < 10:
                break

            time.sleep(0.1)  # Be polite

        logger.info("HCL: scanned %d jobs across %d pages, matched %d in target cities",
                     total_scanned, min(page + 1, self._max_pages), len(matched_jobs))

        return matched_jobs

    def _fetch_page(self, page: int) -> tuple[list | None, int]:
        """Fetch one page of results from the recruiting API.

        Returns:
            Tuple of (results_list, total_jobs). results_list is None on error.
        """
        body = {
            "keywords": "",
            "locale": "en_US",
            "location": "",
            "pageNumber": page,
            "sortBy": "date",
            "facetFilters": {"custCountryRegion": ["India"]},
        }

        try:
            resp = self._client.post(_API_URL, json=body)
            resp.raise_for_status()
            data = resp.json()
            return data.get("jobSearchResult", []), data.get("totalJobs", 0)
        except httpx.HTTPError as e:
            logger.error("HCL API fetch failed (page %d): %s", page, e)
            return None, 0
        except Exception as e:
            logger.error("HCL API parse failed (page %d): %s", page, e)
            return None, 0

    def _parse_job(self, resp: dict, posted_date: str) -> JobPosting | None:
        """Parse a job response dict into a JobPosting.

        Args:
            resp:        The 'response' dict from a jobSearchResult item.
            posted_date: Already-parsed YYYY-MM-DD date string.

        Returns:
            JobPosting or None if essential fields are missing.
        """
        job_id = str(resp.get("id", ""))
        title = resp.get("unifiedStandardTitle", "")
        url_title = resp.get("urlTitle", "")

        if not job_id or not title:
            return None

        city = resp.get("custprimecity", "")
        location = f"{city}, India" if city else "India"
        job_url = f"{_JOB_BASE_URL}/{url_title}/{job_id}-en_US" if url_title else ""

        return JobPosting(
            job_id=job_id,
            title=title,
            company="HCLTech",
            location=location,
            description="",  # Filled later by enrich_descriptions
            url=job_url,
            posted_date=posted_date,
        )

    def enrich_descriptions(self, jobs: list[JobPosting]) -> None:
        """Fetch full descriptions from individual job detail pages.

        Args:
            jobs: List of JobPosting objects to enrich in-place.
        """
        logger.info("HCL: enriching descriptions for %d jobs", len(jobs))

        for i, job in enumerate(jobs):
            if not job.url:
                continue
            try:
                resp = self._client.get(job.url)
                resp.raise_for_status()
                html = resp.text

                # Extract description from the job page HTML
                # SF job pages have description in a div with class containing "jobDescription"
                # or within the main content area
                desc = self._extract_description(html)
                if desc:
                    job.description = desc

            except httpx.HTTPError as e:
                logger.warning("HCL: failed to fetch description for %s: %s", job.job_id, e)

            if (i + 1) % 20 == 0:
                logger.info("HCL: enriched %d/%d descriptions", i + 1, len(jobs))
                time.sleep(0.2)

    @staticmethod
    def _extract_description(html: str) -> str:
        """Extract job description text from the HCL job detail page."""
        # Look for the main job description content between Job Summary and Apply
        match = re.search(
            r'Job\s+Summary(.*?)(?:Skill\s+Requirements|Other\s+Requirements|Apply\s+now)',
            html,
            re.DOTALL | re.IGNORECASE,
        )
        if match:
            return _strip_html(match.group(1))

        # Fallback: look for content:encoded or description meta
        match = re.search(r'<meta\s+name="description"\s+content="([^"]*)"', html, re.IGNORECASE)
        if match:
            return match.group(1).strip()

        return ""

    def close(self):
        """Close the httpx client."""
        self._client.close()
