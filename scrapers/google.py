"""
scrapers/google.py — Google Careers Scraper
=============================================
Scrapes job postings from Google's career site using their internal batchexecute RPC API.

How it works:
  - Google's career page (google.com/about/careers/applications/jobs/results) uses
    a Wiz framework frontend that calls a batchexecute RPC endpoint internally.
  - RPC endpoint: /about/careers/applications/_/HiringCportalFrontendUi/data/batchexecute
  - RPC ID: r06xKb
  - Returns paginated results (20/page) with full descriptions, qualifications, locations.
  - No authentication needed — same endpoint the browser uses.

Approach:
  1. POST to batchexecute with location + page number
  2. Parse the nested JSON response to extract job arrays
  3. Paginate until all pages are fetched
  4. Query each target city separately and deduplicate

Example locations: "Bangalore India", "Hyderabad India"
Note: "Bangalore India" and "Bengaluru India" return identical results.
"""

import json
import logging
import re
import time
from datetime import datetime, timezone

import httpx

from scrapers.base import BaseScraper, JobPosting

logger = logging.getLogger(__name__)

_BATCH_URL = "https://www.google.com/about/careers/applications/_/HiringCportalFrontendUi/data/batchexecute"
_RPC_ID = "r06xKb"
_PAGE_SIZE = 20

# Google normalizes "Bengaluru" to "Bangalore", so no need to query both
_DEFAULT_LOCATIONS = ["Bangalore India", "Hyderabad India"]


def _strip_html(html: str) -> str:
    """Remove HTML tags from a string."""
    clean = re.sub(r"<[^>]+>", " ", html)
    clean = re.sub(r"\s+", " ", clean)
    return clean.strip()


def _safe_field(job: list, idx: int, sub_idx: int | None = None) -> str:
    """Safely extract a field from the job array."""
    try:
        val = job[idx]
        if val is None:
            return ""
        if sub_idx is not None:
            return val[sub_idx] or ""
        return val or ""
    except (IndexError, TypeError):
        return ""


class GoogleScraper(BaseScraper):
    """Scrapes Google Careers via their internal batchexecute RPC API."""

    def __init__(self, locations: list[str] | None = None, max_age_days: int | None = None, **kwargs):
        """Initialize with location search terms and optional age limit.

        Args:
            locations:    List of location strings for Google's search.
                          Defaults to ["Bangalore India", "Hyderabad India"].
            max_age_days: Only include jobs posted within this many days. None = no limit.
        """
        self._locations = locations or _DEFAULT_LOCATIONS
        self._max_age_days = max_age_days
        self._client = httpx.Client(
            timeout=30,
            verify=False,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/125.0.0.0 Safari/537.36",
            },
            follow_redirects=True,
        )

    def scrape(self, url: str) -> list[JobPosting]:
        """Scrape all Google jobs for configured locations.

        Args:
            url: Ignored (kept for interface compatibility). The scraper
                 always hits Google's batchexecute endpoint directly.

        Returns:
            List of deduplicated JobPosting objects.
        """
        logger.info("Scraping Google Careers (locations: %s)", self._locations)

        seen_ids = set()
        all_jobs = []

        for location in self._locations:
            jobs, total = self._scrape_location(location)
            for job in jobs:
                if job.job_id not in seen_ids:
                    seen_ids.add(job.job_id)
                    all_jobs.append(job)
            logger.info("  Google/%s: %d jobs (total available: %d)", location, len(jobs), total)

        # Filter by age
        if self._max_age_days is not None:
            now = datetime.now(timezone.utc)
            before = len(all_jobs)
            all_jobs = [j for j in all_jobs if self._within_age(j.posted_date, now)]
            logger.info(
                "Google: %d unique jobs, %d within %d days (skipped %d)",
                before, len(all_jobs), self._max_age_days, before - len(all_jobs),
            )
        else:
            logger.info("Google: %d unique jobs across locations", len(all_jobs))

        return all_jobs

    def _scrape_location(self, location: str) -> tuple[list[JobPosting], int]:
        """Fetch all pages for a single location.

        Returns:
            Tuple of (jobs, total_available).
        """
        all_jobs = []
        page = 1
        total = 0

        while True:
            raw_jobs, page_total = self._fetch_page(location, page)
            if page_total:
                total = page_total

            if not raw_jobs:
                break

            for raw in raw_jobs:
                job = self._parse_job(raw)
                if job:
                    all_jobs.append(job)

            if len(all_jobs) >= total or len(raw_jobs) < _PAGE_SIZE:
                break

            page += 1
            time.sleep(0.3)  # Be polite

        return all_jobs, total

    def _fetch_page(self, location: str, page: int) -> tuple[list | None, int]:
        """Fetch one page of results via batchexecute RPC.

        Returns:
            Tuple of (raw_jobs_list, total_count). raw_jobs_list is None on error.
        """
        inner_params = json.dumps(
            [[None, None, None, None, "en-US", None, [[location]], page]]
        )
        freq = json.dumps([[[_RPC_ID, inner_params, None, "generic"]]])

        try:
            resp = self._client.post(
                _BATCH_URL,
                data={"f.req": freq},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            resp.raise_for_status()
        except httpx.HTTPError as e:
            logger.error("Google batchexecute failed (page %d, %s): %s", page, location, e)
            return None, 0

        # Parse the batchexecute response format:
        # )]}'  (XSS prefix)
        # N    (length)
        # [["wrb.fr","r06xKb","<JSON string>", ...]]
        for line in resp.text.split("\n"):
            line = line.strip()
            if line.startswith('[["wrb.fr"'):
                try:
                    outer = json.loads(line)
                    jobs_data = json.loads(outer[0][2])
                    jobs_list = jobs_data[0] if jobs_data[0] else []
                    total = jobs_data[2] if len(jobs_data) > 2 and jobs_data[2] else 0
                    return jobs_list, total
                except (json.JSONDecodeError, IndexError, TypeError) as e:
                    logger.warning("Failed to parse Google batchexecute response: %s", e)
                    return None, 0

        return None, 0

    def _parse_job(self, raw: list) -> JobPosting | None:
        """Parse a raw job array into a JobPosting.

        Job array indices:
          [0]  = Job ID (numeric string)
          [1]  = Title
          [2]  = Apply URL
          [3]  = Responsibilities (HTML): [None, "<ul>..."]
          [4]  = Qualifications (HTML): [None, "<h3>Minimum..."]
          [7]  = Company name
          [9]  = Locations: [["City, State, Country", [address]], ...]
          [10] = Description/summary (HTML): [None, "..."]
          [12] = Posted date: [unix_timestamp, nanos]
          [19] = Preferred qualifications (HTML): [None, "<ul>..."]
        """
        try:
            job_id = str(raw[0])
            title = raw[1] or ""
            company = raw[7] or "Google"

            # Location
            locations = raw[9] if raw[9] else []
            location = locations[0][0] if locations and locations[0] else ""

            # Build job URL
            job_url = (
                f"https://www.google.com/about/careers/applications/jobs/results/"
                f"{job_id}"
            )

            # Combine description parts
            desc_parts = []
            summary = _safe_field(raw, 10, 1)
            if summary:
                desc_parts.append(summary)
            responsibilities = _safe_field(raw, 3, 1)
            if responsibilities:
                desc_parts.append(responsibilities)
            qualifications = _safe_field(raw, 4, 1)
            if qualifications:
                desc_parts.append(qualifications)
            preferred = _safe_field(raw, 19, 1)
            if preferred:
                desc_parts.append(preferred)

            description = _strip_html(" ".join(desc_parts))

            # Posted date from field [12] (unix timestamp)
            posted_date = ""
            if raw[12] and isinstance(raw[12], list) and raw[12][0]:
                try:
                    dt = datetime.fromtimestamp(raw[12][0], tz=timezone.utc)
                    posted_date = dt.strftime("%Y-%m-%d")
                except (ValueError, OSError):
                    pass

            return JobPosting(
                job_id=job_id,
                title=title,
                company=company,
                location=location,
                description=description,
                url=job_url,
                posted_date=posted_date,
            )
        except (IndexError, TypeError) as e:
            logger.warning("Failed to parse Google job: %s", e)
            return None

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
        """Close the httpx client."""
        self._client.close()
