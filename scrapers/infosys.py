"""
scrapers/infosys.py — Infosys Career Portal Scraper
=====================================================
Scrapes job postings from Infosys's career site (career.infosys.com) using their
internal REST API.

How it works:
  - Portal:   https://career.infosys.com/joblist
  - JSON API: https://intapgateway.infosysapps.com/careersci/search/intapjbsrch/getCareerSearchJobs

  The API accepts GET requests with sourceId and searchText params and returns the
  full list of jobs as a flat JSON array (no pagination). No authentication needed.

Approach:
  1. GET the API endpoint with sourceId from the URL query params
  2. Parse each job object for title, location, description, posted date
  3. Descriptions come inline (rolesResponsibilities + technicalRequirement + postingDescription)
  4. Optionally filter by age using createdOn field

sourceId mapping:
  1     = India lateral (Infosys Limited)
  21    = India fresher (Infosys Limited)
  1,21  = India all (Infosys Limited)
  41    = India lateral/all (Infosys BPM)
  61    = China lateral/all (Infosys Limited)
  81    = China lateral/all (Infosys BPM)
  91    = Manila lateral/all (Infosys BPM)

Example urls.yaml entry:
  infosys | https://career.infosys.com/joblist?sourceId=1,21
"""

import logging
import re
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

import httpx

from scrapers.base import BaseScraper, JobPosting

logger = logging.getLogger(__name__)

_API_BASE = "https://intapgateway.infosysapps.com/careersci/search/intapjbsrch/getCareerSearchJobs"
_JOB_DETAIL_BASE = "https://career.infosys.com/jobdesc?jobReferenceCode="


def _strip_html(html: str) -> str:
    """Remove HTML tags from a string."""
    clean = re.sub(r"<[^>]+>", " ", html)
    clean = re.sub(r"\s+", " ", clean)
    return clean.strip()


class InfosysScraper(BaseScraper):
    """Scrapes Infosys careers via their internal REST API (no browser needed)."""

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
        """Scrape all job postings from Infosys careers.

        The API returns the full dataset in one call (no pagination needed).

        Args:
            url: The career portal URL with sourceId param.
                 e.g., https://career.infosys.com/joblist?sourceId=1,21

        Returns:
            List of JobPosting objects with inline descriptions.
        """
        source_id = self._extract_source_id(url)
        api_url = f"{_API_BASE}?sourceId={source_id}&searchText=ALL"

        try:
            resp = self._client.get(api_url)
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPError as e:
            logger.error("Infosys API request failed: %s", e)
            return []
        except ValueError:
            logger.error("Infosys API returned non-JSON response (status %d)", resp.status_code)
            return []

        if not isinstance(data, list):
            logger.error("Infosys API returned unexpected format (expected array, got %s)", type(data).__name__)
            return []

        all_jobs = []
        for item in data:
            job = self._parse_job(item)
            if job:
                all_jobs.append(job)

        # Filter by age
        before_filter = len(all_jobs)
        if self._max_age_days is not None:
            now = datetime.now(timezone.utc)
            all_jobs = [j for j in all_jobs if self._within_age(j.posted_date, now)]
            skipped = before_filter - len(all_jobs)
            logger.info(
                "Infosys: %d total jobs, %d within %d days, %d older (skipped)",
                before_filter, len(all_jobs), self._max_age_days, skipped,
            )
        else:
            logger.info("Infosys: %d total jobs", len(all_jobs))

        return all_jobs

    @staticmethod
    def _extract_source_id(url: str) -> str:
        """Extract sourceId from URL query params, defaulting to '1,21' (India all)."""
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        source_ids = params.get("sourceId", ["1,21"])
        return source_ids[0]

    def _parse_job(self, item: dict) -> JobPosting | None:
        """Parse a single job object from the API response.

        Args:
            item: A job dict from the API array.

        Returns:
            JobPosting, or None if essential fields are missing.
        """
        ref_code = item.get("referenceCode", "")
        title = item.get("postingTitle", "")
        if not ref_code or not title:
            return None

        company = item.get("company", "Infosys")
        location = (item.get("location", "") or "").strip()

        # Build description from multiple fields
        desc_parts = []
        for field in ("rolesResponsibilities", "technicalRequirement",
                      "postingDescription", "additionalResponsibility",
                      "educationalRequirement"):
            val = item.get(field, "")
            if val and val.strip():
                desc_parts.append(_strip_html(val))
        description = "\n".join(desc_parts)

        # Skills metadata — append if present
        skills_parts = []
        for field in ("skills", "preferredSkills", "genericSkills"):
            val = item.get(field, "")
            if val and val.strip():
                skills_parts.append(val.strip())
        if skills_parts:
            description += "\nSkills: " + "; ".join(skills_parts)

        # Posted date
        created_on = item.get("createdOn", "")
        posted_date = self._parse_created_on(created_on)

        job_url = f"{_JOB_DETAIL_BASE}{ref_code}"
        requisition_id = str(item.get("requisitionId", ""))

        return JobPosting(
            job_id=ref_code,
            title=title,
            company=company,
            location=location,
            description=description,
            url=job_url,
            posted_date=posted_date,
            job_num=requisition_id,
        )

    @staticmethod
    def _parse_created_on(created_on: str) -> str:
        """Parse Infosys ISO timestamp to YYYY-MM-DD date string.

        Args:
            created_on: e.g., "2026-04-30T10:28:48.282"

        Returns:
            Date string like "2026-04-30", or empty string on error.
        """
        if not created_on:
            return ""
        try:
            dt = datetime.fromisoformat(created_on.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return ""

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
