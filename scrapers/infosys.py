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
from urllib.parse import parse_qs, urlparse

import httpx

from scrapers.base import BaseScraper, JobPosting, strip_html, date_or_none

logger = logging.getLogger(__name__)

_API_BASE = "https://intapgateway.infosysapps.com/careersci/search/intapjbsrch/getCareerSearchJobs"
_JOB_DETAIL_BASE = "https://career.infosys.com/jobdesc?jobReferenceCode="


class InfosysScraper(BaseScraper):
    """Scrapes Infosys careers via their internal REST API (no browser needed)."""

    def __init__(self, **kwargs):
        super().__init__(headers={"Accept": "application/json"}, **kwargs)

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

        all_jobs = [job for job in all_jobs if self._within_age(job.posted_date)]
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
                desc_parts.append(strip_html(val))
        description = "\n".join(desc_parts)

        # Skills metadata — append if present
        skills_parts = []
        for field in ("skills", "preferredSkills", "genericSkills"):
            val = item.get(field, "")
            if val and val.strip():
                skills_parts.append(val.strip())
        if skills_parts:
            description += "\nSkills: " + "; ".join(skills_parts)

        posted_date = date_or_none(item.get("createdOn"))

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
