"""
scrapers/goldmansachs.py — Goldman Sachs Career Portal Scraper
================================================================
Scrapes job postings from Goldman Sachs's career site (higher.gs.com) using their
internal GraphQL API.

How it works:
  - Portal:      https://higher.gs.com/roles
  - GraphQL API: https://api-higher.gs.com/gateway/api/v1/graphql

  The API accepts POST requests with GraphQL queries and returns paginated job listings.
  No authentication needed for public job search. Requires an x-higher-request-id header.

Approach:
  1. POST GetRoles GraphQL query with filters from the URL
  2. Paginate via pageNumber (0-indexed, 20 per page)
  3. Search returns metadata (title, location, skills) but not full descriptions
  4. enrich_descriptions() fetches the SSR role detail page for full JDs

Example urls.yaml entries:
  goldmansachs | https://higher.gs.com/roles?location=Bangalore
  goldmansachs | https://higher.gs.com/roles
"""

import json
import logging
import re
import uuid
from urllib.parse import parse_qs, urlparse

import httpx

from scrapers.base import BaseScraper, JobPosting

logger = logging.getLogger(__name__)

_GRAPHQL_URL = "https://api-higher.gs.com/gateway/api/v1/graphql"
_ROLE_DETAIL_URL = "https://higher.gs.com/roles/"
_PAGE_SIZE = 20

_GET_ROLES_QUERY = """
query GetRoles($searchQueryInput: RoleSearchQueryInput!) {
  roleSearch(searchQueryInput: $searchQueryInput) {
    totalCount
    items {
      roleId
      corporateTitle
      jobTitle
      jobFunction
      locations {
        primary
        state
        country
        city
      }
      status
      division
      skills
      jobType {
        code
        description
      }
      externalSource {
        sourceId
      }
    }
  }
}
"""


def _strip_html(html: str) -> str:
    """Remove HTML tags from a string."""
    clean = re.sub(r"<[^>]+>", " ", html)
    clean = re.sub(r"\s+", " ", clean)
    return clean.strip()


class GoldmanSachsScraper(BaseScraper):
    """Scrapes Goldman Sachs careers via their GraphQL API (no browser needed)."""

    def __init__(self, max_age_days: int | None = None, **kwargs):
        """Initialize with optional age filter.

        Args:
            max_age_days: Not currently used (GS API doesn't expose posted dates),
                          but accepted for interface consistency.
        """
        self._max_age_days = max_age_days
        self._client = httpx.Client(
            timeout=30,
            verify=False,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/125.0.0.0 Safari/537.36",
            },
            follow_redirects=True,
        )

    def scrape(self, url: str) -> list[JobPosting]:
        """Scrape all job postings from Goldman Sachs careers.

        Paginates through the GraphQL API until all results are fetched.

        Args:
            url: The career portal URL with optional query params.
                 e.g., https://higher.gs.com/roles?location=Bangalore

        Returns:
            List of JobPosting objects (descriptions may be sparse from search;
            call enrich_descriptions() for full JDs).
        """
        filters = self._build_filters(url)
        all_jobs = []
        page_number = 0
        total_count = None

        while True:
            items, total = self._fetch_page(filters, page_number)
            if total_count is None:
                total_count = total

            if not items:
                break

            for item in items:
                job = self._parse_job(item)
                if job:
                    all_jobs.append(job)

            page_number += 1
            if len(all_jobs) >= total_count:
                break

        logger.info("Goldman Sachs: %d jobs fetched (total available: %d)",
                    len(all_jobs), total_count or 0)
        return all_jobs

    @staticmethod
    def _build_filters(url: str) -> list[dict]:
        """Build GraphQL filter list from URL query params.

        Supports ?location=X and ?division=Y params.
        """
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        filters = []

        location = params.get("location", [None])[0]
        if location:
            filters.append({
                "filterCategoryType": "LOCATION",
                "filters": [{"filter": location, "subFilters": []}],
            })

        division = params.get("division", [None])[0]
        if division:
            filters.append({
                "filterCategoryType": "DIVISION",
                "filters": [{"filter": division, "subFilters": []}],
            })

        return filters

    def _fetch_page(self, filters: list[dict], page_number: int) -> tuple[list[dict], int]:
        """Fetch one page of results via GraphQL.

        Returns:
            Tuple of (items, total_count).
        """
        variables = {
            "searchQueryInput": {
                "page": {"pageSize": _PAGE_SIZE, "pageNumber": page_number},
                "sort": {"sortStrategy": "RELEVANCE", "sortOrder": "DESC"},
                "filters": filters,
                "experiences": ["PROFESSIONAL"],
                "searchTerm": "",
            }
        }

        payload = {
            "operationName": "GetRoles",
            "query": _GET_ROLES_QUERY,
            "variables": variables,
        }

        headers = {"x-higher-request-id": str(uuid.uuid4())}

        try:
            resp = self._client.post(_GRAPHQL_URL, json=payload, headers=headers)
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPError as e:
            logger.error("Goldman Sachs GraphQL request failed: %s", e)
            return [], 0
        except ValueError:
            logger.error("Goldman Sachs API returned non-JSON response (status %d)", resp.status_code)
            return [], 0

        role_search = data.get("data", {}).get("roleSearch", {})
        total_count = role_search.get("totalCount", 0)
        items = role_search.get("items", [])

        return items, total_count

    def _parse_job(self, item: dict) -> JobPosting | None:
        """Parse a single GraphQL result into a JobPosting.

        Args:
            item: A role dict from the GraphQL response.

        Returns:
            JobPosting, or None if essential fields are missing.
        """
        role_id = str(item.get("roleId", ""))
        title = item.get("jobTitle", "")
        if not role_id or not title:
            return None

        # Location: use first location's city + country
        locations = item.get("locations") or []
        location_parts = []
        for loc in locations:
            city = loc.get("city", "")
            country = loc.get("country", "")
            if city:
                location_parts.append(f"{city}, {country}" if country else city)
        location = " | ".join(location_parts) if location_parts else ""

        # Build a description from available metadata
        desc_parts = []
        corporate_title = item.get("corporateTitle", "")
        if corporate_title:
            desc_parts.append(f"Level: {corporate_title}")
        division = item.get("division", "")
        if division:
            desc_parts.append(f"Division: {division}")
        job_function = item.get("jobFunction", "")
        if job_function:
            desc_parts.append(f"Function: {job_function}")
        job_type = item.get("jobType", {})
        if job_type and job_type.get("description"):
            desc_parts.append(f"Type: {job_type['description']}")
        skills = item.get("skills") or []
        if skills:
            desc_parts.append(f"Skills: {', '.join(skills)}")
        description = "\n".join(desc_parts)

        # Job URL
        source_id = ""
        ext_source = item.get("externalSource") or {}
        source_id = str(ext_source.get("sourceId", ""))
        job_url = f"{_ROLE_DETAIL_URL}{source_id}" if source_id else f"{_ROLE_DETAIL_URL}{role_id}"

        return JobPosting(
            job_id=role_id,
            title=title,
            company="Goldman Sachs",
            location=location,
            description=description,
            url=job_url,
            posted_date="",
            job_num=source_id,
        )

    def enrich_descriptions(self, jobs: list[JobPosting]):
        """Fetch full job descriptions from the SSR role detail pages.

        Goldman Sachs role pages are Next.js SSR pages with a __NEXT_DATA__
        script tag containing the full role details. Falls back to keeping
        the metadata-only description if the page fetch fails.

        Args:
            jobs: List of JobPosting objects to enrich in-place.
        """
        for job in jobs:
            detail = self._fetch_role_detail(job.url)
            if detail:
                job.description = detail

    def _fetch_role_detail(self, url: str) -> str:
        """Fetch the full description from a role detail page.

        Extracts role data from the __NEXT_DATA__ JSON embedded in the SSR page.

        Returns:
            Full description text, or empty string on failure.
        """
        try:
            resp = self._client.get(url)
            resp.raise_for_status()
            html = resp.text
        except httpx.HTTPError as e:
            logger.warning("Failed to fetch GS role detail %s: %s", url, e)
            return ""

        # Extract __NEXT_DATA__ JSON
        match = re.search(
            r'<script\s+id="__NEXT_DATA__"\s+type="application/json">(.*?)</script>',
            html, re.DOTALL,
        )
        if not match:
            return ""

        try:
            next_data = json.loads(match.group(1))
            role = next_data.get("props", {}).get("pageProps", {}).get("role")
            if not role:
                return ""

            # Extract description fields from the role object
            desc_parts = []
            for field in ("description", "responsibilities", "qualifications",
                          "preferredQualifications", "aboutUs"):
                val = role.get(field, "")
                if val and val.strip():
                    desc_parts.append(_strip_html(val))

            # Also grab skills if present
            skills = role.get("skills") or []
            if skills:
                desc_parts.append(f"Skills: {', '.join(skills)}")

            return "\n".join(desc_parts) if desc_parts else ""
        except (json.JSONDecodeError, AttributeError) as e:
            logger.warning("Failed to parse __NEXT_DATA__ from %s: %s", url, e)
            return ""

    def close(self):
        """Close the httpx client."""
        self._client.close()
