"""
scrapers/cognizant.py — Cognizant Careers XML Feed Scraper
===========================================================
Scrapes job postings from Cognizant's public XML/RSS feed.

How it works:
  - Feed URL: https://careers.cognizant.com/india-en/jobs/xml/?rss=true
  - Returns ALL India jobs (~1,700+) in a single XML response with full descriptions.
  - No authentication, no pagination needed.
  - Filter by <city> field for Hyderabad/Bangalore after parsing.

The city field uses format "CityName,StateCode" (e.g., "Bangalore,KA", "Hyderabad,TG").
"""

import logging
from xml.etree import ElementTree

import httpx

from scrapers.base import BaseScraper, JobPosting, strip_html, date_or_none

logger = logging.getLogger(__name__)

# Cities we want to match (case-insensitive)
_DEFAULT_CITIES = ["bangalore", "bengaluru", "hyderabad", "banglore"]


class CognizantScraper(BaseScraper):
    """Scrapes Cognizant careers via their public India XML feed."""

    def __init__(self, cities: list[str] | None = None, **kwargs):
        """Initialize with city filter and optional age limit.

        Args:
            cities:       List of city names to filter for (case-insensitive).
                          Defaults to ["bangalore", "bengaluru", "hyderabad"].
        """
        super().__init__(
            headers={"Accept": "application/xml, text/xml"},
            timeout=60,  # Large XML response
            **kwargs,
        )
        self._cities = [c.lower() for c in (cities or _DEFAULT_CITIES)]

    def scrape(self, url: str) -> list[JobPosting]:
        """Fetch the Cognizant India XML feed and filter for target cities.

        Args:
            url: The XML feed URL (from urls.yaml).

        Returns:
            List of JobPosting objects for jobs in target cities.
        """
        logger.info("Fetching Cognizant XML feed: %s", url)
        try:
            resp = self._client.get(url)
            resp.raise_for_status()
        except httpx.HTTPError as e:
            logger.error("Cognizant XML feed request failed: %s", e)
            return []

        try:
            root = ElementTree.fromstring(resp.content)
        except ElementTree.ParseError as e:
            logger.error("Failed to parse Cognizant XML: %s", e)
            return []

        # Feed uses <job> elements (not <item>)
        jobs = []
        total_parsed = 0
        for item in root.iter("job"):
            total_parsed += 1
            city_el = item.find("city")
            city_text = (city_el.text or "").strip() if city_el is not None else ""

            # Filter by city
            if not self._matches_city(city_text):
                continue

            title = self._get_text(item, "title")
            job_url = self._get_text(item, "url") or self._get_text(item, "link")
            description_html = self._get_text(item, "description")
            date_str = self._get_text(item, "date")
            req_id = self._get_text(item, "requisitionid")
            state = self._get_text(item, "state")

            # Build location string from city (e.g., "BANGALORE" -> "Bangalore, India")
            city_clean = city_text.split(",")[0].strip().title()
            location = f"{city_clean}, {state}" if state else f"{city_clean}, India"

            posted_date = date_or_none(date_str)
            description = strip_html(description_html) if description_html else ""
            job_id = req_id or (job_url if job_url else title)

            jobs.append(JobPosting(
                job_id=job_id,
                title=title,
                company="Cognizant",
                location=location,
                description=description,
                url=job_url,
                posted_date=posted_date,
                job_num=req_id,
            ))

        jobs = [job for job in jobs if self._within_age(job.posted_date)]
        logger.info("Cognizant: %d total in feed, %d in target cities", total_parsed, len(jobs))
        return jobs

    def _matches_city(self, city_text: str) -> bool:
        """Check if city_text matches any of the target cities."""
        lower = city_text.lower()
        return any(c in lower for c in self._cities)

    @staticmethod
    def _get_text(item, tag: str) -> str:
        """Get text content of a child element, handling CDATA."""
        el = item.find(tag)
        if el is None:
            return ""
        return (el.text or "").strip()
