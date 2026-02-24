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
import re
from datetime import datetime, timezone
from xml.etree import ElementTree

import httpx

from scrapers.base import BaseScraper, JobPosting

logger = logging.getLogger(__name__)

# Cities we want to match (case-insensitive)
_DEFAULT_CITIES = ["bangalore", "bengaluru", "hyderabad", "banglore"]


def _strip_html(html: str) -> str:
    """Remove HTML tags from a string."""
    clean = re.sub(r"<[^>]+>", " ", html)
    clean = re.sub(r"\s+", " ", clean)
    return clean.strip()


def _parse_rfc2822_date(date_str: str) -> str:
    """Parse RFC 2822 date to YYYY-MM-DD string."""
    if not date_str:
        return ""
    try:
        # RFC 2822: "Mon, 14 Apr 2026 00:00:00 GMT"
        dt = datetime.strptime(date_str.strip(), "%a, %d %b %Y %H:%M:%S %Z")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        try:
            # Fallback: without timezone name
            dt = datetime.strptime(date_str.strip()[:25], "%a, %d %b %Y %H:%M:%S")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return date_str


class CognizantScraper(BaseScraper):
    """Scrapes Cognizant careers via their public India XML feed."""

    def __init__(self, cities: list[str] | None = None, max_age_days: int | None = None, **kwargs):
        """Initialize with city filter and optional age limit.

        Args:
            cities:       List of city names to filter for (case-insensitive).
                          Defaults to ["bangalore", "bengaluru", "hyderabad"].
            max_age_days: Only include jobs posted within this many days. None = no limit.
        """
        self._cities = [c.lower() for c in (cities or _DEFAULT_CITIES)]
        self._max_age_days = max_age_days
        self._client = httpx.Client(
            timeout=60,  # Large XML response
            verify=False,
            headers={
                "Accept": "application/xml, text/xml",
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/125.0.0.0 Safari/537.36",
            },
            follow_redirects=True,
        )

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
            country = self._get_text(item, "country")

            # Build location string from city (e.g., "BANGALORE" -> "Bangalore, India")
            city_clean = city_text.split(",")[0].strip().title()
            location = f"{city_clean}, {state}" if state else f"{city_clean}, India"

            posted_date = _parse_rfc2822_date(date_str)
            description = _strip_html(description_html) if description_html else ""
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

        # Filter by age
        if self._max_age_days is not None:
            now = datetime.now(timezone.utc)
            before = len(jobs)
            jobs = [j for j in jobs if self._within_age(j.posted_date, now)]
            logger.info(
                "Cognizant: %d total in feed, %d in target cities, %d within %d days (skipped %d older)",
                total_parsed, before, len(jobs), self._max_age_days, before - len(jobs),
            )
        else:
            logger.info(
                "Cognizant: %d total in feed, %d in target cities",
                total_parsed, len(jobs),
            )

        return jobs

    def _matches_city(self, city_text: str) -> bool:
        """Check if city_text matches any of the target cities."""
        lower = city_text.lower()
        return any(c in lower for c in self._cities)

    def _within_age(self, posted_date: str, now: datetime) -> bool:
        """Check if a posted date is within max_age_days."""
        if not posted_date or self._max_age_days is None:
            return True
        try:
            dt = datetime.strptime(posted_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            return (now - dt).days <= self._max_age_days
        except ValueError:
            return True  # Keep jobs with unparseable dates

    @staticmethod
    def _get_text(item, tag: str) -> str:
        """Get text content of a child element, handling CDATA."""
        el = item.find(tag)
        if el is None:
            return ""
        return (el.text or "").strip()

    def close(self):
        """Close the httpx client."""
        self._client.close()
