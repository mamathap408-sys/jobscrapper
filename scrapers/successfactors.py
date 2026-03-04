"""
scrapers/successfactors.py — SAP SuccessFactors RSS Scraper
=============================================================
Scrapes job postings from career sites powered by SAP SuccessFactors Recruiting Marketing.

How it works:
  - RSS endpoint: https://{domain}/services/rss/job/?locale=en_US&keywords=(locationSearch:(City))&rows=N
  - Returns job listings with full HTML descriptions, sorted by most recent first.
  - No authentication needed — public unauthenticated GET.
  - Queries each target city separately and deduplicates.

The `rows` parameter controls how many results are returned (default 20 if omitted).
Some sites (e.g., Wipro) support rows=500+ for full coverage; others (Capgemini, HCLTech)
ignore it and always return max ~20. The scraper probes with rows=500 and works with
whatever the site returns.

Companies using this platform (confirmed with India jobs):
  - Wipro:       careers.wipro.com       (rows works — 3500+ Bengaluru jobs available)
  - Capgemini:   careers.capgemini.com   (rows ignored — max ~20 per city)
  - HCLTech:     careers.hcltech.com     (rows ignored — sparse results)
  - LTIMindtree: careers.ltimindtree.com (rows ignored — sparse results)
"""

import logging
import re
from datetime import datetime, timezone
from urllib.parse import urlparse
from xml.etree import ElementTree

import httpx

from scrapers.base import BaseScraper, JobPosting

logger = logging.getLogger(__name__)

# Default cities to search for Hyderabad + Bangalore
_DEFAULT_CITIES = ["Hyderabad", "Bengaluru", "Bangalore", "Banglore"]

# How many RSS rows to request per city query.
# Sites that support it return up to this many; others ignore it and return ~20.
_DEFAULT_RSS_ROWS = 5000

# RSS namespace
_NAMESPACES = {
    "content": "http://purl.org/rss/1.0/modules/content/",
}


def _strip_html(html: str) -> str:
    """Remove HTML tags from a string."""
    clean = re.sub(r"<[^>]+>", " ", html)
    clean = re.sub(r"\s+", " ", clean)
    return clean.strip()


def _parse_rfc2822_date(date_str: str) -> str:
    """Parse RSS pubDate to YYYY-MM-DD string."""
    if not date_str:
        return ""
    formats = [
        "%a, %d %b %Y %H:%M:%S %Z",
        "%a, %d %b %Y %H:%M:%S %z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return date_str


def _extract_rss_id(guid_text: str) -> str:
    """Extract a stable ID from the RSS guid/link URL.

    URLs look like: .../job/City-Title-PostalCode/1326877455/?feedId=...
    We extract the numeric ID.
    """
    if not guid_text:
        return ""
    match = re.search(r"/(\d{7,})/", guid_text)
    if match:
        return match.group(1)
    return guid_text


class SuccessFactorsScraper(BaseScraper):
    """Scrapes SAP SuccessFactors career sites via their public RSS feed."""

    def __init__(self, cities: list[str] | None = None, max_age_days: int | None = None,
                 rss_rows: int = _DEFAULT_RSS_ROWS, **kwargs):
        """Initialize with city search terms and optional limits.

        Args:
            cities:       List of city names to search for via locationSearch.
                          Defaults to ["Hyderabad", "Bengaluru", "Bangalore"].
            max_age_days: Only include jobs posted within this many days. None = no limit.
            rss_rows:     Max rows to request per RSS query. Sites that support it
                          (Wipro) return up to this many; others ignore it.
        """
        self._cities = cities or _DEFAULT_CITIES
        self._max_age_days = max_age_days
        self._rss_rows = rss_rows
        self._client = httpx.Client(
            timeout=60,  # Large RSS responses can be slow
            verify=False,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/125.0.0.0 Safari/537.36",
            },
            follow_redirects=True,
        )

    def scrape(self, url: str) -> list[JobPosting]:
        """Scrape jobs from SuccessFactors RSS feed for all target cities.

        Args:
            url: The base career site URL (e.g., https://careers.wipro.com).

        Returns:
            List of deduplicated JobPosting objects across all city queries.
        """
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.hostname}"
        company = self._extract_company(parsed.hostname)

        logger.info("Scraping SuccessFactors RSS: %s (cities: %s, rows: %d)",
                    base, self._cities, self._rss_rows)

        seen_ids = set()
        all_jobs = []

        for city in self._cities:
            feed_url = (
                f"{base}/services/rss/job/"
                f"?locale=en_US&keywords=(locationSearch:({city}))&rows={self._rss_rows}"
            )
            jobs = self._fetch_feed(feed_url, company)
            for job in jobs:
                if job.job_id not in seen_ids:
                    seen_ids.add(job.job_id)
                    all_jobs.append(job)

        # Filter by age
        if self._max_age_days is not None:
            now = datetime.now(timezone.utc)
            before = len(all_jobs)
            all_jobs = [j for j in all_jobs if self._within_age(j.posted_date, now)]
            logger.info(
                "%s: %d unique jobs, %d within %d days (skipped %d)",
                company, before, len(all_jobs), self._max_age_days, before - len(all_jobs),
            )
        else:
            logger.info("%s: %d unique jobs across cities", company, len(all_jobs))

        return all_jobs

    def _fetch_feed(self, url: str, company: str) -> list[JobPosting]:
        """Fetch and parse a single RSS feed URL."""
        try:
            resp = self._client.get(url)
            resp.raise_for_status()
        except httpx.HTTPError as e:
            logger.error("SuccessFactors RSS fetch failed for %s: %s", url, e)
            return []

        try:
            root = ElementTree.fromstring(resp.content)
        except ElementTree.ParseError as e:
            logger.error("Failed to parse RSS XML from %s: %s", url, e)
            return []

        jobs = []
        channel = root.find("channel")
        if channel is None:
            items = root.iter("item")
        else:
            items = channel.iter("item")

        for item in items:
            title = self._get_text(item, "title")

            # Skip placeholder items
            if "no jobs currently available" in title.lower():
                continue

            link = self._get_text(item, "link")
            guid = self._get_text(item, "guid")
            pub_date = self._get_text(item, "pubDate")
            description_html = self._get_text(item, "description")

            # Try content:encoded for full description
            content_encoded = item.find("content:encoded", _NAMESPACES)
            if content_encoded is not None and content_encoded.text:
                description_html = content_encoded.text

            location = self._extract_location(title, description_html)
            job_id = _extract_rss_id(guid) or _extract_rss_id(link) or title
            posted_date = _parse_rfc2822_date(pub_date)
            description = _strip_html(description_html) if description_html else ""

            jobs.append(JobPosting(
                job_id=str(job_id),
                title=title,
                company=company,
                location=location,
                description=description,
                url=link,
                posted_date=posted_date,
            ))

        logger.info("  %s/%s: %d items from RSS", company, url.split("locationSearch:(")[1].split(")")[0], len(jobs))
        return jobs

    # ─── Helpers ──────────────────────────────────────────────────────────────────

    def _extract_location(self, title: str, description: str) -> str:
        """Try to extract location from title or description."""
        # Check title for location in parentheses like "(Bengaluru, IND-29, IN, 560035)"
        match = re.search(r"\(([^)]*(?:Bengaluru|Hyderabad|Bangalore)[^)]*)\)", title, re.IGNORECASE)
        if match:
            city = match.group(1).split(",")[0].strip()
            return f"{city}, India"

        # Check for city in title text
        title_lower = title.lower()
        for city in self._cities:
            if city.lower() in title_lower:
                return f"{city}, India"

        # Fallback: check description
        desc_lower = (description or "").lower()
        for city in self._cities:
            if city.lower() in desc_lower:
                return f"{city}, India"

        return "India"

    @staticmethod
    def _extract_company(hostname: str) -> str:
        """Extract company name from hostname."""
        parts = hostname.split(".")
        for part in parts:
            if part not in ("careers", "jobs", "www", "com", "in", "co"):
                return part.capitalize()
        return parts[0]

    def _within_age(self, posted_date: str, now: datetime) -> bool:
        """Check if a posted date is within max_age_days."""
        if not posted_date or self._max_age_days is None:
            return True
        try:
            dt = datetime.strptime(posted_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            return (now - dt).days <= self._max_age_days
        except ValueError:
            return True

    @staticmethod
    def _get_text(item, tag: str) -> str:
        """Get text content of a child element."""
        el = item.find(tag)
        if el is None:
            return ""
        return (el.text or "").strip()

    def close(self):
        """Close the httpx client."""
        self._client.close()
