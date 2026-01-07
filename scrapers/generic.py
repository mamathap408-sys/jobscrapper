"""
scrapers/generic.py — Generic Playwright Scraper
==================================================
Fallback scraper for career pages that don't have a known API (non-Workday sites).
Uses Playwright (headless Chromium browser) to render the page like a real browser would.

How it works:
  1. Opens the URL in headless Chromium
  2. Waits for the page to fully load (networkidle)
  3. Scrolls down 3 times to trigger lazy-loaded content
  4. Searches for links matching common job listing patterns (CSS selectors)
  5. Extracts the link text as the job title and the href as the job URL

Limitations:
  - Only finds basic link-based job listings
  - Doesn't extract location, description, or posted date
  - May need customization per site (different CSS selectors)
  - Requires Playwright browser to be installed: `playwright install chromium`

This is a starting point — for specific non-Workday sites, you may want to
create a dedicated scraper with site-specific selectors.
"""

import logging
from urllib.parse import urlparse, urljoin

from scrapers.base import BaseScraper, JobPosting, generate_fallback_id

logger = logging.getLogger(__name__)

_COMMON_SUBDOMAINS = {"www", "jobs", "careers", "career", "recruiting", "hire", "apply", "work"}


class GenericScraper(BaseScraper):
    """Headless browser scraper using Playwright for unknown career pages."""

    def __init__(self, browser=None, **kwargs):
        """Initialize with a Playwright browser instance.

        Args:
            browser: A Playwright Browser object (created in main.py).
                     If None, scraping will fail gracefully with empty results.
        """
        self._browser = browser

    def scrape(self, url: str) -> list[JobPosting]:
        """Open a career page in headless Chrome and extract job links.

        Args:
            url: The career page URL to scrape.

        Returns:
            List of JobPosting objects. Location and description will be empty.
        """
        if self._browser is None:
            logger.error("GenericScraper requires a Playwright browser instance")
            return []

        # Extract company name from hostname (e.g., "danaher" from "jobs.danaher.com")
        parsed = urlparse(url)
        parts = parsed.hostname.split(".")
        # Skip common subdomains to find the actual company name
        company = next((p for p in parts if p not in _COMMON_SUBDOMAINS), parts[0])
        logger.info("Scraping (generic) %s", url)

        page = self._browser.new_page()
        try:
            # Load the page and wait for all network requests to finish
            page.goto(url, wait_until="networkidle", timeout=30000)

            # Scroll down 3 times to trigger lazy-loaded job cards
            for _ in range(3):
                page.evaluate("window.scrollBy(0, window.innerHeight)")
                page.wait_for_timeout(1000)

            jobs = self._extract_jobs(page, url, company)
            logger.info("Found %d jobs from %s (generic)", len(jobs), url)
            return jobs
        except Exception as e:
            logger.error("Generic scraper failed for %s: %s", url, e)
            return []
        finally:
            page.close()  # Always close the page to free memory

    def _extract_jobs(
        self, page, base_url: str, company: str
    ) -> list[JobPosting]:
        """Search the page for job listing links using common CSS patterns.

        Tries multiple CSS selectors that commonly match job listing links on
        career pages. Deduplicates by URL to avoid counting the same job twice.

        Args:
            page:     Playwright Page object (already loaded).
            base_url: The career page URL (for resolving relative links).
            company:  Company name (extracted from hostname).

        Returns:
            List of JobPosting objects found on the page.
        """
        # CSS selectors that commonly match job links on career pages
        selectors = [
            "a[href*='job']",        # Links containing "job" in the URL
            "a[href*='position']",   # Links containing "position"
            "a[href*='career']",     # Links containing "career"
            "a[href*='opening']",    # Links containing "opening"
            ".job-listing a",        # Links inside .job-listing containers
            ".job-card a",           # Links inside .job-card containers
            ".careers-list a",       # Links inside .careers-list containers
            "[data-job] a",          # Links inside elements with data-job attribute
            ".opening a",            # Links inside .opening containers
        ]

        seen_urls = set()  # Dedup by URL
        jobs = []

        for selector in selectors:
            elements = page.query_selector_all(selector)
            for el in elements:
                href = el.get_attribute("href") or ""
                text = (el.inner_text() or "").strip()

                # Skip empty or too-short links (likely navigation, not job titles)
                if not href or not text or len(text) < 5:
                    continue

                # Convert relative URLs to absolute
                full_url = urljoin(base_url, href)
                if full_url in seen_urls:
                    continue
                seen_urls.add(full_url)

                # Use URL hash as the job ID (no better identifier available)
                job_id = generate_fallback_id(full_url)
                jobs.append(JobPosting(
                    job_id=job_id,
                    title=text[:200],  # Truncate overly long link text
                    company=company,
                    location="",       # Not available from generic scraping
                    description="",    # Not available from generic scraping
                    url=full_url,
                    posted_date="",    # Not available from generic scraping
                ))

        return jobs
