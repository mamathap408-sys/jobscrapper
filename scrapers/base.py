"""
scrapers/base.py — Base Scraper Interface
==========================================
Defines the standard data format (JobPosting) and the abstract interface (BaseScraper)
that all scrapers must implement.

Every scraper (Workday, generic, or custom) must:
  1. Inherit from BaseScraper
  2. Implement the scrape(url) method
  3. Return a list of JobPosting objects

This ensures all scrapers produce consistent output that the matcher, database,
and notifier can work with regardless of where the jobs came from.
"""

import hashlib
from dataclasses import dataclass


@dataclass
class JobProfile:
    """The user's job search profile, loaded from config.yaml."""
    title: str
    skills: list[str]
    tools: list[str]
    experience_years: float
    location_preference: str
    additional_criteria: str = ""


@dataclass
class JobPosting:
    """Represents a single job posting scraped from a career portal.

    Attributes:
        job_id:      Unique identifier for deduplication.
                     From the site's API (e.g., Workday externalPath) or a hash of the URL.
        title:       Job title, e.g., "Senior Backend Engineer"
        company:     Company name, extracted from the portal URL or API response.
        location:    Job location, e.g., "Bangalore, India" or "Remote". Empty if unknown.
        description: Full job description text (HTML tags stripped).
                     Used by the GenAI matcher to score relevance.
                     May be empty initially and filled in later (e.g., Workday detail fetch).
        url:         Direct link to the job posting page (for the email digest).
        posted_date: When the job was posted, e.g., "2024-01-15". Empty if unavailable.
        job_num:     Job number / requisition ID from the portal (e.g., "R1308276"). Empty if unavailable.
    """
    job_id: str
    title: str
    company: str
    location: str
    description: str
    url: str
    posted_date: str
    job_num: str = ""


def generate_fallback_id(text: str) -> str:
    """Generate a stable fallback job ID from arbitrary text using MD5."""
    return hashlib.md5(text.encode()).hexdigest()


class BaseScraper:
    """Abstract base class for all site scrapers.

    To add a new scraper:
      1. Create a new file in scrapers/ (e.g., scrapers/greenhouse.py)
      2. Create a class that inherits from BaseScraper
      3. Implement the scrape() method
      4. Register it in scrapers/__init__.py SCRAPER_REGISTRY
      5. Use the new type label in urls.yaml (e.g., "greenhouse | https://...")
    """

    def scrape(self, url: str) -> list[JobPosting]:
        """Scrape the given career portal URL and return job postings.

        Args:
            url: The career portal URL to scrape.

        Returns:
            List of JobPosting objects found on the page.
        """
        raise NotImplementedError
