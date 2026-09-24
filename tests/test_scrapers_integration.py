"""Integration tests — scrape every portal in config/urls.yaml and fail if it returns no jobs.

Scrapers are built exactly as main.py builds them (Workday location facets, city
filters), minus the max_age_days window, so a failure means the portal is broken
or has no jobs in the target locations — not just that nothing was posted this week.
"""

import os

import pytest

from config import load_config, load_urls
from main import _create_scraper, _load_workday_locations

PORTALS = load_urls()
CONFIG = {**load_config(), "max_age_days": None}
WD_LOCATIONS = _load_workday_locations()


@pytest.mark.skipif(bool(os.environ.get("SKIP_INTEGRATION")), reason="SKIP_INTEGRATION is set")
@pytest.mark.parametrize("scraper_type,url", PORTALS, ids=[f"{t}|{u}" for t, u in PORTALS])
def test_portal_returns_jobs(scraper_type, url):
    scraper = _create_scraper(scraper_type, CONFIG, url, WD_LOCATIONS)
    try:
        jobs = scraper.scrape(url)
    finally:
        scraper.close()

    assert jobs, f"{scraper_type} scraped 0 jobs from {url}"
    job = jobs[0]
    assert job.job_id and job.title and job.url, f"{scraper_type} returned an incomplete job: {job}"
