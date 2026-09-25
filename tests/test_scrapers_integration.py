"""Integration tests — scrape every portal in config/urls.yaml and fail if it returns no jobs
or any job without a sane posted_date.

Scrapers are built exactly as main.py builds them (Workday location facets, city
filters), minus the max_age_days window, so a failure means the portal is broken
or has no jobs in the target locations — not just that nothing was posted this week.
"""

import os
from datetime import date, timedelta
from functools import cache

import pytest

from config import load_config, load_urls
from main import _create_scraper, _load_workday_locations

PORTALS = load_urls()
CONFIG = {**load_config(), "max_age_days": None}
WD_LOCATIONS = _load_workday_locations()
NO_DATES = {"goldmansachs"}  # portals whose API gives no posted date
IDS = [f"{t}|{u}" for t, u in PORTALS]
integration = pytest.mark.skipif(bool(os.environ.get("SKIP_INTEGRATION")), reason="SKIP_INTEGRATION is set")


@cache
def _scrape(scraper_type, url):
    """Scrape each portal once and share the result across tests."""
    scraper = _create_scraper(scraper_type, CONFIG, url, WD_LOCATIONS)
    try:
        return scraper.scrape(url)
    finally:
        scraper.close()


@integration
@pytest.mark.parametrize("scraper_type,url", PORTALS, ids=IDS)
def test_portal_returns_jobs(scraper_type, url):
    jobs = _scrape(scraper_type, url)
    assert jobs, f"{scraper_type} scraped 0 jobs from {url}"
    job = jobs[0]
    assert job.job_id and job.title and job.url, f"{scraper_type} returned an incomplete job: {job}"


@integration
@pytest.mark.parametrize("scraper_type,url", PORTALS, ids=IDS)
def test_portal_dates_are_valid(scraper_type, url):
    """Every job has a posted_date that isn't in the future (None means parsing failed)."""
    if scraper_type in NO_DATES:
        pytest.skip(f"{scraper_type} has no posted dates")
    latest = date.today() + timedelta(days=1)  # UTC vs local timezone slack
    jobs = _scrape(scraper_type, url)
    missing = sum(job.posted_date is None for job in jobs)
    future = [job.posted_date for job in jobs if job.posted_date and job.posted_date > latest]
    assert not missing, f"{scraper_type}: {missing}/{len(jobs)} jobs have unparseable dates from {url} (see warnings)"
    assert not future, f"{scraper_type}: future posted_date(s) from {url}: {future[:5]}"
