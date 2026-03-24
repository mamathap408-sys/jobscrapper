"""Tests for Greenhouse scraper."""

from scrapers.greenhouse import GreenhouseScraper


def test_extract_board_token():
    url = "https://job-boards.greenhouse.io/devrev?department=engineering"
    assert GreenhouseScraper._extract_board_token(url) == "devrev"


def test_build_filters():
    url = "https://job-boards.greenhouse.io/devrev?department=engineering&keyword=python"
    filters = GreenhouseScraper._build_filters(url)
    assert filters == {
        "department": "engineering",
        "keyword": "python",
    }


def test_parse_job():
    scraper = GreenhouseScraper()
    item = {
        "id": 12345,
        "title": "Software Engineer",
        "absolute_url": "https://job-boards.greenhouse.io/devrev/jobs/12345",
        "updated_at": "2026-05-01T10:00:00Z",
        "location": {"name": "Bangalore, India"},
        "content": "<p>Build backend systems in Python.</p>",
        "departments": [{"name": "Engineering"}],
        "metadata": [{"name": "Req ID", "value": "REQ-123"}],
    }
    job = scraper._parse_job(item, "DevRev")
    assert job is not None
    assert job.job_id == "12345"
    assert job.title == "Software Engineer"
    assert job.company == "DevRev"
    assert job.location == "Bangalore, India"
    assert "backend systems" in job.description
    assert job.posted_date == "2026-05-01"
    assert job.job_num == "REQ-123"
    scraper.close()


def test_matches_filters():
    scraper = GreenhouseScraper()
    item = {
        "departments": [{"name": "Engineering"}],
    }
    job = scraper._parse_job(
        {
            "id": 1,
            "title": "Python Engineer",
            "absolute_url": "https://example.com/jobs/1",
            "location": {"name": "Bangalore, India"},
            "content": "<p>Python and APIs</p>",
        },
        "Example",
    )
    assert job is not None
    filters = {
        "department": "engineering",
        "keyword": "python",
    }
    assert scraper._matches_filters(job, item, filters) is True
    scraper.close()


def test_matches_target_cities():
    scraper = GreenhouseScraper()
    blr_job = scraper._parse_job(
        {
            "id": 2,
            "title": "Engineer",
            "absolute_url": "https://example.com/jobs/2",
            "location": {"name": "Bangalore, India"},
            "content": "",
        },
        "Example",
    )
    hyd_job = scraper._parse_job(
        {
            "id": 3,
            "title": "Engineer",
            "absolute_url": "https://example.com/jobs/3",
            "location": {"name": "Hyderabad, India"},
            "content": "",
        },
        "Example",
    )
    chennai_job = scraper._parse_job(
        {
            "id": 4,
            "title": "Engineer",
            "absolute_url": "https://example.com/jobs/4",
            "location": {"name": "Chennai, India"},
            "content": "",
        },
        "Example",
    )
    remote_job = scraper._parse_job(
        {
            "id": 5,
            "title": "Engineer",
            "absolute_url": "https://example.com/jobs/5",
            "location": {"name": "Remote"},
            "content": "",
        },
        "Example",
    )
    empty_location_job = scraper._parse_job(
        {
            "id": 6,
            "title": "Engineer",
            "absolute_url": "https://example.com/jobs/6",
            "content": "",
        },
        "Example",
    )
    assert blr_job is not None
    assert hyd_job is not None
    assert chennai_job is not None
    assert remote_job is not None
    assert empty_location_job is not None
    assert scraper._matches_target_cities(blr_job, {}) is True
    assert scraper._matches_target_cities(hyd_job, {}) is True
    assert scraper._matches_target_cities(chennai_job, {}) is False
    assert scraper._matches_target_cities(remote_job, {}) is True
    assert scraper._matches_target_cities(empty_location_job, {}) is True
    scraper.close()
