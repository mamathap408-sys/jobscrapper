"""Tests for Greenhouse scraper."""

from scrapers.greenhouse import GreenhouseScraper


def test_extract_board_token():
    url = "https://job-boards.greenhouse.io/devrev?location=bangalore"
    assert GreenhouseScraper._extract_board_token(url) == "devrev"


def test_build_filters():
    url = "https://job-boards.greenhouse.io/devrev?location=bangalore&department=engineering&keyword=python"
    filters = GreenhouseScraper._build_filters(url)
    assert filters == {
        "location": "bangalore",
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
        "location": "bangalore",
        "department": "engineering",
        "keyword": "python",
    }
    assert scraper._matches_filters(job, item, filters) is True
    scraper.close()
