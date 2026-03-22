"""Tests for Ashby scraper."""

from scrapers.ashby import AshbyScraper


def test_extract_organization():
    url = "https://jobs.ashbyhq.com/ema?location=bengaluru"
    assert AshbyScraper._extract_organization(url) == "ema"


def test_build_filters():
    url = "https://jobs.ashbyhq.com/ema?location=bengaluru&team=platform&department=engineering&keyword=python"
    filters = AshbyScraper._build_filters(url)
    assert filters == {
        "location": "bengaluru",
        "team": "platform",
        "department": "engineering",
        "keyword": "python",
    }


def test_parse_job():
    scraper = AshbyScraper()
    item = {
        "id": "ashby-1",
        "jobPostingId": "JP-10",
        "title": "Platform Engineer",
        "jobUrl": "https://jobs.ashbyhq.com/ema/ashby-1",
        "publishedDate": "2026-05-01T12:30:00Z",
        "descriptionHtml": "<p>Build platform tooling with Python.</p>",
        "location": {"name": "Bengaluru, India"},
        "secondaryLocations": [{"name": "Remote"}],
        "team": {"name": "Platform"},
        "department": {"name": "Engineering"},
        "employmentType": "Full-time",
    }
    job = scraper._parse_job(item, "Ema")
    assert job is not None
    assert job.job_id == "ashby-1"
    assert job.title == "Platform Engineer"
    assert job.company == "Ema"
    assert "Bengaluru, India" in job.location
    assert "Remote" in job.location
    assert "platform tooling" in job.description
    assert job.posted_date == "2026-05-01"
    assert job.job_num == "JP-10"
    scraper.close()


def test_matches_filters():
    scraper = AshbyScraper()
    item = {
        "team": {"name": "Platform"},
        "department": {"name": "Engineering"},
    }
    job = scraper._parse_job(
        {
            "id": "ashby-2",
            "title": "Python Engineer",
            "jobUrl": "https://jobs.ashbyhq.com/example/ashby-2",
            "descriptionHtml": "<p>Python backend role</p>",
            "location": {"name": "Bengaluru, India"},
            "team": item["team"],
            "department": item["department"],
        },
        "Example",
    )
    assert job is not None
    filters = {
        "location": "bengaluru",
        "team": "platform",
        "department": "engineering",
        "keyword": "python",
    }
    assert scraper._matches_filters(job, item, filters) is True
    scraper.close()
