"""Tests for Ashby scraper."""

from scrapers.ashby import AshbyScraper


def test_extract_organization():
    url = "https://jobs.ashbyhq.com/ema?department=engineering"
    assert AshbyScraper._extract_organization(url) == "ema"


def test_build_filters():
    url = "https://jobs.ashbyhq.com/ema?team=platform&department=engineering&keyword=python"
    filters = AshbyScraper._build_filters(url)
    assert filters == {
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
        "team": "platform",
        "department": "engineering",
        "keyword": "python",
    }
    assert scraper._matches_filters(job, item, filters) is True
    scraper.close()


def test_matches_target_cities():
    scraper = AshbyScraper()
    bengaluru_job = scraper._parse_job(
        {
            "id": "ashby-3",
            "title": "Engineer",
            "jobUrl": "https://jobs.ashbyhq.com/example/ashby-3",
            "location": {"name": "Bengaluru, India"},
        },
        "Example",
    )
    chennai_job = scraper._parse_job(
        {
            "id": "ashby-4",
            "title": "Engineer",
            "jobUrl": "https://jobs.ashbyhq.com/example/ashby-4",
            "location": {"name": "Chennai, India"},
        },
        "Example",
    )
    remote_job = scraper._parse_job(
        {
            "id": "ashby-5",
            "title": "Engineer",
            "jobUrl": "https://jobs.ashbyhq.com/example/ashby-5",
            "location": {"name": "Remote"},
        },
        "Example",
    )
    empty_location_job = scraper._parse_job(
        {
            "id": "ashby-6",
            "title": "Engineer",
            "jobUrl": "https://jobs.ashbyhq.com/example/ashby-6",
        },
        "Example",
    )
    assert bengaluru_job is not None
    assert chennai_job is not None
    assert remote_job is not None
    assert empty_location_job is not None
    assert scraper._matches_target_cities(bengaluru_job) is True
    assert scraper._matches_target_cities(chennai_job) is False
    assert scraper._matches_target_cities(remote_job) is True
    assert scraper._matches_target_cities(empty_location_job) is True
    scraper.close()
