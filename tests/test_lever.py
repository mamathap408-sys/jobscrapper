"""Tests for Lever scraper."""

from scrapers.lever import LeverScraper


def test_extract_site():
    url = "https://jobs.lever.co/hevodata?location=bangalore"
    assert LeverScraper._extract_site(url) == "hevodata"


def test_build_filters():
    url = "https://jobs.lever.co/hevodata?location=bangalore&team=engineering&commitment=full-time&keyword=python"
    filters = LeverScraper._build_filters(url)
    assert filters == {
        "location": "bangalore",
        "team": "engineering",
        "commitment": "full-time",
        "keyword": "python",
    }


def test_parse_job():
    scraper = LeverScraper()
    item = {
        "id": "lever-1",
        "text": "Backend Engineer",
        "hostedUrl": "https://jobs.lever.co/hevodata/lever-1",
        "createdAt": 1_746_067_200_000,
        "descriptionPlain": "Build data products.",
        "categories": {
            "location": "Bangalore, India",
            "team": "Engineering",
            "commitment": "Full-time",
            "allLocations": ["Bangalore, India", "Remote"],
        },
        "lists": [
            {"text": "Requirements", "content": "<ul><li>Python</li></ul>"},
        ],
    }
    job = scraper._parse_job(item, "Hevo Data")
    assert job is not None
    assert job.job_id == "lever-1"
    assert job.title == "Backend Engineer"
    assert job.company == "Hevo Data"
    assert "Bangalore, India" in job.location
    assert "Requirements: Python" in job.description
    assert job.url == "https://jobs.lever.co/hevodata/lever-1"
    assert job.job_num == "lever-1"
    scraper.close()


def test_matches_filters():
    scraper = LeverScraper()
    item = {
        "categories": {
            "location": "Bangalore, India",
            "team": "Engineering",
            "commitment": "Full-time",
        }
    }
    job = scraper._parse_job(
        {
            "id": "lever-2",
            "text": "Python Engineer",
            "hostedUrl": "https://jobs.lever.co/example/lever-2",
            "descriptionPlain": "Python backend role",
            "categories": item["categories"],
        },
        "Example",
    )
    assert job is not None
    filters = {
        "location": "bangalore",
        "team": "engineering",
        "commitment": "full-time",
        "keyword": "python",
    }
    assert scraper._matches_filters(job, item, filters) is True
    scraper.close()
