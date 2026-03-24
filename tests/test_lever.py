"""Tests for Lever scraper."""

from scrapers.lever import LeverScraper


def test_extract_site():
    url = "https://jobs.lever.co/hevodata?team=engineering"
    assert LeverScraper._extract_site(url) == "hevodata"


def test_build_filters():
    url = "https://jobs.lever.co/hevodata?team=engineering&commitment=full-time&keyword=python"
    filters = LeverScraper._build_filters(url)
    assert filters == {
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
        "team": "engineering",
        "commitment": "full-time",
        "keyword": "python",
    }
    assert scraper._matches_filters(job, item, filters) is True
    scraper.close()


def test_matches_target_cities():
    scraper = LeverScraper()
    blr_job = scraper._parse_job(
        {
            "id": "lever-3",
            "text": "Engineer",
            "hostedUrl": "https://jobs.lever.co/example/lever-3",
            "categories": {"location": "Bangalore, India"},
        },
        "Example",
    )
    hyd_job = scraper._parse_job(
        {
            "id": "lever-4",
            "text": "Engineer",
            "hostedUrl": "https://jobs.lever.co/example/lever-4",
            "categories": {"location": "Hyderabad, India"},
        },
        "Example",
    )
    pune_job = scraper._parse_job(
        {
            "id": "lever-5",
            "text": "Engineer",
            "hostedUrl": "https://jobs.lever.co/example/lever-5",
            "categories": {"location": "Pune, India"},
        },
        "Example",
    )
    remote_job = scraper._parse_job(
        {
            "id": "lever-6",
            "text": "Engineer",
            "hostedUrl": "https://jobs.lever.co/example/lever-6",
            "categories": {"location": "Remote"},
        },
        "Example",
    )
    empty_location_job = scraper._parse_job(
        {
            "id": "lever-7",
            "text": "Engineer",
            "hostedUrl": "https://jobs.lever.co/example/lever-7",
            "categories": {},
        },
        "Example",
    )
    assert blr_job is not None
    assert hyd_job is not None
    assert pune_job is not None
    assert remote_job is not None
    assert empty_location_job is not None
    assert scraper._matches_target_cities(blr_job) is True
    assert scraper._matches_target_cities(hyd_job) is True
    assert scraper._matches_target_cities(pune_job) is False
    assert scraper._matches_target_cities(remote_job) is True
    assert scraper._matches_target_cities(empty_location_job) is True
    scraper.close()
