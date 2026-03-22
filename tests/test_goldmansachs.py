"""Tests for Goldman Sachs scraper."""

from scrapers.goldmansachs import GoldmanSachsScraper


def test_build_filters_empty():
    url = "https://higher.gs.com/roles"
    filters = GoldmanSachsScraper._build_filters(url)
    assert filters == []


def test_build_filters_location():
    url = "https://higher.gs.com/roles?location=Bangalore"
    filters = GoldmanSachsScraper._build_filters(url)
    assert len(filters) == 1
    assert filters[0]["filterCategoryType"] == "LOCATION"
    assert filters[0]["filters"][0]["filter"] == "Bangalore"


def test_build_filters_multiple():
    url = "https://higher.gs.com/roles?location=Bangalore&division=Engineering"
    filters = GoldmanSachsScraper._build_filters(url)
    assert len(filters) == 2
    types = {f["filterCategoryType"] for f in filters}
    assert types == {"LOCATION", "DIVISION"}


def test_parse_job():
    scraper = GoldmanSachsScraper()
    item = {
        "roleId": "12345",
        "jobTitle": "Software Engineer",
        "corporateTitle": "Vice President",
        "division": "Engineering",
        "jobFunction": "Technology",
        "skills": ["Python", "Java", "AWS"],
        "locations": [
            {"primary": True, "city": "Bangalore", "state": "Karnataka", "country": "India"},
            {"primary": False, "city": "Hyderabad", "state": "Telangana", "country": "India"},
        ],
        "jobType": {"code": "FT", "description": "Full-Time"},
        "externalSource": {"sourceId": "99999"},
        "status": "Open",
    }
    job = scraper._parse_job(item)
    assert job is not None
    assert job.job_id == "12345"
    assert job.title == "Software Engineer"
    assert job.company == "Goldman Sachs"
    assert "Bangalore, India" in job.location
    assert "Hyderabad, India" in job.location
    assert "Vice President" in job.description
    assert "Python" in job.description
    assert job.url == "https://higher.gs.com/roles/99999"
    assert job.job_num == "99999"
    scraper.close()


def test_parse_job_missing_fields():
    scraper = GoldmanSachsScraper()
    assert scraper._parse_job({}) is None
    assert scraper._parse_job({"roleId": "1"}) is None
    assert scraper._parse_job({"jobTitle": "Dev"}) is None
    scraper.close()


def test_parse_job_no_location():
    scraper = GoldmanSachsScraper()
    item = {
        "roleId": "1",
        "jobTitle": "Analyst",
        "locations": [],
        "externalSource": {"sourceId": "100"},
    }
    job = scraper._parse_job(item)
    assert job is not None
    assert job.location == ""
    scraper.close()


def test_parse_job_no_external_source():
    scraper = GoldmanSachsScraper()
    item = {
        "roleId": "42",
        "jobTitle": "Analyst",
        "externalSource": None,
    }
    job = scraper._parse_job(item)
    assert job is not None
    assert job.url == "https://higher.gs.com/roles/42"
    scraper.close()


def test_scrape_real_portal():
    """Integration test — requires network access."""
    import os
    if os.environ.get("SKIP_INTEGRATION"):
        return

    url = "https://higher.gs.com/roles?location=Bangalore"
    scraper = GoldmanSachsScraper()
    try:
        jobs = scraper.scrape(url)
        print(f"Found {len(jobs)} jobs")
        for job in jobs[:5]:
            print(f"  - {job.title} ({job.location})")
        assert isinstance(jobs, list)
    finally:
        scraper.close()


if __name__ == "__main__":
    test_build_filters_empty()
    test_build_filters_location()
    test_build_filters_multiple()
    test_parse_job()
    test_parse_job_missing_fields()
    test_parse_job_no_location()
    test_parse_job_no_external_source()
    print("Unit tests passed!")
    print("\nRunning integration test...")
    test_scrape_real_portal()
    print("All tests passed!")
