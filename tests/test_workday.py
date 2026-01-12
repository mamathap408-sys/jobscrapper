"""Tests for Workday scraper — run against a real Workday portal."""

from scrapers.workday import WorkdayScraper, _parse_workday_url, _build_api_base


def test_parse_workday_url():
    url = "https://company1.wd5.myworkdayjobs.com/en-US/External"
    company, site = _parse_workday_url(url)
    assert company == "company1"
    assert site == "External"


def test_parse_workday_url_careers():
    url = "https://mycompany.wd1.myworkdayjobs.com/en-US/Careers"
    company, site = _parse_workday_url(url)
    assert company == "mycompany"
    assert site == "Careers"


def test_build_api_base():
    url = "https://company1.wd5.myworkdayjobs.com/en-US/External"
    api_base = _build_api_base(url)
    assert api_base == "https://company1.wd5.myworkdayjobs.com/wday/cxs/company1/External"



def test_scrape_real_portal():
    """Integration test — requires network access.

    Replace the URL below with a real Workday portal to test.
    Skip this test in CI or if no network is available.
    """
    import os
    if os.environ.get("SKIP_INTEGRATION"):
        return

    # Example: Netflix jobs portal
    url = "https://explore.jobs.netflix.net/careers"
    scraper = WorkdayScraper()
    try:
        jobs = scraper.scrape(url)
        print(f"Found {len(jobs)} jobs")
        for job in jobs[:5]:
            print(f"  - {job.title} ({job.location})")
        # We just verify it doesn't crash and returns a list
        assert isinstance(jobs, list)
    finally:
        scraper.close()


if __name__ == "__main__":
    test_parse_workday_url()
    test_parse_workday_url_careers()
    test_build_api_base()
    print("Unit tests passed!")
    print("\nRunning integration test...")
    test_scrape_real_portal()
    print("All tests passed!")
