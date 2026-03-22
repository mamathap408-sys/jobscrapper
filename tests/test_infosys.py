"""Tests for Infosys scraper."""

from scrapers.infosys import InfosysScraper


def test_extract_source_id_default():
    url = "https://career.infosys.com/joblist"
    assert InfosysScraper._extract_source_id(url) == "1,21"


def test_extract_source_id_custom():
    url = "https://career.infosys.com/joblist?sourceId=41"
    assert InfosysScraper._extract_source_id(url) == "41"


def test_extract_source_id_multiple():
    url = "https://career.infosys.com/joblist?sourceId=1,21,41"
    assert InfosysScraper._extract_source_id(url) == "1,21,41"


def test_parse_created_on():
    assert InfosysScraper._parse_created_on("2026-04-30T10:28:48.282") == "2026-04-30"
    assert InfosysScraper._parse_created_on("2026-01-15T08:00:00") == "2026-01-15"
    assert InfosysScraper._parse_created_on("") == ""
    assert InfosysScraper._parse_created_on("invalid") == ""


def test_parse_job():
    scraper = InfosysScraper()
    item = {
        "referenceCode": "INFSYS-EXTERNAL-244865",
        "postingTitle": "Senior Manager - Privacy",
        "requisitionId": 244865,
        "location": "BANGALORE",
        "company": "Infosys Limited",
        "rolesResponsibilities": "Conduct employee awareness programs.",
        "technicalRequirement": "5+ years in data privacy.",
        "postingDescription": "",
        "skills": "Python, Java",
        "preferredSkills": "Cloud, AWS",
        "createdOn": "2026-04-30T10:28:48.282",
    }
    job = scraper._parse_job(item)
    assert job is not None
    assert job.job_id == "INFSYS-EXTERNAL-244865"
    assert job.title == "Senior Manager - Privacy"
    assert job.company == "Infosys Limited"
    assert job.location == "BANGALORE"
    assert "awareness programs" in job.description
    assert "Python, Java" in job.description
    assert job.posted_date == "2026-04-30"
    assert job.job_num == "244865"
    assert "jobReferenceCode=INFSYS-EXTERNAL-244865" in job.url
    scraper.close()


def test_parse_job_missing_fields():
    scraper = InfosysScraper()
    assert scraper._parse_job({}) is None
    assert scraper._parse_job({"referenceCode": "X"}) is None
    assert scraper._parse_job({"postingTitle": "Y"}) is None
    scraper.close()


def test_parse_job_strips_location_whitespace():
    scraper = InfosysScraper()
    item = {
        "referenceCode": "REF-1",
        "postingTitle": "Dev",
        "location": "PUNE                     ",
        "company": "Infosys",
        "createdOn": "",
    }
    job = scraper._parse_job(item)
    assert job.location == "PUNE"
    scraper.close()


def test_scrape_real_portal():
    """Integration test — requires network access."""
    import os
    if os.environ.get("SKIP_INTEGRATION"):
        return

    url = "https://career.infosys.com/joblist?sourceId=1,21"
    scraper = InfosysScraper(max_age_days=7)
    try:
        jobs = scraper.scrape(url)
        print(f"Found {len(jobs)} jobs within 7 days")
        for job in jobs[:5]:
            print(f"  - {job.title} ({job.location})")
        assert isinstance(jobs, list)
    finally:
        scraper.close()


if __name__ == "__main__":
    test_extract_source_id_default()
    test_extract_source_id_custom()
    test_extract_source_id_multiple()
    test_parse_created_on()
    test_parse_job()
    test_parse_job_missing_fields()
    test_parse_job_strips_location_whitespace()
    print("Unit tests passed!")
    print("\nRunning integration test...")
    test_scrape_real_portal()
    print("All tests passed!")
