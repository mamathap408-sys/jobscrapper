"""Tests for Visa scraper parsing, pagination, filtering, and failures."""

from datetime import datetime, timezone

import httpx

from scrapers.visa import VisaScraper, _parse_age_days, _strip_html


class DummyResponse:
    def __init__(self, payload=None, status_code=200, json_error=False):
        self._payload = payload
        self.status_code = status_code
        self._json_error = json_error
        self.text = "not json"

    def raise_for_status(self):
        if self.status_code >= 400:
            request = httpx.Request("POST", "https://search.visa.com/CAREERS/careers/jobs?q=")
            response = httpx.Response(self.status_code, request=request)
            raise httpx.HTTPStatusError("boom", request=request, response=response)

    def json(self):
        if self._json_error:
            raise ValueError("invalid json")
        return self._payload


class DummyClient:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def post(self, url, json):
        self.calls.append((url, json))
        return self._responses.pop(0)

    def close(self):
        return None


def _sample_job(**overrides):
    job = {
        "refNumber": "REF123",
        "postingId": "744000000000001",
        "jobTitle": "Software Engineer",
        "jobDescription": "<p>Build APIs</p>",
        "qualifications": "<ul><li>Python</li></ul>",
        "additionalInformation": "<p>Remote eligible</p>",
        "city": "Bangalore",
        "region": "INDIA",
        "country": "India",
        "createdOn": "2026-04-17T10:00:00.000Z",
        "applyUrl": "https://jobs.smartrecruiters.com/Visa/744000000000001-role",
    }
    job.update(overrides)
    return job


def test_strip_html():
    assert _strip_html("<p>Hello &amp; <b>world</b></p>") == "Hello & world"


def test_parse_age_days():
    now = datetime(2026, 4, 18, 10, 0, tzinfo=timezone.utc)
    assert _parse_age_days("2026-04-17T10:00:00.000Z", now) == 1
    assert _parse_age_days("bad-date", now) is None


def test_build_job_maps_fields():
    scraper = VisaScraper(client=DummyClient([]))
    job = scraper._build_job(_sample_job())
    assert job.job_id == "REF123"
    assert job.title == "Software Engineer"
    assert job.company == "Visa"
    assert job.location == "Bangalore, INDIA, India"
    assert job.job_num == "REF123"
    assert "Build APIs" in job.description
    assert "Python" in job.description
    assert job.url.endswith("744000000000001-role")


def test_build_job_falls_back_to_posting_id():
    scraper = VisaScraper(client=DummyClient([]))
    job = scraper._build_job(_sample_job(refNumber="", postingId="744000000000999"))
    assert job.job_id == "744000000000999"
    assert job.job_num == ""


def test_scrape_paginates_until_records_matched():
    payload_page_1 = {
        "recordsMatched": 3,
        "pageSize": 2,
        "jobDetails": [_sample_job(refNumber="REF1"), _sample_job(refNumber="REF2")],
    }
    payload_page_2 = {
        "recordsMatched": 3,
        "pageSize": 2,
        "jobDetails": [_sample_job(refNumber="REF3")],
    }
    client = DummyClient([DummyResponse(payload_page_1), DummyResponse(payload_page_2)])
    scraper = VisaScraper(client=client, page_size=2)

    jobs = scraper.scrape("https://corporate.visa.com/en/jobs/?cities=Bangalore")

    assert [job.job_id for job in jobs] == ["REF1", "REF2", "REF3"]
    assert client.calls[0][1] == {"city": ["Bangalore"], "from": 0, "size": 2}
    assert client.calls[1][1] == {"city": ["Bangalore"], "from": 2, "size": 2}


def test_scrape_stops_on_empty_page():
    client = DummyClient([DummyResponse({"recordsMatched": 5, "jobDetails": []})])
    scraper = VisaScraper(client=client)

    jobs = scraper.scrape("https://corporate.visa.com/en/jobs/?cities=Bangalore")

    assert jobs == []


def test_scrape_filters_by_age():
    payload = {
        "recordsMatched": 2,
        "jobDetails": [
            _sample_job(refNumber="RECENT", createdOn="2026-04-17T10:00:00.000Z"),
            _sample_job(refNumber="OLD", createdOn="2026-04-10T10:00:00.000Z"),
        ],
    }
    client = DummyClient([DummyResponse(payload)])
    scraper = VisaScraper(client=client, max_age_days=3)

    from scrapers import visa as visa_module

    original_datetime = visa_module.datetime

    class FixedDateTime:
        @staticmethod
        def now(tz=None):
            return datetime(2026, 4, 18, 10, 0, tzinfo=timezone.utc)

        @staticmethod
        def fromisoformat(value):
            return original_datetime.fromisoformat(value)

    visa_module.datetime = FixedDateTime
    try:
        jobs = scraper.scrape("https://corporate.visa.com/en/jobs/?cities=Bangalore")
    finally:
        visa_module.datetime = original_datetime

    assert [job.job_id for job in jobs] == ["RECENT"]


def test_scrape_keeps_unparseable_dates():
    payload = {
        "recordsMatched": 1,
        "jobDetails": [_sample_job(refNumber="BADDATE", createdOn="not-a-date")],
    }
    client = DummyClient([DummyResponse(payload)])
    scraper = VisaScraper(client=client, max_age_days=1)

    jobs = scraper.scrape("https://corporate.visa.com/en/jobs/?cities=Bangalore")

    assert [job.job_id for job in jobs] == ["BADDATE"]


def test_fetch_page_returns_empty_on_http_error():
    client = DummyClient([DummyResponse(status_code=500)])
    scraper = VisaScraper(client=client)

    jobs, total, page_size = scraper._fetch_page(0)

    assert jobs == []
    assert total == 0
    assert page_size == scraper._page_size


def test_fetch_page_returns_empty_on_invalid_json():
    client = DummyClient([DummyResponse(json_error=True)])
    scraper = VisaScraper(client=client)

    jobs, total, page_size = scraper._fetch_page(0)

    assert jobs == []
    assert total == 0
    assert page_size == scraper._page_size
