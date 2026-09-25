"""Tests for shared scraper helpers in scrapers/base.py."""

from datetime import date, timedelta

import pytest

from scrapers.base import BaseScraper, date_or_none, to_date


def test_to_date_iso():
    assert to_date("2026-04-30T10:28:48.282") == date(2026, 4, 30)   # Infosys
    assert to_date("2026-05-01T10:00:00Z") == date(2026, 5, 1)       # Greenhouse / Ashby
    assert to_date("2026-05-01") == date(2026, 5, 1)


def test_to_date_epoch():
    assert to_date(1714521600) == date(2024, 5, 1)         # Google (seconds)
    assert to_date(1714521600000) == date(2024, 5, 1)      # Lever (milliseconds)
    assert to_date("1714521600000") == date(2024, 5, 1)


def test_to_date_rfc2822():
    assert to_date("Tue, 14 Apr 2026 00:00:00 GMT") == date(2026, 4, 14)    # Cognizant / SuccessFactors
    assert to_date("Tue, 14 Apr 2026 00:00:00 +0530") == date(2026, 4, 14)


def test_to_date_custom_formats():
    assert to_date("April 10, 2026", "%B %d, %Y") == date(2026, 4, 10)      # Amazon
    assert to_date("4/20/26", "%m/%d/%y") == date(2026, 4, 20)              # HCL


@pytest.mark.parametrize("value", [None, "", "invalid", "Posted 2 Days Ago"])
def test_to_date_raises(value):
    with pytest.raises(ValueError, match="Unparseable date"):
        to_date(value)


def test_date_or_none_warns(caplog):
    assert date_or_none("2026-05-01") == date(2026, 5, 1)
    assert date_or_none("garbage") is None
    assert "Unparseable date: 'garbage'" in caplog.text


def test_within_age():
    scraper = BaseScraper(max_age_days=10)
    try:
        assert scraper._within_age(date.today() - timedelta(days=3))
        assert not scraper._within_age(date.today() - timedelta(days=11))
        assert scraper._within_age(None)   # jobs without a date are kept
    finally:
        scraper.close()


def test_within_age_no_limit():
    scraper = BaseScraper(max_age_days=None)
    try:
        assert scraper._within_age(date(2000, 1, 1))
    finally:
        scraper.close()
