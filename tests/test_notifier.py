"""Tests for email digest builder."""

from scrapers.base import JobPosting
from notifier import _build_html


def _sample_match():
    job = JobPosting(
        job_id="test-1",
        title="Senior Python Engineer",
        company="TechCorp",
        location="San Francisco, CA",
        description="Backend role focusing on distributed systems.",
        url="https://example.com/jobs/1",
        posted_date="2024-01-10",
    )
    return (job, 8, "Strong match on Python, AWS, and backend focus.")


def test_build_html_single_match():
    matches = [_sample_match()]
    html = _build_html(matches)
    assert "Senior Python Engineer" in html
    assert "TechCorp" in html
    assert "8/10" in html
    assert "Strong match" in html
    assert "https://example.com/jobs/1" in html


def test_build_html_multiple_matches():
    m1 = _sample_match()
    job2 = JobPosting(
        job_id="test-2",
        title="Platform Engineer",
        company="StartupXYZ",
        location="Remote",
        description="Infrastructure and platform engineering.",
        url="https://example.com/jobs/2",
        posted_date="",
    )
    m2 = (job2, 7, "Good fit for infrastructure interest.")
    matches = [m1, m2]
    html = _build_html(matches)
    assert "2 matching job(s)" in html
    assert "Platform Engineer" in html
    assert "StartupXYZ" in html


def test_build_html_color_coding():
    """Verify score colors: green >= 8, yellow >= 6, red < 6."""
    job = _sample_match()[0]
    high = _build_html([(job, 9, "Great")])
    assert "#34a853" in high  # green

    mid = _build_html([(job, 6, "OK")])
    assert "#fbbc04" in mid  # yellow

    low = _build_html([(job, 4, "Poor")])
    assert "#ea4335" in low  # red


if __name__ == "__main__":
    test_build_html_single_match()
    test_build_html_multiple_matches()
    test_build_html_color_coding()
    print("All notifier tests passed!")
