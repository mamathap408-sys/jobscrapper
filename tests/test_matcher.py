"""Tests for GenAI job matcher."""

import base64
import json
from unittest.mock import MagicMock, patch

from scrapers.base import JobPosting
from services.matcher import _build_profile_text, _build_jobs_text


def _sample_job(title="Senior Backend Engineer", company="Acme Corp"):
    return JobPosting(
        job_id="test-123",
        title=title,
        company=company,
        location="Remote",
        description="We are looking for a senior backend engineer with Python and AWS experience.",
        url="https://example.com/jobs/123",
        posted_date="2024-01-15",
    )


def _sample_profile():
    return {
        "title": "Software Engineer",
        "skills": ["Python", "AWS", "Distributed Systems"],
        "tools": ["Git", "Claude Code"],
        "experience_years": 5,
        "location_preference": "Remote",
        "additional_criteria": "Looking for senior IC backend roles.",
    }


def _sample_config():
    return {
        "genai": {
            "login_url": "https://fake.com/auth/login",
            "validate_url": "https://fake.com/auth/validate",
            "chat_url": "https://fake.com/queries/chat",
            "username": "testuser",
            "password": base64.b64encode(b"testpass").decode(),
            "model": "claude-sonnet-4-20250514",
            "application_name": "test-app",
            "match_threshold": 6,
        },
        "profile": _sample_profile(),
    }


def test_build_profile_text():
    text = _build_profile_text(_sample_profile())
    assert "Software Engineer" in text
    assert "Python" in text
    assert "5 years" in text
    assert "Remote" in text
    assert "Claude Code" in text


def test_build_jobs_text():
    jobs = [_sample_job(), _sample_job("Frontend Dev", "Other Inc")]
    text = _build_jobs_text(jobs)
    assert "Senior Backend Engineer" in text
    assert "Frontend Dev" in text
    assert "Job 1" in text
    assert "Job 2" in text


def test_matcher_with_mock_gateway():
    """Test matcher end-to-end with a mocked GenAI Gateway."""
    # Mock the gateway chat response (Claude format)
    chat_response = {
        "full_model_response": {
            "content": [
                {"text": json.dumps([
                    {"job_id": "test-123", "score": 8, "reason": "Strong Python + AWS match"}
                ])}
            ]
        }
    }

    # Mock httpx.Client
    with patch("services.matcher.httpx.Client") as MockClient:
        mock_client = MagicMock()
        MockClient.return_value = mock_client

        # Mock login response
        mock_login_resp = MagicMock()
        mock_login_resp.status_code = 200
        mock_login_resp.json.return_value = {"access_token": "fake-jwt-token"}

        # Mock validate response (token valid)
        mock_validate_resp = MagicMock()
        mock_validate_resp.status_code = 200

        # Mock chat response
        mock_chat_resp = MagicMock()
        mock_chat_resp.status_code = 200
        mock_chat_resp.json.return_value = chat_response
        mock_chat_resp.raise_for_status = MagicMock()

        def side_effect_post(url, **kwargs):
            if "login" in url:
                return mock_login_resp
            return mock_chat_resp

        mock_client.post.side_effect = side_effect_post
        mock_client.get.return_value = mock_validate_resp

        from services.matcher import JobMatcher
        m = JobMatcher(_sample_config())
        results = m.match_jobs([_sample_job()])

        assert len(results) == 1
        job, score, reason = results[0]
        assert score == 8
        assert "Python" in reason


if __name__ == "__main__":
    test_build_profile_text()
    test_build_jobs_text()
    test_matcher_with_mock_gateway()
    print("All matcher tests passed!")
