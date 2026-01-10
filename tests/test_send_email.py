"""Integration test — sends a real test email via Gmail API."""

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from scrapers.base import JobPosting
from services.notifier import EmailNotifier


def test_send_real_email():
    """Send a test digest email with sample job matches."""
    config = {
        "email": {
            "sender_email": "xxxx",
            "recipient_email": "xxxx",
        }
    }

    matches = [
        (
            JobPosting(
                job_id="test-001",
                title="Senior Java Developer",
                company="Google",
                location="Bangalore, India",
                description="Backend role with Spring Boot and distributed systems.",
                url="https://careers.google.com/jobs/test-001",
                posted_date="2026-04-01",
            ),
            9,
            "Excellent match — Java, Spring Boot, and backend focus align perfectly with profile.",
        ),
        (
            JobPosting(
                job_id="test-002",
                title="Full Stack Engineer",
                company="Amazon",
                location="Remote, India",
                description="React + Java microservices role.",
                url="https://amazon.jobs/test-002",
                posted_date="2026-04-02",
            ),
            7,
            "Good fit — React and Java skills match, full-stack role aligns with experience.",
        ),
        (
            JobPosting(
                job_id="test-003",
                title="Frontend Developer",
                company="Startup XYZ",
                location="Hyderabad, India",
                description="Pure React/TypeScript frontend role.",
                url="https://startupxyz.com/careers/test-003",
                posted_date="",
            ),
            4,
            "Weak match — frontend-only role, candidate prefers backend/full-stack.",
        ),
    ]

    notifier = EmailNotifier(config)
    notifier.send_digest(matches)
    print("Test email sent! Check xxxx inbox.")


if __name__ == "__main__":
    test_send_real_email()
