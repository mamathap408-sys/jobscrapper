"""
notifier.py — Email Digest via Gmail API (OAuth2)
===================================================
Sends HTML email digests with matched job postings using the Gmail API.

How it works:
  1. Authenticates with Google via OAuth2 (first time opens browser, then uses cached token)
  2. Builds an HTML email with a table of matched jobs (title, score, reason, link)
  3. Sends it via Gmail API (not SMTP — works through corporate firewalls)

Setup required:
  1. Create a Google Cloud project and enable Gmail API
  2. Create OAuth2 Desktop credentials → download as credentials.json
  3. Place credentials.json in the project root
  4. First run opens browser → authorize with your Gmail account
  5. token.json is saved automatically — no browser needed again

Files:
  - credentials.json: OAuth2 client credentials (from Google Cloud Console)
  - token.json: Cached access + refresh tokens (auto-created after first login)

Why _RequestsHttp adapter?
  Google's API client library normally uses httplib2 for HTTP calls, but httplib2
  doesn't support disabling SSL verification properly on Python 3.13. Since we're
  behind a corporate Zscaler proxy that intercepts HTTPS, we need verify=False.
  So we wrap a requests.Session (which supports verify=False) to look like httplib2.
"""

import base64
import logging
import os
import urllib3
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timezone
from pathlib import Path

import requests as req_lib
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from scrapers.base import JobPosting

logger = logging.getLogger(__name__)

# Suppress "InsecureRequestWarning" logs from urllib3 (we know SSL is disabled)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Only request permission to send emails (not read, not manage)
_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]

# File paths relative to project root
_PROJECT_DIR = Path(__file__).parent
_CREDENTIALS_PATH = _PROJECT_DIR / "credentials.json"  # From Google Cloud Console
_TOKEN_PATH = _PROJECT_DIR / "token.json"               # Auto-created after OAuth login


def _build_html(matches: list[tuple[JobPosting, int, str]]) -> str:
    """Build an HTML email body with a table of matched jobs.

    Each job gets a row with:
      - Job title (clickable link to the posting)
      - Company name and location
      - Score with color coding: green (8+), yellow (6-7), red (<6)
      - Reason from the GenAI matcher

    Args:
        matches: List of (JobPosting, score, reason) tuples.

    Returns:
        Complete HTML string ready to be used as email body.
    """
    rows = []
    for job, score, reason in matches:
        # Color-code the score: green=great, yellow=good, red=weak
        color = '#34a853' if score >= 8 else '#fbbc04' if score >= 6 else '#ea4335'
        rows.append(f"""
        <tr>
            <td style="padding:12px; border-bottom:1px solid #eee;">
                <strong><a href="{job.url}" style="color:#1a73e8; text-decoration:none;">
                    {job.title}
                </a></strong><br>
                <span style="color:#555;">{job.company}</span>
                {f' &mdash; {job.location}' if job.location else ''}
                {f'<br><span style="color:#888; font-size:0.9em;">Posted: {job.posted_date}</span>' if job.posted_date else ''}
            </td>
            <td style="padding:12px; border-bottom:1px solid #eee; text-align:center;">
                <span style="font-size:1.4em; font-weight:bold; color:{color};">
                    {score}/10
                </span>
            </td>
            <td style="padding:12px; border-bottom:1px solid #eee; color:#555; font-size:0.9em;">
                {reason}
            </td>
        </tr>
        """)

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return f"""
    <html>
    <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width:800px; margin:auto;">
        <h2 style="color:#333;">Job Match Digest</h2>
        <p style="color:#666;">{len(matches)} matching job(s) found &mdash; {timestamp}</p>
        <table style="width:100%; border-collapse:collapse;">
            <thead>
                <tr style="background:#f8f9fa;">
                    <th style="padding:10px; text-align:left;">Position</th>
                    <th style="padding:10px; text-align:center; width:80px;">Score</th>
                    <th style="padding:10px; text-align:left;">Why it matches</th>
                </tr>
            </thead>
            <tbody>
                {''.join(rows)}
            </tbody>
        </table>
        <p style="color:#999; font-size:0.8em; margin-top:20px;">
            Sent by Job Posting Watcher
        </p>
    </body>
    </html>
    """


class _RequestsHttp:
    """Makes a requests.Session look like httplib2.Http for Google's API client.

    Google's googleapiclient.discovery.build() expects an httplib2-like HTTP object.
    This adapter wraps requests.Session so we can use verify=False for SSL.

    It also handles automatic token refresh: if the OAuth token is expired,
    it refreshes it before making the API call.
    """

    def __init__(self, session, credentials):
        self._session = session      # requests.Session with verify=False
        self._creds = credentials    # Google OAuth2 credentials

    def request(self, uri, method="GET", body=None, headers=None, **kwargs):
        """Make an HTTP request, auto-refreshing the OAuth token if needed."""
        if self._creds.expired:
            self._creds.refresh(Request(session=self._session))

        auth_headers = {}
        self._creds.apply(auth_headers)
        if headers:
            headers.update(auth_headers)
        else:
            headers = auth_headers

        resp = self._session.request(method, uri, data=body, headers=headers)
        return _HttplibResponse(resp), resp.content


class _HttplibResponse:
    """Wraps a requests.Response to look like an httplib2.Response."""

    def __init__(self, resp):
        self.status = resp.status_code
        self._headers = {k.lower(): v for k, v in resp.headers.items()}

    def __getitem__(self, key):
        return self._headers.get(key.lower())

    def __contains__(self, key):
        return key.lower() in self._headers


class EmailNotifier:
    """Sends job match digest emails via Gmail API."""

    def __init__(self, config: dict):
        """Initialize with email settings from config.yaml.

        Args:
            config: The full config dictionary. Uses config["email"] section:
                    - sender_email: Gmail address authorized via OAuth (sends the email)
                    - recipient_email: Where the digest is delivered
        """
        email_cfg = config["email"]
        self._sender = email_cfg["sender_email"]
        self._recipient = email_cfg["recipient_email"]
        self._service = None  # Lazily initialized on first send

    def _ensure_service(self):
        """Initialize the Gmail API service if not already done.

        Lazy initialization means we only authenticate when actually sending,
        not at startup. This avoids unnecessary browser popups during testing.
        """
        if self._service is None:
            self._service = self._build_gmail_service()

    def _build_gmail_service(self):
        """Authenticate with Gmail and return an API service client.

        Authentication flow:
          1. Check if token.json exists → load cached credentials
          2. If token is expired → silently refresh using the refresh_token
          3. If no token at all → open browser for OAuth consent (first time only)
          4. Save the new/refreshed token to token.json for next time

        Returns:
            A Gmail API service client (googleapiclient.discovery.Resource).
        """
        # Allow OAuth over HTTP localhost (needed for the local redirect during consent)
        os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
        creds = None

        if _TOKEN_PATH.exists():
            creds = Credentials.from_authorized_user_file(str(_TOKEN_PATH), _SCOPES)

        session = req_lib.Session()
        session.verify = False

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request(session=session))
            else:
                if not _CREDENTIALS_PATH.exists():
                    raise FileNotFoundError(
                        f"OAuth2 credentials not found at {_CREDENTIALS_PATH}. "
                        "Download credentials.json from Google Cloud Console."
                    )
                flow = InstalledAppFlow.from_client_secrets_file(
                    str(_CREDENTIALS_PATH), _SCOPES
                )
                original_request = flow.oauth2session.request
                def _patched_request(method, url, **kwargs):
                    kwargs["verify"] = False
                    return original_request(method, url, **kwargs)
                flow.oauth2session.request = _patched_request
                creds = flow.run_local_server(port=0)

            with open(_TOKEN_PATH, "w") as f:
                f.write(creds.to_json())
            logger.info("Gmail OAuth2 token saved to %s", _TOKEN_PATH)

        http = _RequestsHttp(session, creds)
        return build("gmail", "v1", http=http)

    def send_digest(self, matches: list[tuple[JobPosting, int, str]]):
        """Send an HTML digest email with all matched jobs from this cycle.

        Args:
            matches: List of (JobPosting, score, reason) tuples to include.
                     Only jobs that scored >= match_threshold should be here.
        """
        self._ensure_service()

        # Build email with both HTML and plain text versions
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"Job Match Digest — {len(matches)} new match(es)"
        msg["From"] = self._sender
        msg["To"] = self._recipient

        # Plain text version (for email clients that don't render HTML)
        plain_lines = ["Job Match Digest\n"]
        for job, score, reason in matches:
            plain_lines.append(
                f"- {job.title} at {job.company} ({job.location}) "
                f"— Score: {score}/10\n  {reason}\n  {job.url}\n"
            )
        msg.attach(MIMEText("\n".join(plain_lines), "plain"))

        # HTML version (nice formatted table with color-coded scores)
        msg.attach(MIMEText(_build_html(matches), "html"))

        # Encode the email as base64 and send via Gmail API
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        try:
            result = self._service.users().messages().send(
                userId="me",
                body={"raw": raw}
            ).execute()
            logger.info("Digest email sent to %s (%d matches) — message id: %s",
                        self._recipient, len(matches), result.get("id"))
        except Exception as e:
            logger.error("Failed to send email via Gmail API: %s", e)
            raise
