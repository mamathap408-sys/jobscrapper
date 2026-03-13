"""
backfill_descriptions.py — Fetch and populate job_description for existing DB rows
==================================================================================
One-time script to backfill the job_description column for jobs that were
saved before the column existed.

Usage:
    python scripts/backfill_descriptions.py              # all 8+ score jobs
    python scripts/backfill_descriptions.py --min-score 7  # custom threshold
"""

import argparse
import logging
import re
import sqlite3
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("backfill")

DB_PATH = Path(__file__).parent.parent / "data" / "jobs.db"

# Shared httpx client (SSL disabled for corporate proxy)
_client = httpx.Client(timeout=30, verify=False)


def _strip_html(html: str) -> str:
    """Remove HTML tags and collapse whitespace."""
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"&[a-zA-Z]+;", " ", text)
    text = re.sub(r"&#\d+;", " ", text)
    return re.sub(r"\s+", " ", text).strip()


# ── Workday ──────────────────────────────────────────────────────────────────

def _workday_api_base(url: str) -> str:
    """Convert portal URL → API base. e.g. https://wf.wd1.../WellsFargoJobs → API base."""
    parsed = urlparse(url)
    company = parsed.hostname.split(".")[0]
    path_parts = [p for p in parsed.path.strip("/").split("/") if p]
    # Filter out locale segments like "en-US"
    path_parts = [p for p in path_parts if not re.match(r"^[a-z]{2}-[A-Z]{2}$", p)]
    # Remove "job" and the job-specific path — keep only the site name
    if "job" in path_parts:
        site = path_parts[path_parts.index("job") - 1] if path_parts.index("job") > 0 else path_parts[0]
    else:
        site = path_parts[0] if path_parts else "External"
    return f"{parsed.scheme}://{parsed.hostname}/wday/cxs/{company}/{site}"


def fetch_workday(url: str, job_id: str) -> str:
    """Fetch description from Workday JSON API."""
    api_base = _workday_api_base(url)
    resp = _client.get(f"{api_base}{job_id}")
    resp.raise_for_status()
    posting_info = resp.json().get("jobPostingInfo", {})
    raw_desc = posting_info.get("jobDescription", "")
    return _strip_html(raw_desc)


# ── Amazon ───────────────────────────────────────────────────────────────────

def fetch_amazon(url: str, job_id: str) -> str:
    """Fetch description from Amazon Jobs page."""
    # Amazon has a JSON endpoint for individual jobs
    api_url = f"https://www.amazon.jobs/en/jobs/{job_id}"
    resp = _client.get(api_url, headers={"Accept": "application/json"})
    if resp.status_code == 200:
        try:
            data = resp.json()
            desc_parts = [
                data.get("description", ""),
                data.get("basic_qualifications", ""),
                data.get("preferred_qualifications", ""),
            ]
            return _strip_html(" ".join(part for part in desc_parts if part))
        except Exception:
            pass

    # Fallback: fetch HTML page and extract description
    resp = _client.get(api_url)
    resp.raise_for_status()
    match = re.search(r'<div[^>]*class="[^"]*description[^"]*"[^>]*>(.*?)</div>', resp.text, re.DOTALL | re.IGNORECASE)
    if match:
        return _strip_html(match.group(1))
    return _strip_html(resp.text[:5000])


# ── Google ───────────────────────────────────────────────────────────────────

def fetch_google(url: str, job_id: str) -> str:
    """Fetch description from Google Careers page."""
    resp = _client.get(url, follow_redirects=True)
    resp.raise_for_status()
    html = resp.text
    # Google career pages embed job data in various ways
    # Try to extract from meta description or main content
    match = re.search(r'<meta\s+name="description"\s+content="([^"]*)"', html, re.IGNORECASE)
    if match and len(match.group(1)) > 100:
        return match.group(1).strip()
    # Try to find the job description div
    match = re.search(r'Minimum qualifications.*?(?=Preferred qualifications|About the job|$)', html, re.DOTALL | re.IGNORECASE)
    if match:
        return _strip_html(match.group(0))
    return _strip_html(html[:8000])


# ── HCL ─────────────────────────────────────────────────────────────────────

def fetch_hcl(url: str, job_id: str) -> str:
    """Fetch description from HCL Careers page."""
    resp = _client.get(url, follow_redirects=True)
    resp.raise_for_status()
    html = resp.text
    match = re.search(
        r'Job\s+Summary(.*?)(?:Skill\s+Requirements|Other\s+Requirements|Apply\s+now)',
        html, re.DOTALL | re.IGNORECASE,
    )
    if match:
        return _strip_html(match.group(1))
    match = re.search(r'<meta\s+name="description"\s+content="([^"]*)"', html, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return ""


# ── Cognizant ────────────────────────────────────────────────────────────────

def fetch_cognizant(url: str, job_id: str) -> str:
    """Fetch description from Cognizant Careers page."""
    resp = _client.get(url, follow_redirects=True)
    resp.raise_for_status()
    html = resp.text
    # Look for job description section
    match = re.search(r'<div[^>]*class="[^"]*job-description[^"]*"[^>]*>(.*?)</div>', html, re.DOTALL | re.IGNORECASE)
    if match:
        return _strip_html(match.group(1))
    match = re.search(r'<meta\s+name="description"\s+content="([^"]*)"', html, re.IGNORECASE)
    if match and len(match.group(1)) > 50:
        return match.group(1).strip()
    return _strip_html(html[:8000])


# ── Dispatcher ───────────────────────────────────────────────────────────────

def _detect_source(url: str) -> str:
    """Detect the job source from its URL."""
    if "myworkdayjobs.com" in url:
        return "workday"
    if "amazon.jobs" in url:
        return "amazon"
    if "google.com" in url:
        return "google"
    if "hcltech.com" in url:
        return "hcl"
    if "cognizant.com" in url:
        return "cognizant"
    return "unknown"


FETCHERS = {
    "workday": fetch_workday,
    "amazon": fetch_amazon,
    "google": fetch_google,
    "hcl": fetch_hcl,
    "cognizant": fetch_cognizant,
}


def main():
    parser = argparse.ArgumentParser(description="Backfill job descriptions for existing DB rows")
    parser.add_argument("--min-score", type=float, default=8, help="Minimum match score (default: 8)")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        """SELECT job_id, url, title, company, match_score
           FROM seen_jobs
           WHERE match_score >= ?
             AND (job_description IS NULL OR job_description = '')
           ORDER BY match_score DESC""",
        (args.min_score,),
    ).fetchall()

    logger.info("Found %d jobs with score >= %.1f needing descriptions", len(rows), args.min_score)

    success = 0
    failed = 0

    for row in rows:
        job_id = row["job_id"]
        url = row["url"]
        title = row["title"]
        source = _detect_source(url)

        fetcher = FETCHERS.get(source)
        if not fetcher:
            logger.warning("  SKIP: Unknown source for %s (%s)", title, url)
            failed += 1
            continue

        try:
            desc = fetcher(url, job_id)
            if desc and len(desc) > 50:
                conn.execute(
                    "UPDATE seen_jobs SET job_description = ? WHERE job_id = ?",
                    (desc, job_id),
                )
                conn.commit()
                success += 1
                logger.info("  OK [%s] %s — %d chars", source, title, len(desc))
            else:
                logger.warning("  EMPTY [%s] %s — got %d chars", source, title, len(desc) if desc else 0)
                failed += 1
        except Exception as e:
            logger.error("  FAIL [%s] %s — %s", source, title, e)
            failed += 1

        time.sleep(0.5)  # Be respectful

    conn.close()
    _client.close()
    logger.info("Done: %d success, %d failed out of %d total", success, failed, len(rows))


if __name__ == "__main__":
    main()
