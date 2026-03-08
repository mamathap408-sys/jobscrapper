"""
db.py — SQLite Database for Job Deduplication & Tracking
=========================================================
Keeps track of every job posting we've ever seen, so we don't process or
notify about the same job twice.

Database file: jobs.db (created automatically in the project directory)

Table: seen_jobs
  - job_id (PRIMARY KEY): Unique identifier from the scraper
  - title, company, location, url: Basic job info
  - first_seen: When we first discovered this job
  - last_seen: Last time the scraper found this job (updated each cycle)
  - match_score: GenAI relevance score (1-10), NOT NULL
  - match_reason: GenAI explanation of the score
  - notified: Whether we've already sent an email about this job (0 or 1)
  - matched: Whether this job scored >= threshold (0 or 1)

Lifecycle of a job:
  1. Scraper finds a new job → filter_new() says it's new → matcher scores it → save_job()
  2. Next cycle, scraper finds it again → is_seen() returns True → touch_seen() updates last_seen
  3. Email sent (both matches and filtered) → mark_notified() for all
  4. If job disappears from portal → last_seen stops updating
"""

import sqlite3
import logging
from datetime import datetime, timezone
from pathlib import Path

from scrapers.base import JobPosting

logger = logging.getLogger(__name__)

# Database file lives in the data/ directory
DB_PATH = Path(__file__).parent.parent / "data" / "jobs.db"


class JobDatabase:
    """SQLite-backed storage for seen job postings."""

    def __init__(self, db_path: Path = DB_PATH):
        """Open (or create) the database and ensure the schema exists.

        Args:
            db_path: Path to the SQLite database file. Defaults to jobs.db in project root.
        """
        self._conn = sqlite3.connect(str(db_path))
        self._conn.row_factory = sqlite3.Row  # Allows accessing columns by name
        self._init_schema()

    def _init_schema(self):
        """Create the seen_jobs table if it doesn't exist yet.

        Uses CREATE TABLE IF NOT EXISTS so it's safe to call every time.
        """
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS seen_jobs (
                job_id TEXT PRIMARY KEY,
                job_num TEXT,
                title TEXT NOT NULL,
                company TEXT NOT NULL,
                location TEXT,
                url TEXT NOT NULL,
                first_seen TIMESTAMP NOT NULL,
                last_seen TIMESTAMP NOT NULL,
                match_score REAL NOT NULL,
                match_reason TEXT,
                notified BOOLEAN DEFAULT 0,
                matched BOOLEAN DEFAULT 0
            )
        """)
        self._conn.commit()

    def is_seen(self, job_id: str) -> bool:
        """Check if we've already seen this job in a previous cycle.

        Args:
            job_id: The job's unique identifier (from the scraper).

        Returns:
            True if the job exists in the database, False if it's new.
        """
        row = self._conn.execute(
            "SELECT 1 FROM seen_jobs WHERE job_id = ?", (job_id,)
        ).fetchone()
        return row is not None

    def filter_new(self, jobs: list[JobPosting]) -> list[JobPosting]:
        """From a list of scraped jobs, return only the ones we haven't seen before.

        Also updates last_seen for already-known jobs in the same pass,
        so callers don't need a separate touch_seen loop.

        Args:
            jobs: All jobs found by the scraper in this cycle.

        Returns:
            Only the jobs that are NOT in the database (truly new postings).
        """
        new_jobs = []
        for job in jobs:
            if self.is_seen(job.job_id):
                self.touch_seen(job.job_id)
            else:
                new_jobs.append(job)
        return new_jobs

    def save_job(
        self,
        job: JobPosting,
        match_score: float,
        match_reason: str | None = None,
        matched: bool = False,
    ):
        """Save a job to the database (insert or update).

        Uses SQLite UPSERT: if the job already exists, updates last_seen and score.
        If it's new, inserts a fresh row.

        Args:
            job:          The JobPosting to save.
            match_score:  GenAI relevance score (1-10).
            match_reason: GenAI explanation, or None.
            matched:      Whether this job scored >= threshold.
        """
        now = datetime.now(timezone.utc).isoformat()
        self._conn.execute(
            """
            INSERT INTO seen_jobs (job_id, job_num, title, company, location, url,
                                   first_seen, last_seen, match_score, match_reason, matched)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(job_id) DO UPDATE SET
                last_seen = excluded.last_seen,
                match_score = excluded.match_score,
                match_reason = COALESCE(excluded.match_reason, match_reason),
                matched = excluded.matched
            """,
            (
                job.job_id, job.job_num, job.title, job.company, job.location, job.url,
                now, now, match_score, match_reason, int(matched),
            ),
        )
        self._conn.commit()

    def mark_notified(self, job_id: str):
        """Mark a job as 'email sent' so we don't notify about it again.

        Args:
            job_id: The job's unique identifier.
        """
        self._conn.execute(
            "UPDATE seen_jobs SET notified = 1 WHERE job_id = ?", (job_id,)
        )
        self._conn.commit()

    def touch_seen(self, job_id: str):
        """Update the last_seen timestamp for a job that's still on the portal.

        Called every cycle for jobs we've already seen. This lets us detect when
        a job disappears (last_seen stops being updated).

        Args:
            job_id: The job's unique identifier.
        """
        now = datetime.now(timezone.utc).isoformat()
        self._conn.execute(
            "UPDATE seen_jobs SET last_seen = ? WHERE job_id = ?", (now, job_id)
        )
        self._conn.commit()

    def get_unnotified_jobs(self) -> tuple[list[dict], list[dict]]:
        """Return jobs that were scored but never emailed, split into matches and filtered.

        This catches jobs from a previous run that were saved to the DB
        but whose email failed to send before the process exited.

        Returns:
            Tuple of (matches, filtered) where each is a list of dicts.
        """
        rows = self._conn.execute(
            """
            SELECT job_id, job_num, title, company, location, url,
                   match_score, match_reason, matched
            FROM seen_jobs
            WHERE notified = 0
            ORDER BY match_score DESC
            """,
        ).fetchall()
        matches = [dict(r) for r in rows if r["matched"]]
        filtered = [dict(r) for r in rows if not r["matched"]]
        return matches, filtered

    def close(self):
        """Close the database connection."""
        self._conn.close()
