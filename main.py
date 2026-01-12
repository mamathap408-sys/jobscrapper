"""
main.py — Job Posting Watcher Entry Point
===========================================
This is the main script that ties everything together and runs the watcher loop.

What it does:
  1. Loads configuration (config.yaml + urls.txt)
  2. Initializes the database (SQLite), matcher (GenAI), notifier (Gmail), and browser (Playwright)
  3. Runs the first check cycle immediately
  4. Then repeats every N minutes (configured in config.yaml)
  5. On Ctrl+C or kill signal, shuts down gracefully

Each cycle:
  - For each portal URL in urls.txt:
      a. Pick the right scraper (Workday API or generic Playwright)
      b. Scrape all job postings from that portal
      c. Check which jobs are NEW (not seen in SQLite database)
      d. For new Workday jobs, fetch full descriptions
      e. Send new jobs to GenAI matcher for scoring
      f. Save all jobs to database with their scores
      g. Collect jobs that score >= threshold
  - If any matches found: send one digest email with all matches
  - Log a summary (X portals checked, Y matches found)

Usage:
  python main.py

  Make sure config.yaml is configured and env vars are set before running.
  First run may open a browser for Gmail OAuth consent.
"""

import logging
import signal
import time

import schedule

from config import load_config, load_urls
from services.db import JobDatabase
from services.matcher import JobMatcher
from services.notifier import EmailNotifier
from scrapers import get_scraper

# Configure logging to show timestamps and module names
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("jobwatcher")

# Global flags for graceful shutdown
_shutdown = False   # Set to True when Ctrl+C or SIGTERM is received
_browser = None     # Playwright browser instance (shared across cycles)


def _signal_handler(sig, frame):
    """Handle Ctrl+C (SIGINT) and kill (SIGTERM) signals.

    Instead of crashing immediately, sets the _shutdown flag so the main loop
    can finish the current work and clean up properly (close browser, DB, etc.).

    This is registered via signal.signal() in main() — it tells the OS:
    "When this process receives SIGINT or SIGTERM, call this function."
    """
    global _shutdown
    logger.info("Shutdown requested (signal %s)", sig)
    _shutdown = True


def _init_playwright():
    """Launch a headless Chromium browser for the generic scraper.

    Playwright is only needed for non-Workday sites (type "generic" in urls.txt).
    If Playwright is not installed or fails to launch, the generic scraper is
    simply disabled — Workday scraper still works fine.

    The browser is created once and reused across all cycles to avoid the
    overhead of launching a new browser every hour.

    Returns:
        The Playwright context manager (needed for cleanup), or None if unavailable.
    """
    global _browser
    try:
        from playwright.sync_api import sync_playwright
        pw = sync_playwright().start()
        _browser = pw.chromium.launch(headless=True)
        logger.info("Playwright browser launched")
        return pw
    except Exception as e:
        logger.warning("Playwright not available — generic scraper disabled: %s", e)
        return None


def run_cycle(config: dict, db: JobDatabase, matcher: JobMatcher, notifier: EmailNotifier):
    """Run one full check cycle: scrape all portals, match new jobs, send digest.

    This function is called once immediately at startup, then every N minutes
    by the schedule library.

    Args:
        config:   The loaded config.yaml dictionary.
        db:       The SQLite database for deduplication.
        matcher:  The GenAI matcher for scoring jobs.
        notifier: The email notifier for sending digests.
    """
    urls = load_urls()  # Re-read urls.txt each cycle (in case you add new portals)
    delay = config["schedule"].get("delay_between_sites_seconds", 5)
    all_matches = []    # Collect matches across all portals for one digest email

    for scraper_type, url in urls:
        if _shutdown:
            break  # Stop checking if shutdown was requested

        logger.info("Checking [%s] %s", scraper_type, url)
        try:
            # Create the appropriate scraper (generic needs the Playwright browser)
            if scraper_type == "generic":
                scraper = get_scraper(scraper_type, browser=_browser)
            elif scraper_type == "workday":
                wd_cfg = config.get("workday", {})
                scraper = get_scraper(
                    scraper_type,
                    facets=wd_cfg.get("facets", {}),
                    max_age_days=wd_cfg.get("max_age_days"),
                )
            else:
                scraper = get_scraper(scraper_type)

            # Step 1: Scrape all jobs from this portal
            jobs = scraper.scrape(url)

            # Step 2: Filter to new jobs (also updates last_seen for known ones)
            new_jobs = db.filter_new(jobs)
            logger.info("[%s] %s — %d total, %d new", scraper_type, url, len(jobs), len(new_jobs))

            if new_jobs:
                # Step 3: Fetch full descriptions if the scraper supports it
                if hasattr(scraper, "enrich_descriptions"):
                    scraper.enrich_descriptions(new_jobs)

                # Step 4: Score new jobs against user profile via GenAI
                scored = matcher.match_jobs(new_jobs)

                # Step 5: Save all scored jobs to database
                for job, score, reason in scored:
                    db.save_job(job, match_score=score, match_reason=reason)
                    # Collect high-scoring jobs for the email digest
                    if score >= matcher.threshold:
                        all_matches.append((job, score, reason))
                        logger.info(
                            "  MATCH: %s at %s (score=%d)", job.title, job.company, score
                        )

            # Clean up scraper resources (e.g., httpx client connections)
            if hasattr(scraper, "close"):
                scraper.close()

        except Exception as e:
            logger.error("Error processing %s: %s", url, e)

        # Be respectful: wait between portal checks to avoid getting blocked
        if not _shutdown:
            time.sleep(delay)

    # Step 6: Send one digest email with ALL matches from this cycle
    if all_matches:
        notifier.send_digest(all_matches)
        # Mark these jobs as notified so we don't email about them again
        for job, _, _ in all_matches:
            db.mark_notified(job.job_id)

    logger.info(
        "Cycle complete: %d portals checked, %d matches",
        len(urls), len(all_matches),
    )


def main():
    """Main entry point: initialize everything and start the scheduler loop."""
    global _shutdown

    # Register signal handlers so Ctrl+C and kill trigger graceful shutdown
    # These tell the OS: "call _signal_handler when this process gets these signals"
    signal.signal(signal.SIGINT, _signal_handler)   # Ctrl+C
    signal.signal(signal.SIGTERM, _signal_handler)   # kill command

    # Initialize all components
    logger.info("Loading configuration...")
    config = load_config()

    logger.info("Initializing database...")
    db = JobDatabase()

    logger.info("Initializing matcher...")
    matcher = JobMatcher(config)

    logger.info("Initializing notifier...")
    notifier = EmailNotifier(config)

    logger.info("Initializing Playwright...")
    pw = _init_playwright()

    # Read schedule interval from config (default: every 60 minutes)
    interval = config["schedule"].get("interval_minutes", 60)
    logger.info("Scheduling checks every %d minutes", interval)

    # Run the first check immediately (don't wait for the timer)
    run_cycle(config, db, matcher, notifier)

    # Schedule all subsequent checks using the 'schedule' library
    schedule.every(interval).minutes.do(run_cycle, config, db, matcher, notifier)

    # Main loop: check if any scheduled jobs need to run, every second
    logger.info("Watcher running. Press Ctrl+C to stop.")
    while not _shutdown:
        schedule.run_pending()  # Runs run_cycle() when the timer fires
        time.sleep(1)           # Check every second (low CPU usage)

    # Graceful shutdown: clean up all resources
    logger.info("Shutting down...")
    if _browser:
        _browser.close()     # Close headless Chromium
        logger.info("Playwright browser closed")
    if pw:
        pw.stop()            # Stop Playwright process
    matcher.close()          # Close httpx client
    db.close()               # Close SQLite connection
    logger.info("Goodbye.")


if __name__ == "__main__":
    main()
