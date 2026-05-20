"""
apply.py — CLI for Auto-Apply
===============================
Standalone script to automatically apply to job postings.
Detects the correct applicant (Workday, etc.) from the job URL.

Usage:
    python apply.py                         # Apply to all qualifying jobs (default)
    python apply.py --auto                  # Same as above
    python apply.py --job-id <id>           # Apply to one specific job by DB job_id
    python apply.py --company genpact       # Apply only to jobs at a specific company
"""

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from config import load_config, load_answers
from config.loader import ANSWERS_PATH
from services.db import JobDatabase
from services.genai_client import GenAIClient
from services.resume_builder import ResumeBuilder
from applicants import detect_applicant_type, get_applicant
from applicants.resume_parser import parse_resume_tex

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("auto_apply")

RESUME_DIR = Path(__file__).parent / "generated_resumes"
APPLIED_RESUME_DIR = Path(__file__).parent / "applied_resumes"


# ── LLM-based invalid job filter ───────────────────────────────

def _filter_invalid_jobs(jobs: list[dict], config: dict, db: JobDatabase) -> list[dict]:
    """Filter out jobs that are not relevant (wrong location, etc.). Marks them as invalid in DB."""
    client = GenAIClient(config["genai"])
    invalid_ids = set()

    for batch_start in range(0, len(jobs), 10):
        batch = jobs[batch_start:batch_start + 10]
        job_list = "\n".join(
            f"{i+1}. \"{j['title']}\" at \"{j['company']}\" — Location: \"{j.get('location', 'unknown')}\""
            for i, j in enumerate(batch)
        )
        prompt = (
            f"I am a candidate based in Bangalore, India. I can only work in India.\n"
            f"Check which of these jobs are NOT based in India. Mark them as invalid.\n\n"
            f"JOBS:\n{job_list}\n\n"
            f"Return a JSON array: [{{\"index\": 1, \"invalid\": true/false, \"reason\": \"...\"}}]\n"
            f"Only set invalid=true if the job is clearly NOT in India (e.g., US, UK, Singapore, etc.).\n"
            f"If location is ambiguous or says 'Multiple Locations', assume valid."
        )
        try:
            response = client.chat(prompt)
            text = response.strip()
            if text.startswith("```"):
                text = text[text.find("\n") + 1:]
            if text.endswith("```"):
                text = text[:-3].rstrip()
            results = json.loads(text)
            for item in results:
                if item.get("invalid"):
                    idx = item["index"] - 1
                    if 0 <= idx < len(batch):
                        job = batch[idx]
                        reason = item.get("reason", "not in India")
                        logger.info("Invalid job: '%s' at %s — %s", job["title"], job["company"], reason)
                        db.create_application(job["job_id"])
                        db.mark_invalid(job["job_id"], reason)
                        invalid_ids.add(job["job_id"])
        except Exception as e:
            logger.warning("Invalid job filter LLM batch failed: %s — skipping filter for this batch", e)

    if invalid_ids:
        jobs = [j for j in jobs if j["job_id"] not in invalid_ids]
        logger.info("Invalid job filter: removed %d job(s)", len(invalid_ids))
    return jobs


# ── LLM-based role filter ──────────────────────────────────────

def _filter_skip_roles(jobs: list[dict], skip_roles: list[dict], config: dict, db: JobDatabase) -> list[dict]:
    """Filter out jobs matching skip_roles using LLM. Marks skipped jobs as failed in DB."""
    # Build rules text for the prompt
    rules = []
    for sr in skip_roles:
        rule = f"- Role: \"{sr['role']}\" at company: \"{sr['company']}\" — Reason: \"{sr.get('reason', 'not relevant')}\""
        if sr.get("instructions"):
            rule += f"\n  Instructions: {sr['instructions']}"
        rules.append(rule)
    rules_text = "\n".join(rules)

    # Only check jobs that could potentially match (quick pre-filter by company)
    skip_companies = {sr["company"].lower() for sr in skip_roles}
    candidates = [j for j in jobs if j["company"].lower() in skip_companies]
    if not candidates:
        return jobs

    client = GenAIClient(config["genai"])
    skipped_ids = set()

    # Process in batches of 10
    for batch_start in range(0, len(candidates), 10):
        batch = candidates[batch_start:batch_start + 10]
        job_list = "\n".join(
            f"{i+1}. \"{j['title']}\" at \"{j['company']}\"" for i, j in enumerate(batch)
        )
        prompt = (
            f"You are filtering job applications. Based on the rules below, determine which jobs should be SKIPPED.\n\n"
            f"SKIP RULES:\n{rules_text}\n\n"
            f"JOBS TO CHECK:\n{job_list}\n\n"
            f"For each job, respond with a JSON array of objects: "
            f"[{{\"index\": 1, \"skip\": true/false, \"reason\": \"...\"}}]\n"
            f"Only set skip=true if the job clearly matches a skip rule. When in doubt, do NOT skip."
        )
        try:
            response = client.chat(prompt)
            # Parse JSON from response
            text = response.strip()
            if text.startswith("```"):
                text = text[text.find("\n") + 1:]
            if text.endswith("```"):
                text = text[:-3].rstrip()
            results = json.loads(text)
            for item in results:
                if item.get("skip"):
                    idx = item["index"] - 1
                    if 0 <= idx < len(batch):
                        job = batch[idx]
                        reason = item.get("reason", "role blacklisted")
                        logger.info("Role filter: skipping '%s' at %s — %s", job["title"], job["company"], reason)
                        db.create_application(job["job_id"])
                        db.mark_apply_failed(job["job_id"], reason)
                        skipped_ids.add(job["job_id"])
        except Exception as e:
            logger.warning("Role filter LLM batch failed: %s — skipping filter for this batch", e)

    if skipped_ids:
        jobs = [j for j in jobs if j["job_id"] not in skipped_ids]
        logger.info("Role filter: skipped %d job(s)", len(skipped_ids))
    return jobs


# ── Resume preparation ─────────────────────────────────────────

def prepare_resume(resume_name: str, apply_email: str, resume_email: str = "",
                   tex_bin: str = "") -> Path | None:
    """Get the resume PDF ready for application, recompiling only if emails differ.

    If emails match (or resume_email is empty), returns the original PDF as-is.
    If they differ, copies .tex to applied_resumes/, swaps email, recompiles.
    Original generated_resumes/ files are never touched.
    """
    # No mismatch — use original PDF directly
    if not resume_email or resume_email == apply_email:
        original_pdf = RESUME_DIR / f"{resume_name}.pdf"
        if original_pdf.exists():
            return original_pdf
        logger.error("Original PDF not found: %s", original_pdf)
        return None

    # Emails differ — need to recompile with the apply email
    src_tex = RESUME_DIR / f"{resume_name}.tex"
    if not src_tex.exists():
        logger.error("Source .tex not found: %s", src_tex)
        return None

    APPLIED_RESUME_DIR.mkdir(exist_ok=True)
    dst_tex = APPLIED_RESUME_DIR / f"{resume_name}.tex"
    dst_pdf = APPLIED_RESUME_DIR / f"{resume_name}.pdf"

    # Copy and swap email
    tex_content = src_tex.read_text(encoding="utf-8")
    tex_content = tex_content.replace(resume_email, apply_email)
    dst_tex.write_text(tex_content, encoding="utf-8")

    # Compile with pdflatex
    env = os.environ.copy()
    if tex_bin:
        env["PATH"] = tex_bin + os.pathsep + env.get("PATH", "")

    pdflatex = shutil.which("pdflatex", path=env.get("PATH"))
    if not pdflatex:
        logger.error("pdflatex not found — cannot compile resume")
        return None

    try:
        subprocess.run(
            [pdflatex, "-interaction=nonstopmode",
             "-output-directory", str(APPLIED_RESUME_DIR), str(dst_tex)],
            capture_output=True, text=True, timeout=60, env=env,
        )
        if dst_pdf.exists():
            for ext in [".aux", ".log", ".out"]:
                aux = APPLIED_RESUME_DIR / dst_tex.with_suffix(ext).name
                if aux.exists():
                    aux.unlink()
            logger.info("Compiled applied resume: %s", dst_pdf.name)
            return dst_pdf
        else:
            logger.error("pdflatex did not produce PDF for %s", resume_name)
            return None
    except subprocess.TimeoutExpired:
        logger.error("pdflatex timed out for %s", resume_name)
        return None
    except Exception as e:
        logger.error("pdflatex failed for %s: %s", resume_name, e)
        return None


# ── Job loading & filtering ────────────────────────────────────

def _load_jobs(db: JobDatabase, args, threshold: float) -> list[dict]:
    """Load candidate jobs from DB based on CLI args."""
    if args.job_id:
        job = db.get_job_by_id(args.job_id)
        if not job:
            print(f"Job not found: {args.job_id}", file=sys.stderr)
            sys.exit(1)
        if not job.get("resume_name"):
            print("No resume generated for this job. Run generate_resume.py first.", file=sys.stderr)
            sys.exit(1)
        return [job]
    else:
        jobs = db.get_jobs_to_apply(threshold)
        if not jobs:
            print("No jobs to apply to (all qualifying jobs already applied or no resumes generated).")
        return jobs


def _filter_new_jobs(jobs: list[dict], config: dict, db: JobDatabase, answers: dict, apply_cfg: dict) -> list[dict]:
    """Run full filter pipeline on new (unvalidated) jobs. Marks survivors as pending."""
    blacklist = [c.lower() for c in apply_cfg.get("blacklist_companies", [])]
    skip_roles = apply_cfg.get("skip_roles", [])

    # Blacklist companies
    if blacklist:
        before = len(jobs)
        jobs = [j for j in jobs if j["company"].lower() not in blacklist]
        skipped = before - len(jobs)
        if skipped:
            logger.info("Skipped %d job(s) from blacklisted companies: %s", skipped, ", ".join(blacklist))

    if not jobs:
        return []

    # Remove unsupported platforms
    supported = []
    for job in jobs:
        if detect_applicant_type(job["url"]):
            supported.append(job)
        else:
            logger.warning("Unsupported platform: %s at %s — %s", job["title"], job["company"], job["url"][:60])
    jobs = supported

    if not jobs:
        return []

    # Batch expiry check (HTTP, no browser)
    answers_raw = ANSWERS_PATH.read_text()
    by_type = defaultdict(list)
    for job in jobs:
        by_type[detect_applicant_type(job["url"])].append(job)

    active = []
    for atype, type_jobs in by_type.items():
        applicant = get_applicant(atype, config=config, answers=answers, answers_raw=answers_raw)
        for job in type_jobs:
            is_valid, status_code = applicant.is_job_valid(job["url"])
            if not is_valid:
                logger.info("Job expired: %s at %s", job["title"], job["company"])
                db.create_application(job["job_id"])
                db.mark_expired(job["job_id"])
            elif status_code != 200 and status_code != 0:
                logger.info("Job check inconclusive (HTTP %d), skipping for now: %s at %s",
                            status_code, job["title"], job["company"])
            else:
                active.append(job)
    jobs = active

    if not jobs:
        return []

    # LLM invalid job filter (non-India)
    jobs = _filter_invalid_jobs(jobs, config, db)

    # LLM role filter
    if jobs and skip_roles:
        jobs = _filter_skip_roles(jobs, skip_roles, config, db)

    # Mark survivors as pending (validated)
    for job in jobs:
        db.create_application(job["job_id"])

    return jobs


# ── Apply loop ─────────────────────────────────────────────────

def _apply_jobs(jobs_by_type: dict, config: dict, db: JobDatabase, answers: dict, apply_cfg: dict):
    """Start browsers and apply to all jobs. Re-checks expiry before each application."""
    apply_email = answers.get("workday_account", {}).get("email", "")
    resume_email = apply_cfg.get("resume_email", "")
    tex_bin = config.get("resume_builder", {}).get("tex_bin", "")
    regenerate_resumes = apply_cfg.get("regenerate_resumes", False)
    resume_freshness_days = apply_cfg.get("resume_freshness_days", 5)
    delay = apply_cfg.get("delay_between_jobs_seconds", 10)

    total = sum(len(jl) for jl in jobs_by_type.values())
    submitted = 0
    failed = 0
    job_num = 0

    answers_raw = ANSWERS_PATH.read_text()
    for atype, type_jobs in jobs_by_type.items():
        logger.info("Starting %s applicant for %d job(s)", atype, len(type_jobs))
        applicant = get_applicant(atype, config=config, answers=answers, answers_raw=answers_raw)
        applicant.start()

        try:
            for i, job in enumerate(type_jobs):
                job_num += 1
                job_id = job["job_id"]

                try:
                    # Re-check expiry (time may have passed since validation)
                    is_valid, status_code = applicant.is_job_valid(job["url"])
                    if not is_valid:
                        logger.info("Skipping expired job [%d/%d]: %s at %s",
                                    job_num, total, job["title"], job["company"])
                        db.mark_expired(job_id)
                        failed += 1
                        continue
                    if status_code != 200 and status_code != 0:
                        raise RuntimeError(
                            f"Job check inconclusive (HTTP {status_code}) — will retry later"
                        )

                    # Regenerate resume if configured (skip if recently generated)
                    if regenerate_resumes:
                        generated_at = job.get("resume_generated_at")
                        skip_regen = False
                        if generated_at:
                            gen_dt = datetime.fromisoformat(generated_at).replace(tzinfo=timezone.utc)
                            if datetime.now(timezone.utc) - gen_dt < timedelta(days=resume_freshness_days):
                                skip_regen = True
                                logger.info("Resume recently generated (%s) — skipping regeneration for %s",
                                            generated_at, job["title"])
                        if not skip_regen:
                            builder = ResumeBuilder(config, db)
                            try:
                                builder.regenerate_for_job_id(job_id)
                            except Exception as e:
                                logger.warning("Resume regeneration failed for %s: %s", job["title"], e)
                            finally:
                                builder.close()

                    # Prepare resume (recompile if emails differ)
                    pdf_path = prepare_resume(
                        job["resume_name"], apply_email, resume_email, tex_bin
                    )
                    if not pdf_path:
                        raise RuntimeError(f"Failed to prepare resume: {job['resume_name']}")

                    resume_data = parse_resume_tex(pdf_path)
                    applicant.apply(job, pdf_path, resume_data)
                    db.mark_applied(job_id, answer_reasoning=applicant._answer_reasoning)
                    submitted += 1
                    logger.info("Applied [%d/%d]: %s at %s",
                                job_num, total, job["title"], job["company"])
                except Exception as e:
                    db.mark_apply_failed(job_id, str(e))
                    failed += 1
                    logger.error("Failed [%d/%d] %s at %s: %s",
                                 job_num, total, job["title"], job["company"], e)


                time.sleep(delay)
        finally:
            applicant.close()

    print(f"\nDone: {submitted} submitted, {failed} failed out of {total} total.")


# ── Main ────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Auto-apply to job postings")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--job-id", help="Apply to a specific job by DB job_id")
    group.add_argument("--auto", action="store_true", default=True,
                       help="Apply to all qualifying jobs (default when no args given)")
    parser.add_argument("--company", help="Filter jobs to a specific company (case-insensitive substring match)")
    parser.add_argument("--portal", help="Filter jobs by applicant portal type (e.g. workday, successfactors)")
    args = parser.parse_args()
    if args.job_id:
        args.auto = False

    config = load_config()
    answers = load_answers()
    db = JobDatabase()
    apply_cfg = config.get("apply", {})
    threshold = apply_cfg.get("apply_threshold", 7)

    try:
        # Load candidate jobs
        jobs = _load_jobs(db, args, threshold)
        if not jobs:
            return

        # Company filter
        if args.company:
            needle = args.company.lower()
            jobs = [j for j in jobs if needle in j["company"].lower()]
            if not jobs:
                print(f"No jobs found for company matching '{args.company}'.")
                return
            logger.info("Company filter '%s': %d job(s)", args.company, len(jobs))

        # Split: already-pending (skip filters) vs new (need full pipeline)
        already_pending = []
        already_failed = []
        new_jobs = []
        for job in jobs:
            app = db.get_application_status(job["job_id"])
            if app and app["status"] == "pending":
                already_pending.append(job)
            elif app and app["status"] == "failed":
                already_failed.append(job)
            else:
                new_jobs.append(job)

        if already_pending or already_failed:
            logger.info("%d pending + %d failed job(s) already validated — skipping filters",
                        len(already_pending), len(already_failed))

        # Run filters on new jobs only
        validated = []
        if new_jobs:
            validated = _filter_new_jobs(new_jobs, config, db, answers, apply_cfg)

        # Combine: pending first, then newly validated, then failed (retries last)
        all_jobs = already_pending + validated + already_failed
        if not all_jobs:
            print("No jobs to apply to after all filters.")
            return

        # Group by applicant type
        jobs_by_type = defaultdict(list)
        for job in all_jobs:
            atype = detect_applicant_type(job["url"])
            if atype:
                jobs_by_type[atype].append(job)

        # Portal filter
        if args.portal:
            needle = args.portal.lower()
            jobs_by_type = {k: v for k, v in jobs_by_type.items() if needle in k.lower()}
            if not jobs_by_type:
                print(f"No jobs found for portal matching '{args.portal}'.")
                return
            logger.info("Portal filter '%s': %d type(s)", args.portal, len(jobs_by_type))

        # Print summary
        total = sum(len(jl) for jl in jobs_by_type.values())
        print(f"\nFound {total} job(s) to apply to:\n")
        for atype, type_jobs in jobs_by_type.items():
            print(f"  [{atype}] {len(type_jobs)} job(s):")
            for j in type_jobs:
                print(f"    [{j['match_score']:.0f}] {j['title']} at {j['company']} (resume: {j['resume_name']})")
        print()

        # Apply
        _apply_jobs(jobs_by_type, config, db, answers, apply_cfg)

    finally:
        db.close()


if __name__ == "__main__":
    main()
