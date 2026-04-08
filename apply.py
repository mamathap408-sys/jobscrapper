"""
apply.py — CLI for Auto-Apply
===============================
Standalone script to automatically apply to job postings.
Detects the correct applicant (Workday, etc.) from the job URL.

Usage:
    python apply.py                     # Apply to all qualifying jobs (default)
    python apply.py --auto              # Same as above
    python apply.py --job-id <id>       # Apply to one specific job by DB job_id
"""

import argparse
import logging
import os
import shutil
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path

from config import load_config, load_answers
from config.loader import ANSWERS_PATH
from services.db import JobDatabase
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

    # If already compiled, reuse it
    if dst_pdf.exists():
        logger.info("Using cached applied resume: %s", dst_pdf.name)
        return dst_pdf

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


# ── Main ────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Auto-apply to job postings")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--job-id", help="Apply to a specific job by DB job_id")
    group.add_argument("--auto", action="store_true", default=True,
                       help="Apply to all qualifying jobs (default when no args given)")
    args = parser.parse_args()
    if args.job_id:
        args.auto = False

    config = load_config()
    answers = load_answers()
    db = JobDatabase()
    apply_cfg = config.get("apply", {})
    threshold = apply_cfg.get("apply_threshold", 7)
    delay = apply_cfg.get("delay_between_jobs_seconds", 10)
    blacklist = [c.lower() for c in apply_cfg.get("blacklist_companies", [])]
    apply_email = answers.get("personal", {}).get("email", "")
    resume_email = apply_cfg.get("resume_email", "")
    tex_bin = config.get("resume_builder", {}).get("tex_bin", "")

    try:
        # Determine which jobs to apply to
        if args.job_id:
            job = db.get_job_by_id(args.job_id)
            if not job:
                print(f"Job not found: {args.job_id}", file=sys.stderr)
                sys.exit(1)
            if not job.get("resume_name"):
                print("No resume generated for this job. Run generate_resume.py first.", file=sys.stderr)
                sys.exit(1)
            jobs = [job]

        elif args.auto:
            jobs = db.get_jobs_to_apply(threshold)
            if not jobs:
                print("No jobs to apply to (all qualifying jobs already applied or no resumes generated).")
                return

        # Filter out blacklisted companies
        if blacklist:
            before = len(jobs)
            jobs = [j for j in jobs if j["company"].lower() not in blacklist]
            skipped = before - len(jobs)
            if skipped:
                logger.info("Skipped %d job(s) from blacklisted companies: %s", skipped, ", ".join(blacklist))
            if not jobs:
                print("No jobs remaining after blacklist filter.")
                return

        # Group jobs by applicant type
        jobs_by_type = defaultdict(list)
        unsupported = []
        for job in jobs:
            atype = detect_applicant_type(job["url"])
            if atype:
                jobs_by_type[atype].append(job)
            else:
                unsupported.append(job)

        if unsupported:
            logger.warning("Skipping %d job(s) with unsupported platforms:", len(unsupported))
            for j in unsupported:
                logger.warning("  %s at %s — %s", j["title"], j["company"], j["url"][:60])

        if not jobs_by_type:
            print("No jobs with supported applicant types.")
            return

        # Print summary
        total = sum(len(jl) for jl in jobs_by_type.values())
        print(f"\nFound {total} job(s) to apply to:\n")
        for atype, type_jobs in jobs_by_type.items():
            print(f"  [{atype}] {len(type_jobs)} job(s):")
            for j in type_jobs:
                print(f"    [{j['match_score']:.0f}] {j['title']} at {j['company']} (resume: {j['resume_name']})")
        print()

        submitted = 0
        failed = 0
        job_num = 0

        # Process each applicant type
        for atype, type_jobs in jobs_by_type.items():
            logger.info("Starting %s applicant for %d job(s)", atype, len(type_jobs))
            answers_raw = ANSWERS_PATH.read_text()
            applicant = get_applicant(atype, config=config, answers=answers, answers_raw=answers_raw)
            applicant.start()

            try:
                for i, job in enumerate(type_jobs):
                    job_num += 1
                    job_id = job["job_id"]

                    db.create_application(job_id)

                    try:
                        # Check if job is still active before applying
                        if not applicant.is_job_valid(job["url"]):
                            logger.info("Skipping expired job [%d/%d]: %s at %s",
                                        job_num, total, job["title"], job["company"])
                            db.mark_expired(job_id)
                            failed += 1
                            continue

                        # Prepare resume (recompile if emails differ)
                        pdf_path = prepare_resume(
                            job["resume_name"], apply_email, resume_email, tex_bin
                        )
                        if not pdf_path:
                            raise RuntimeError(f"Failed to prepare resume: {job['resume_name']}")

                        resume_data = parse_resume_tex(pdf_path)
                        applicant.apply(job, pdf_path, resume_data)
                        db.mark_applied(job_id)
                        submitted += 1
                        logger.info("Applied [%d/%d]: %s at %s",
                                    job_num, total, job["title"], job["company"])
                    except Exception as e:
                        db.mark_apply_failed(job_id, str(e))
                        failed += 1
                        logger.error("Failed [%d/%d] %s at %s: %s",
                                     job_num, total, job["title"], job["company"], e)

                    if i < len(type_jobs) - 1:
                        time.sleep(delay)
            finally:
                applicant.close()

        # Summary
        print(f"\nDone: {submitted} submitted, {failed} failed out of {total} total.")

    finally:
        db.close()


if __name__ == "__main__":
    main()
