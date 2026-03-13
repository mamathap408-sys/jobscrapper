"""
generate_resume.py — CLI for Tailored Resume Generation
========================================================
Standalone script to generate job-tailored resumes.

Usage:
    python generate_resume.py --job-id <id>       # Generate for a specific job from DB
    python generate_resume.py --job-file <path>   # Generate from a job description text file
    python generate_resume.py --auto              # Generate for all jobs above threshold
"""

import argparse
import logging
import sys

from config import load_config
from services.db import JobDatabase
from services.resume_builder import ResumeBuilder

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("resume_gen")


def main():
    parser = argparse.ArgumentParser(description="Generate tailored resumes for job postings")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--job-id", help="Job ID from the database")
    group.add_argument("--job-file", help="Path to a job description text file")
    group.add_argument("--auto", action="store_true", help="Generate for all jobs above threshold without a resume")
    args = parser.parse_args()

    config = load_config()
    db = JobDatabase()
    builder = ResumeBuilder(config, db)

    try:
        if args.job_id:
            pdf = builder.generate_for_job_id(args.job_id)
            if pdf:
                print(f"Resume generated: {pdf}")
            else:
                print("Failed to generate resume.", file=sys.stderr)
                sys.exit(1)

        elif args.job_file:
            pdf = builder.generate_from_file(args.job_file)
            if pdf:
                print(f"Resume generated: {pdf}")
            else:
                print("Failed to generate resume.", file=sys.stderr)
                sys.exit(1)

        elif args.auto:
            pdfs = builder.generate_all_above_threshold()
            if pdfs:
                print(f"Generated {len(pdfs)} resume(s):")
                for p in pdfs:
                    print(f"  {p}")
            else:
                print("No resumes to generate.")
    finally:
        builder.close()
        db.close()


if __name__ == "__main__":
    main()
