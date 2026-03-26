"""
resume_builder.py — Tailored Resume Generator
===============================================
Generates job-specific resumes by tweaking the base LaTeX resume to better
align with a given job description. Uses GenAI to modify Summary, Projects,
and Technical Skills sections while keeping Experience, Education, and
Achievements untouched.

Usage (standalone):
    python generate_resume.py --job-id <id>       # from DB
    python generate_resume.py --job-file <path>   # from text file
    python generate_resume.py --auto              # all above threshold

Usage (programmatic):
    builder = ResumeBuilder(config, db)
    pdf_path = builder.generate_for_job_id("some-job-id")
"""

import logging
import os
import shutil
import subprocess
from pathlib import Path

from services.db import JobDatabase
from services.genai_client import GenAIClient

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent.parent / "generated_resumes"

_TAILOR_PROMPT = """\
You will receive a base LaTeX resume and a job description. Make MINIMAL, surgical edits to help the resume pass ATS keyword filters for this job. The resume should still read like a human wrote it.

## What to do — LESS IS MORE

1. **Summary**: Rewrite to align with the JD, but keep the original tone and structure. Mention key skills/technologies from the JD that the candidate actually has. Keep to 2-3 sentences.
2. **Technical Skills**: Reorder so JD-relevant skills appear first. If the candidate clearly has a skill from the JD (proven by their projects/experience) but it's missing, add it. Remove nothing.
3. **Projects**: Rephrase bullet points to incorporate JD keywords and terminology naturally. Keep the same meaning — do NOT fabricate accomplishments or add new content. Reorder bullets so JD-relevant ones come first.
4. **Experience**: Rephrase bullets to echo JD language where truthful. Keep the same facts — do NOT change job titles, dates, companies, or add/remove bullets.
5. **Education & Additional**: Copy EXACTLY as-is. Zero changes.
6. **Section order**: If the target role clearly matches the candidate's previous experience, move **Experience** ahead of **Technical Skills** and **Projects** so the order is: Experience, Technical Skills, Projects. Otherwise keep the original section order.

## Critical constraints
- Keep changes minimal and natural. If the base resume already matches well, return it nearly unchanged.
- Do NOT add flowery language, buzzwords, or filler. Keep the candidate's original voice.
- Do NOT fabricate skills or experience.
- Do NOT add new bullet points anywhere.
- **Skills limit**: The final total skill count must NOT exceed the original count by more than 2. You CAN remove irrelevant skills and replace them with more suitable ones from the JD — replacements don't count toward the limit since the total stays the same.

## Format
- Return ONLY the complete LaTeX source code. No commentary, no markdown fences.
- Must compile with pdflatex without errors.
- Preserve all LaTeX commands, packages, and formatting exactly.

## Base Resume:
{base_resume}

## Job Description:
{job_description}
"""


class ResumeBuilder:
    """Generates tailored resumes using GenAI and compiles them to PDF."""

    def __init__(self, config: dict, db: JobDatabase):
        genai_cfg = config["genai"]
        self._client = GenAIClient(genai_cfg)
        self._db = db

        rb_cfg = config.get("resume_builder", {})
        self._threshold = rb_cfg.get("threshold", 7)
        self._base_resume_path = Path(rb_cfg.get("base_resume", "data/resume_v2.tex"))

        if not self._base_resume_path.is_absolute():
            self._base_resume_path = Path(__file__).parent.parent / self._base_resume_path

        self._base_resume = self._base_resume_path.read_text(encoding="utf-8")
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        # Add TeX bin directory to PATH if configured
        tex_bin = rb_cfg.get("tex_bin")
        if tex_bin and Path(tex_bin).is_dir() and tex_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = tex_bin + ":" + os.environ.get("PATH", "")

    @property
    def threshold(self) -> float:
        return self._threshold

    def generate_for_job_id(self, job_id: str) -> Path | None:
        """Generate a tailored resume for a job stored in the database.

        Returns:
            Path to the generated PDF, or None on failure.
        """
        job = self._db.get_job_by_id(job_id)
        if not job:
            logger.error("Job ID '%s' not found in database", job_id)
            return None

        jd = job.get("job_description") or ""
        if not jd.strip():
            logger.error("Job ID '%s' has no description stored — cannot generate resume", job_id)
            return None

        return self._generate(
            job_description=jd,
            job_title=job["title"],
            company=job["company"],
            job_id=job_id,
        )

    def generate_from_file(self, file_path: str) -> Path | None:
        """Generate a tailored resume from a job description text file.

        Returns:
            Path to the generated PDF, or None on failure.
        """
        fp = Path(file_path)
        if not fp.exists():
            logger.error("File not found: %s", file_path)
            return None

        jd = fp.read_text(encoding="utf-8")
        if not jd.strip():
            logger.error("File is empty: %s", file_path)
            return None

        return self._generate(
            job_description=jd,
            job_title="ad-hoc",
            company="unknown",
        )

    def generate_all_above_threshold(self) -> list[Path]:
        """Generate resumes for all jobs above threshold that don't have one yet.

        Returns:
            List of paths to generated PDFs.
        """
        jobs = self._db.get_jobs_needing_resume(self._threshold)
        if not jobs:
            logger.info("No jobs above threshold %.1f need resumes", self._threshold)
            return []

        logger.info("Generating resumes for %d jobs above threshold %.1f", len(jobs), self._threshold)
        results = []
        for job in jobs:
            jd = job.get("job_description") or ""
            if not jd.strip():
                logger.warning("Skipping job '%s' (ID: %s) — no description", job["title"], job["job_id"])
                continue

            pdf = self._generate(
                job_description=jd,
                job_title=job["title"],
                company=job["company"],
                job_id=job["job_id"],
            )
            if pdf:
                results.append(pdf)
        return results

    def generate_for_matches(self, matches: list[tuple]) -> dict[str, Path]:
        """Generate resumes for a list of matched jobs (used by email flow).

        Args:
            matches: List of (job, score, reason) tuples from the matcher.

        Returns:
            Dict mapping job_id → PDF path for successfully generated resumes.
        """
        results = {}
        for job, score, reason in matches:
            if score < self._threshold:
                continue
            if not job.description or not job.description.strip():
                logger.warning("Skipping resume for '%s' — no description", job.title)
                continue

            pdf = self._generate(
                job_description=job.description,
                job_title=job.title,
                company=job.company,
                job_id=job.job_id,
            )
            if pdf:
                results[job.job_id] = pdf
        return results

    def _generate(self, job_description: str, job_title: str, company: str,
                  job_id: str | None = None) -> Path | None:
        """Core generation: LLM tailoring → .tex file → pdflatex → .pdf.

        Returns:
            Path to the generated PDF, or None on failure.
        """
        logger.info("Generating resume for: %s at %s", job_title, company)

        prompt = _TAILOR_PROMPT.format(
            base_resume=self._base_resume,
            job_description=job_description,
        )

        try:
            tailored_tex = self._client.chat(prompt)
        except Exception as e:
            logger.error("LLM call failed for '%s': %s", job_title, e)
            return None

        # Strip markdown fences if LLM adds them despite instructions
        tailored_tex = tailored_tex.strip()
        if tailored_tex.startswith("```"):
            first_newline = tailored_tex.find("\n")
            tailored_tex = tailored_tex[first_newline + 1:]
        if tailored_tex.endswith("```"):
            tailored_tex = tailored_tex[:-3].rstrip()

        # Validate it looks like LaTeX
        if "\\documentclass" not in tailored_tex:
            logger.error("LLM output doesn't look like valid LaTeX for '%s'", job_title)
            return None

        # Generate unique filename
        seq = self._next_sequence()
        resume_name = f"resume_{seq}"
        self._db.set_last_resume_name(resume_name)
        logger.debug("Reserved next resume sequence as '%s'", resume_name)
        tex_path = OUTPUT_DIR / f"{resume_name}.tex"

        tex_path.write_text(tailored_tex, encoding="utf-8")
        logger.info("Wrote tailored .tex: %s", tex_path)

        # Compile to PDF
        pdf_path = self._compile_tex_to_pdf(tex_path)
        if not pdf_path:
            logger.error("PDF compilation failed for %s", tex_path)
            return None

        # Update DB with resume name
        if job_id:
            self._db.set_resume_name(job_id, resume_name)
            logger.info("Stored resume_name '%s' for job_id '%s'", resume_name, job_id)

        logger.info("Generated resume: %s", pdf_path)
        return pdf_path

    def _next_sequence(self) -> str:
        """Get next alphabetical sequence (_a, _b, ... _z, _aa, _ab, ...) from DB."""
        last = self._db.get_last_resume_name()

        if not last:
            return "a"

        # Extract suffix after "resume_"
        suffix = last.replace("resume_", "")

        # Increment: a→b, z→aa, az→ba, zz→aaa
        chars = list(suffix)
        i = len(chars) - 1
        while i >= 0:
            if chars[i] < "z":
                chars[i] = chr(ord(chars[i]) + 1)
                return "".join(chars)
            chars[i] = "a"
            i -= 1
        # All were 'z', add one more character
        return "a" * (len(chars) + 1)

    def _compile_tex_to_pdf(self, tex_path: Path) -> Path | None:
        """Compile a .tex file to PDF using pdflatex.

        Returns:
            Path to the PDF file, or None if compilation failed.
        """
        pdflatex = shutil.which("pdflatex")
        if not pdflatex:
            logger.error(
                "pdflatex not found. Install a LaTeX distribution:\n"
                "  brew install --cask basictex\n"
                "  eval \"$(/usr/libexec/path_helper)\""
            )
            return None

        try:
            result = subprocess.run(
                [pdflatex, "-interaction=nonstopmode", "-output-directory", str(OUTPUT_DIR), str(tex_path)],
                capture_output=True,
                text=True,
                timeout=60,
            )
            pdf_path = tex_path.with_suffix(".pdf")
            if pdf_path.exists():
                # Clean up auxiliary files
                for ext in [".aux", ".log", ".out"]:
                    aux = tex_path.with_suffix(ext)
                    if aux.exists():
                        aux.unlink()
                return pdf_path
            else:
                logger.error("pdflatex did not produce a PDF.\nstdout: %s\nstderr: %s",
                             result.stdout[-500:] if result.stdout else "", result.stderr[-500:] if result.stderr else "")
                return None
        except subprocess.TimeoutExpired:
            logger.error("pdflatex timed out for %s", tex_path)
            return None
        except Exception as e:
            logger.error("pdflatex failed: %s", e)
            return None

    def close(self):
        """Close the GenAI client."""
        self._client.close()
