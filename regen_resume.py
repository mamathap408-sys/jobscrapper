"""
regen_resume.py — Regenerate a Resume by Name
===============================================
Given a resume name (e.g., resume_za), looks up the associated job in the DB,
regenerates the tailored .tex and .pdf, opens the PDF for review, and on
confirmation replaces the originals in generated_resumes/.

Usage:
    python regen_resume.py resume_za
"""

import logging
import platform
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from config import load_config
from services.db import JobDatabase
from services.genai_client import GenAIClient
from services.resume_builder import OUTPUT_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("regen_resume")

def load_tailor_prompt():
    from services.resume_builder import _TAILOR_PROMPT, _EXPERIENCE_BOOST_PROMPT
    return _TAILOR_PROMPT + _EXPERIENCE_BOOST_PROMPT


def compile_tex_to_pdf(tex_path: Path, output_dir: Path) -> Path | None:
    """Compile a .tex file to PDF using pdflatex."""
    pdflatex = shutil.which("pdflatex")
    if not pdflatex:
        logger.error("pdflatex not found on PATH")
        return None

    result = subprocess.run(
        [pdflatex, "-interaction=nonstopmode", "-output-directory", str(output_dir), str(tex_path)],
        capture_output=True,
        text=True,
        timeout=60,
    )
    pdf_path = output_dir / tex_path.with_suffix(".pdf").name
    if pdf_path.exists():
        # Clean up auxiliary files
        for ext in [".aux", ".log", ".out"]:
            aux = output_dir / tex_path.with_suffix(ext).name
            if aux.exists():
                aux.unlink()
        return pdf_path
    else:
        logger.error("pdflatex failed:\n%s", result.stderr or result.stdout)
        return None


def open_pdf(pdf_path: Path):
    """Open PDF with the system default viewer."""
    if platform.system() == "Darwin":
        subprocess.Popen(["open", str(pdf_path)])
    elif platform.system() == "Linux":
        subprocess.Popen(["xdg-open", str(pdf_path)])
    else:
        subprocess.Popen(["start", str(pdf_path)], shell=True)


def main():
    resume_name = input("Enter resume name (e.g., resume_za or just za): ").strip()
    if not resume_name:
        print("Error: No resume name provided.", file=sys.stderr)
        sys.exit(1)
    if not resume_name.startswith("resume_"):
        resume_name = f"resume_{resume_name}"

    # Verify the original files exist
    orig_tex = OUTPUT_DIR / f"{resume_name}.tex"
    orig_pdf = OUTPUT_DIR / f"{resume_name}.pdf"
    if not orig_tex.exists():
        print(f"Error: {orig_tex} not found.", file=sys.stderr)
        sys.exit(1)

    # Find the job associated with this resume
    config = load_config()
    db = JobDatabase()

    row = db._conn.execute(
        "SELECT job_id, title, company, job_description FROM seen_jobs WHERE resume_name = ?",
        (resume_name,),
    ).fetchone()

    if not row:
        print(f"Error: No job found in DB with resume_name='{resume_name}'.", file=sys.stderr)
        db.close()
        sys.exit(1)

    job_id, title, company, job_description = row
    if not job_description or not job_description.strip():
        print(f"Error: Job '{title}' at '{company}' has no description stored.", file=sys.stderr)
        db.close()
        sys.exit(1)

    print(f"Regenerating resume for: {title} at {company}")
    print(f"  Job ID: {job_id}")
    print(f"  Original: {orig_tex}")

    # Load base resume and tailor prompt
    rb_cfg = config.get("resume_builder", {})
    base_resume_path = Path(rb_cfg.get("base_resume", "data/resume_v2.tex"))
    if not base_resume_path.is_absolute():
        base_resume_path = Path(__file__).parent / base_resume_path
    base_resume = base_resume_path.read_text(encoding="utf-8")

    tailor_prompt = load_tailor_prompt()
    prompt = tailor_prompt.format(base_resume=base_resume, job_description=job_description)

    # Call LLM
    genai_cfg = config["genai"]
    client = GenAIClient(genai_cfg)
    print("Calling LLM to regenerate...")
    try:
        tailored_tex = client.chat(prompt)
    except Exception as e:
        print(f"Error: LLM call failed: {e}", file=sys.stderr)
        db.close()
        sys.exit(1)

    # Strip markdown fences
    tailored_tex = tailored_tex.strip()
    if tailored_tex.startswith("```"):
        first_newline = tailored_tex.find("\n")
        tailored_tex = tailored_tex[first_newline + 1:]
    if tailored_tex.endswith("```"):
        tailored_tex = tailored_tex[:-3].rstrip()

    if "\\documentclass" not in tailored_tex:
        print("Error: LLM output doesn't look like valid LaTeX.", file=sys.stderr)
        db.close()
        sys.exit(1)

    # Write to a temp directory and compile
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        tmp_tex = tmp_path / f"{resume_name}.tex"
        tmp_tex.write_text(tailored_tex, encoding="utf-8")

        pdf_path = compile_tex_to_pdf(tmp_tex, tmp_path)
        if not pdf_path:
            print("Error: PDF compilation failed.", file=sys.stderr)
            db.close()
            sys.exit(1)

        print(f"\nRegenerated PDF: {pdf_path}")
        open_pdf(pdf_path)

        # Prompt user to validate
        try:
            input("\nReview the PDF. Press Enter to replace the original, or Ctrl+C to cancel: ")
        except KeyboardInterrupt:
            print("\nCancelled. Original files unchanged.")
            db.close()
            sys.exit(0)

        # Replace originals
        shutil.copy2(tmp_tex, orig_tex)
        shutil.copy2(pdf_path, orig_pdf)

    print(f"Replaced:\n  {orig_tex}\n  {orig_pdf}")
    db.close()


if __name__ == "__main__":
    main()
