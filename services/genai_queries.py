"""
genai_queries.py — GenAI Query Catalog
=======================================
One class for every LLM interaction in the system. Each public method is a
self-contained *query*: it owns its prompt, calls the GenAI Gateway, and parses
the response into a typed result.

Layering:
  - genai_client.py  → the transport (auth, retry, HTTP). "How to talk."
  - genai_queries.py → this file. The prompts + parsing. "What to ask."

GenAIQueries owns its own GenAIClient, so callers just construct it with the
genai config and call methods — no client threading.

Configurable text (e.g. the scoring rubric, extra experience bullets) is NOT
hardcoded here — callers load it from config.yaml / yaml files and pass it in.
Prompts that should never be user-tuned (output-format specs, LaTeX rules)
live here as module constants.
"""

import json
import logging
import re

from scrapers.base import JobPosting
from services.genai_client import GenAIClient

logger = logging.getLogger(__name__)

_MAX_JOB_DESCRIPTION_CHARS = 20000

_TAILOR_PROMPT = """\
You will receive a base LaTeX resume and a job description. Make MINIMAL, surgical edits to help the resume pass ATS keyword filters for this job. The resume should still read like a human wrote it.

## What to do — LESS IS MORE

1. **Summary**: Rewrite to align with the JD, but keep the original tone and structure. Mention key skills/technologies from the JD that the candidate actually has. Keep to 2-3 sentences.
2. **Technical Skills**: Reorder so JD-relevant skills appear first. If the candidate clearly has a skill from the JD (proven by their projects/experience) but it's missing, add it. Remove nothing.
3. **Experience**: Rephrase bullets to echo JD language where truthful. Keep the same facts — do NOT change job titles, dates, companies, or add/remove bullets.
4. **Projects**: Rephrase bullet points to incorporate JD keywords and terminology naturally. Keep the same meaning — do NOT fabricate accomplishments or add new content. Reorder bullets so JD-relevant ones come first.
5. **Education & Additional**: Copy EXACTLY as-is. Zero changes.

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

_EXPERIENCE_BOOST_PROMPT = """

## Experience Enhancement (apply ONLY to experience entries relevant to the target JD)

Below are extra candidate-verified bullet points, grouped by experience entry (role/company).
For each entry below whose role is relevant to this JD:

1. **Add 2-3 bullet points** to that experience entry. Pick the most JD-relevant items from its list:

{additional_bullets}

2. **Reorder experience entries**: Move the most JD-relevant boosted entry to the TOP of the Experience section.
3. **Reorder sections**: If any entry was boosted, move the Experience section BEFORE Technical Skills and Projects, so the order becomes: Summary → Experience → Technical Skills → Projects → Education/Achievements.

If none of the entries above are relevant to the JD, ignore this section entirely and keep the resume as-is (original section and experience order).
"""


class GenAIQueries:
    """Catalog of GenAI queries. Owns a GenAIClient; callers just call methods."""

    def __init__(self, genai_cfg: dict):
        self._client = GenAIClient(genai_cfg)

    # -- Job scoring --------------------------------------------------------

    def score_jobs(
        self,
        jobs: list[JobPosting],
        resume: str,
        scoring_instructions: str,
        additional_experience: str,
    ) -> list[tuple[JobPosting, int, str]]:
        """Score one batch of jobs (1-10) against the candidate rubric.

        Args:
            jobs:                  Batch of postings to score (caller handles batching).
            resume:                The candidate's base resume (from data/resume_v5).
            scoring_instructions:  Scoring rubric (from config.yaml).
            additional_experience: Extra experience context (from yaml).

        Returns:
            List of (job, score, reason) tuples, one per input job, same order.
        """
        prompt = (
            "Use the candidate's resume below to judge how well they fit each job "
            "description that follows, applying the scoring instructions.\n\n"
            "## Candidate Resume\n"
            f"{resume}\n\n"
            "## Scoring Instructions\n"
            f"{scoring_instructions}\n\n"
            "## Additional Candidate Experience Details\n"
            f"{additional_experience}\n\n"
            "## Job Postings\n"
            f"{self._build_jobs_text(jobs)}\n\n"
            "Your response should contain, for EACH job, the reasoning through the scoring steps in "
            "order as text — after each step, state whether it passes and why. Then end your response "
            "with a single JSON array inside a ```json code block; I will be parsing that block.\n"
            'Format: [{"job_id": "...", "score": N, "reason": "..."}, ...]\n'
            "One element per job, same order, omit none."
        )

        content = self._client.chat(prompt)

        # The model reasons in prose first, then emits the array in a ```json block.
        # Prefer the last fenced block; fall back to a bracket slice if it forgot the fence.
        fences = re.findall(r"```(?:json)?\s*(.*?)```", content, re.DOTALL)
        if fences:
            content = fences[-1].strip()
        else:
            last_start = content.rfind("[")
            last_end = content.rfind("]")
            if last_start != -1 and last_end != -1:
                content = content[last_start:last_end + 1]

        try:
            scores = json.loads(content)
        except json.JSONDecodeError as e:
            logger.error("Failed to parse LLM response as JSON: %s\nRaw content:\n%s", e, content)
            raise

        results = []
        for job, entry in zip(jobs, scores):
            results.append((job, int(entry.get("score", 0)), entry.get("reason", "")))
        return results

    @staticmethod
    def _build_jobs_text(jobs: list[JobPosting]) -> str:
        """Format a batch of job postings into a numbered text block for the prompt.

        Oversized descriptions are truncated to stay within prompt limits.
        """
        parts = []
        for i, job in enumerate(jobs, 1):
            desc = job.description if job.description else "(no description available)"
            if len(desc) > _MAX_JOB_DESCRIPTION_CHARS:
                logger.warning(
                    "Trimming job description from %s to %s chars for job '%s' (ID: %s). "
                    "First 200 chars: %s",
                    len(desc), _MAX_JOB_DESCRIPTION_CHARS, job.title, job.job_id, desc[:200],
                )
                desc = desc[:_MAX_JOB_DESCRIPTION_CHARS]
            parts.append(
                f"--- Job {i} ---\n"
                f"ID: {job.job_id}\n"
                f"Title: {job.title}\n"
                f"Company: {job.company}\n"
                f"Location: {job.location}\n"
                f"Description:\n{desc}\n"
            )
        return "\n".join(parts)

    # -- Resume tailoring ---------------------------------------------------

    def tailor_resume(
        self,
        base_resume: str,
        job_description: str,
        additional_bullets: str = "",
    ) -> str | None:
        """Tailor a base LaTeX resume to a job description.

        Args:
            base_resume:        The base resume LaTeX source.
            job_description:    The target JD text.
            additional_bullets: Optional extra experience bullets (from yaml). When
                                provided, an experience-boost section is appended.

        Returns:
            Cleaned LaTeX source, or None if the LLM output doesn't look like LaTeX.
            Propagates the underlying exception if the LLM call itself fails.
        """
        prompt = _TAILOR_PROMPT.format(base_resume=base_resume, job_description=job_description)
        if additional_bullets:
            prompt += _EXPERIENCE_BOOST_PROMPT.format(additional_bullets=additional_bullets)

        tex = self._client.chat(prompt).strip()

        # Strip markdown fences if the LLM adds them despite instructions
        if tex.startswith("```"):
            tex = tex[tex.find("\n") + 1:]
        if tex.endswith("```"):
            tex = tex[:-3].rstrip()

        if "\\documentclass" not in tex:
            return None
        return tex
