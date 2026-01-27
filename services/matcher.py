"""
matcher.py — GenAI Job Matching
=================================
Scores job postings against the user's profile using a GenAI LLM.

How it works:
  1. Takes the user's profile (skills, experience, preferences) from config.yaml
  2. Takes a batch of new job postings (up to 5 at a time)
  3. Builds a prompt asking the LLM to score each job 1-10 and explain why
  4. Sends the prompt to the GenAI Gateway (via GenAIClient)
  5. Parses the JSON response to get (score, reason) per job
  6. Only jobs scoring >= match_threshold (default 6) get emailed
"""

import json
import logging

from scrapers.base import JobPosting, JobProfile
from services.genai_client import GenAIClient

logger = logging.getLogger(__name__)

# Send up to 5 jobs per LLM call to reduce API usage and costs
_BATCH_SIZE = 5


def _build_profile_text(profiles: list[JobProfile]) -> str:
    """Format the user's profiles into a readable text block for the LLM prompt.

    Args:
        profiles: List of JobProfile objects loaded from config.yaml.

    Returns:
        A formatted string with each profile numbered, e.g.:
            --- Profile 1: Software Developer ---
            Title: Software Developer
            Skills: Java, Spring Boot, Python
            ...
    """
    sections = []
    for i, profile in enumerate(profiles, 1):
        skills = ", ".join(profile.skills)
        tools = ", ".join(profile.tools)
        text = (
            f"--- Profile {i}: {profile.title} ---\n"
            f"Title: {profile.title or 'N/A'}\n"
            f"Skills: {skills}\n"
            f"Experience: {profile.experience_years} years\n"
            f"Location preference: {profile.location_preference or 'Any'}\n"
            f"Additional criteria:\n{profile.additional_criteria or 'None'}"
        )
        if tools:
            text += f"\nTools: {tools}"
        sections.append(text)
    return "\n\n".join(sections)


def _build_jobs_text(jobs: list[JobPosting]) -> str:
    """Format a batch of job postings into a numbered text block for the LLM prompt.

    Each job's description is truncated to 1500 chars to stay within token limits.

    Args:
        jobs: List of JobPosting objects to include in the prompt.

    Returns:
        Formatted string with each job numbered (Job 1, Job 2, etc.)
    """
    parts = []
    for i, job in enumerate(jobs, 1):
        desc_preview = job.description[:1500] if job.description else "(no description available)"
        parts.append(
            f"--- Job {i} ---\n"
            f"ID: {job.job_id}\n"
            f"Title: {job.title}\n"
            f"Company: {job.company}\n"
            f"Location: {job.location}\n"
            f"Description:\n{desc_preview}\n"
        )
    return "\n".join(parts)


class JobMatcher:
    """Scores job postings against user profile using the GenAI Gateway."""

    def __init__(self, config: dict):
        """Initialize the matcher with config.yaml settings.

        Args:
            config: The full config dictionary (from load_config()).
        """
        genai_cfg = config["genai"]
        self._client = GenAIClient(genai_cfg)
        self._threshold = genai_cfg.get("match_threshold", 6)
        self._profiles = config["profiles"]
        self._profile_text = _build_profile_text(self._profiles)
        self._scoring_instructions = config["scoring_instructions"]

    @property
    def threshold(self) -> int:
        """The minimum score (1-10) required for a job to trigger an email notification."""
        return self._threshold

    def match_jobs(self, jobs: list[JobPosting]) -> list[tuple[JobPosting, int, str]]:
        """Score a list of new jobs against the user's profile.

        Processes jobs in batches of 5 to manage LLM token usage.

        Args:
            jobs: New job postings to score.

        Returns:
            List of (job, score, reason) tuples for ALL jobs.
            Score is 1-10, reason is a brief explanation from the LLM.
        """
        results = []
        for i in range(0, len(jobs), _BATCH_SIZE):
            batch = jobs[i : i + _BATCH_SIZE]
            results.extend(self._match_batch(batch))
        return results

    def _match_batch(self, jobs: list[JobPosting]) -> list[tuple[JobPosting, int, str]]:
        """Send a batch of jobs to the LLM and get relevance scores.

        Args:
            jobs: Batch of 1-5 job postings to score.

        Returns:
            List of (job, score, reason) tuples.
        """
        jobs_text = _build_jobs_text(jobs)

        scoring_section = (
            "## Scoring Instructions\n"
            f"{self._scoring_instructions}\n\n"
        )

        prompt = (
            f"{scoring_section}"
            "## Job Postings\n"
            f"{jobs_text}\n\n"
            "Respond ONLY with a JSON array (no markdown fences). Each element:\n"
            '{"job_id": "...", "score": N, "reason": "..."}\n'
            "Return one element per job, in the same order."
        )

        content = self._client.chat(prompt)

        # Some models wrap JSON in ```json ... ``` fences despite instructions
        if content.startswith("```"):
            content = content.split("\n", 1)[1]
            content = content.rsplit("```", 1)[0]

        scores = json.loads(content)

        results = []
        for job, score_entry in zip(jobs, scores):
            score = int(score_entry.get("score", 0))
            reason = score_entry.get("reason", "")
            results.append((job, score, reason))
        return results

    def close(self):
        """Close the GenAI client."""
        self._client.close()
