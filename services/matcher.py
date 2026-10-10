"""
matcher.py — GenAI Job Matching
=================================
Scores job postings against the candidate resume + rubric using a GenAI LLM.

How it works:
  1. Takes scoring_instructions (embedded candidate resume + rubric) from config.yaml
  2. Takes a batch of new job postings (up to 5 at a time)
  3. Builds a prompt asking the LLM to score each job 1-10 and explain why
  4. Sends the prompt to the GenAI Gateway (via GenAIClient)
  5. Parses the JSON response to get (score, reason) per job
  6. Only jobs scoring >= match_threshold (default 6) get emailed
"""

import logging

from config.loader import load_base_resume, load_exp_additional_bullets
from scrapers.base import JobPosting
from services.genai_queries import GenAIQueries

logger = logging.getLogger(__name__)

# Send up to 5 jobs per LLM call to reduce API usage and costs
_BATCH_SIZE = 5


class JobMatcher:
    """Scores job postings against user profile using the GenAI Gateway."""

    def __init__(self, config: dict):
        """Initialize the matcher with config.yaml settings.

        Args:
            config: The full config dictionary (from load_config()).
        """
        genai_cfg = config["genai"]
        self._queries = GenAIQueries(genai_cfg)
        self._threshold = genai_cfg.get("match_threshold", 6)
        self._scoring_instructions = config["scoring_instructions"]
        self._resume = load_base_resume(config)
        self._additional_experience = load_exp_additional_bullets(config)

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
            results.extend(self._queries.score_jobs(
                batch, self._resume,
                self._scoring_instructions, self._additional_experience,
            ))
        return results
