"""
matcher.py — GenAI Job Matching
=================================
Scores job postings against the user's profile using a GenAI LLM.

How it works:
  1. Takes the user's profile (skills, experience, preferences) from config.yaml
  2. Takes a batch of new job postings (up to 5 at a time)
  3. Builds a prompt asking the LLM to score each job 1-10 and explain why
  4. Sends the prompt to the GenAI Gateway (OpenAI-compatible API)
  5. Parses the JSON response to get (score, reason) per job
  6. Only jobs scoring >= match_threshold (default 6) get emailed

Authentication flow (GenAI Gateway):
  1. POST /auth/login with username + password → get a JWT access_token
  2. GET /auth/validate to check if token is still valid
  3. POST /queries/chat with Bearer token to send the actual LLM prompt
  4. Token auto-refreshes when expired

The gateway supports multiple models (Claude, GPT, Gemini). The response format
differs by model, so _parse_response() handles both Claude and OpenAI/Gemini formats.
"""

import base64
import json
import logging

import httpx

from scrapers.base import JobPosting, JobProfile

logger = logging.getLogger(__name__)

# Send up to 5 jobs per LLM call to reduce API usage and costs
_BATCH_SIZE = 5


def _decode_base64(encoded: str) -> str:
    """Decode a base64-encoded string (used for the GenAI password in config)."""
    return base64.b64decode(encoded).decode("utf-8")


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

        Reads genai section for API endpoints and credentials,
        and profile section for the user's job preferences.

        Args:
            config: The full config dictionary (from load_config()).
        """
        genai_cfg = config["genai"]
        self._login_url = genai_cfg["login_url"]        # POST here to get JWT
        self._validate_url = genai_cfg["validate_url"]   # GET here to check JWT validity
        self._chat_url = genai_cfg["chat_url"]           # POST here with LLM prompt
        self._username = genai_cfg["username"]
        self._password = _decode_base64(genai_cfg["password"])
        self._model = genai_cfg["model"]                 # e.g., "claude-sonnet-4-20250514"
        self._app_name = genai_cfg.get("application_name", "job-posting-watcher")
        self._threshold = genai_cfg.get("match_threshold", 6)  # Only email if score >= this
        self._profiles = config["profiles"]
        self._profile_text = _build_profile_text(self._profiles)
        self._scoring_instructions = config["scoring_instructions"]
        self._token = None  # JWT token, obtained on first use
        # SSL verification disabled because corporate Zscaler proxy intercepts HTTPS
        self._client = httpx.Client(timeout=60, verify=False)

    @property
    def threshold(self) -> int:
        """The minimum score (1-10) required for a job to trigger an email notification."""
        return self._threshold

    def _get_auth_token(self) -> str:
        """Login to the GenAI gateway and get a fresh JWT access token.

        POST /auth/login with username + password → receive access_token in response.

        Returns:
            The JWT access token string.

        Raises:
            RuntimeError: If login fails (wrong credentials, gateway down, etc.)
        """
        resp = self._client.post(
            self._login_url,
            json={"username": self._username, "password": self._password},
            headers={"Content-Type": "application/json"},
        )
        if resp.status_code == 200:
            self._token = resp.json().get("access_token")
            return self._token
        logger.error("GenAI login failed (%d): %s", resp.status_code, resp.text)
        raise RuntimeError(f"GenAI login failed with status {resp.status_code}")

    def _is_token_valid(self) -> bool:
        """Check if the current JWT token is still valid by calling the validate endpoint.

        Returns:
            True if token is valid, False if expired or missing.
        """
        if not self._token:
            return False
        resp = self._client.get(
            self._validate_url,
            headers={"Authorization": f"Bearer {self._token}"},
        )
        return resp.status_code == 200

    def _get_valid_token(self) -> str:
        """Return a valid token, automatically refreshing if the current one is expired.

        Returns:
            A valid JWT access token string.
        """
        if not self._is_token_valid():
            logger.info("GenAI token expired or missing, re-authenticating...")
            self._get_auth_token()
        return self._token

    def match_jobs(self, jobs: list[JobPosting]) -> list[tuple[JobPosting, int, str]]:
        """Score a list of new jobs against the user's profile.

        Processes jobs in batches of 5 to manage LLM token usage.

        Args:
            jobs: New job postings to score.

        Returns:
            List of (job, score, reason) tuples for ALL jobs.
            Score is 1-10, reason is a brief explanation from the LLM.
            Jobs scoring 0 indicate an error during matching.
        """
        results = []
        for i in range(0, len(jobs), _BATCH_SIZE):
            batch = jobs[i : i + _BATCH_SIZE]
            results.extend(self._match_batch(batch))
        return results

    def _match_batch(self, jobs: list[JobPosting]) -> list[tuple[JobPosting, int, str]]:
        """Send a batch of jobs to the LLM and get relevance scores.

        Builds a prompt containing the user profile + job descriptions,
        sends it to the GenAI gateway, and parses the JSON response.

        Args:
            jobs: Batch of 1-5 job postings to score.

        Returns:
            List of (job, score, reason) tuples. On error, returns score=0 for all.
        """
        jobs_text = _build_jobs_text(jobs)

        # The prompt asks the LLM to return a JSON array with score + reason per job
        scoring_section = (
            "## Scoring Instructions\n"
            f"{self._scoring_instructions}\n\n"
        )

        prompt = (
            "You are a job matching assistant. The candidate has multiple profiles "
            "representing different types of roles they are looking for. Score each "
            "job posting against the BEST-matching profile. For each job, provide:\n"
            "- A relevance score from 1 (terrible match) to 10 (perfect match)\n"
            "- The name of the best-matching profile\n"
            "- A brief reason (1-2 sentences)\n\n"
            "## Candidate Profiles\n"
            f"{self._profile_text}\n\n"
            f"{scoring_section}"
            "## Job Postings\n"
            f"{jobs_text}\n\n"
            "Respond ONLY with a JSON array (no markdown fences). Each element:\n"
            '{"job_id": "...", "score": N, "matched_profile": "...", "reason": "..."}\n'
            "Return one element per job, in the same order."
        )

        try:
            token = self._get_valid_token()

            # Payload format required by the GenAI Gateway
            payload = {
                "model_name": self._model,
                "user_context": {
                    "application_name": self._app_name,
                    "end_user": self._username,
                },
                "query_type": "query",
                "query": [
                    {"role": "system", "content": "You are a job matching assistant."},
                    {"role": "user", "content": prompt},
                ],
                "customized_params": {"temperature": 0.2},  # Low temp for consistent scoring
            }

            resp = self._client.post(
                self._chat_url,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {token}",
                },
            )
            resp.raise_for_status()

            # Extract the LLM's text response
            content = self._parse_response(resp.json())

            # Some models wrap JSON in ```json ... ``` fences despite instructions
            if content.startswith("```"):
                content = content.split("\n", 1)[1]
                content = content.rsplit("```", 1)[0]

            scores = json.loads(content)
        except Exception as e:
            logger.error("GenAI matching failed: %s", e)
            # Return score=0 for all jobs so they get saved but not emailed
            return [(job, 0, f"Matching error: {e}") for job in jobs]

        # Pair each score back to its corresponding job
        results = []
        for job, score_entry in zip(jobs, scores):
            score = int(score_entry.get("score", 0))
            reason = score_entry.get("reason", "")
            results.append((job, score, reason))
        return results

    def _parse_response(self, response_json: dict) -> str:
        """Extract the LLM's text from the gateway response.

        The response format differs by model:
          - Claude: {"full_model_response": {"content": [{"text": "..."}]}}
          - OpenAI/Gemini: {"full_model_response": {"choices": [{"message": {"content": "..."}}]}}

        Args:
            response_json: The raw JSON response from the GenAI gateway.

        Returns:
            The LLM's text response (should be a JSON array string).
        """
        model = self._model.lower()
        full_resp = response_json.get("full_model_response", {})
        if "claude" in model:
            return full_resp["content"][0]["text"]
        # OpenAI / Gemini format
        return full_resp["choices"][0]["message"]["content"]

    def close(self):
        """Close the httpx client (releases network connections)."""
        self._client.close()
