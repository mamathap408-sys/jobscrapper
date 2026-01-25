"""
genai_client.py — GenAI Gateway HTTP Client
=================================================
Handles authentication, request retry with exponential backoff, and response
parsing for the GenAI Gateway.
"""

import base64
import logging
import time

import httpx

logger = logging.getLogger(__name__)


def _decode_base64(encoded: str) -> str:
    """Decode a base64-encoded string (used for the GenAI password in config)."""
    return base64.b64decode(encoded).decode("utf-8")


class GenAIClient:
    """HTTP client for the GenAI Gateway with auth and retry logic."""

    def __init__(self, genai_cfg: dict):
        self._login_url = genai_cfg["login_url"]
        self._chat_url = genai_cfg["chat_url"]
        self._username = genai_cfg["username"]
        self._password = _decode_base64(genai_cfg["password"])
        self._model = genai_cfg["model"]
        self._app_name = genai_cfg.get("application_name", "job-posting-watcher")
        self._max_retries = genai_cfg.get("max_retries", 10)
        self._token = None
        self._client = httpx.Client(timeout=60, verify=False)

    def _get_auth_token(self) -> str:
        """POST /auth/login to get a fresh JWT access token."""
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

    def _ensure_token(self) -> str:
        """Return the current token, fetching a new one if missing."""
        if not self._token:
            logger.info("GenAI token missing, authenticating...")
            self._get_auth_token()
        return self._token

    def chat(self, prompt: str) -> str:
        """Send a prompt to the GenAI gateway and return the LLM's text response.

        Retries with exponential backoff (factor 1.5) on failure. Re-authenticates
        on 401. Raises RuntimeError after all retries are exhausted.

        Args:
            prompt: The full prompt string to send to the LLM.

        Returns:
            The LLM's raw text response.
        """
        delay = 1.0
        attempt = 0

        while attempt < self._max_retries:
            attempt += 1
            try:
                token = self._ensure_token()

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
                    "customized_params": {"temperature": 0.2},
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
                return self._parse_response(resp.json())
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 401:
                    logger.info("GenAI token expired, re-authenticating...")
                    self._token = None
                if attempt < self._max_retries:
                    logger.warning(
                        "GenAI request attempt %d/%d failed: %s — retrying in %.1fs",
                        attempt, self._max_retries, e, delay,
                    )
                    time.sleep(delay)
                    delay *= 1.5
                else:
                    raise RuntimeError(
                        f"GenAI request failed after {self._max_retries} attempts: {e}"
                    ) from e
            except Exception as e:
                if attempt < self._max_retries:
                    logger.warning(
                        "GenAI request attempt %d/%d failed: %s — retrying in %.1fs",
                        attempt, self._max_retries, e, delay,
                    )
                    time.sleep(delay)
                    delay *= 1.5
                else:
                    raise RuntimeError(
                        f"GenAI request failed after {self._max_retries} attempts: {e}"
                    ) from e

    def _parse_response(self, response_json: dict) -> str:
        """Extract the LLM's text from the gateway response.

        Handles both Claude and OpenAI/Gemini response formats.
        """
        model = self._model.lower()
        full_resp = response_json.get("full_model_response", {})
        if "claude" in model:
            return full_resp["content"][0]["text"]
        return full_resp["choices"][0]["message"]["content"]

    def close(self):
        """Close the httpx client."""
        self._client.close()
