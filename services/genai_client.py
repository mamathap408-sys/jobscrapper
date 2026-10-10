"""
genai_client.py — GenAI Gateway HTTP Client
=================================================
Handles authentication, request retry with exponential backoff, and response
parsing for the GenAI Gateway.
"""

import base64
import logging
import os
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
        # Seed from GENAI_TOKEN if a caller (e.g. main.py spawning apply.py) already has a
        # valid cached token — skips the login round-trip entirely. Falls back to a normal
        # login on the first 401 if this is missing, empty, or stale.
        self._token = os.environ.get("GENAI_TOKEN") or None
        self._client = httpx.Client(timeout=60, verify=False)

        pricing = genai_cfg.get("pricing", {})
        self._input_price_per_million = pricing.get("input_per_million")
        self._output_price_per_million = pricing.get("output_per_million")
        self._cache_write_price_per_million = pricing.get("cache_write_per_million")
        self._cache_read_price_per_million = pricing.get("cache_read_per_million")
        self._total_input_tokens = 0
        self._total_output_tokens = 0
        self._total_cache_write_tokens = 0
        self._total_cache_read_tokens = 0
        self._next_cost_log_threshold = 1.0  # log once cumulative cost crosses each whole dollar

    def _calc_cost(self, input_tok: int, output_tok: int, cache_write_tok: int, cache_read_tok: int) -> float | None:
        """Cost for the given token counts, or None if pricing isn't fully configured."""
        prices = (
            self._input_price_per_million, self._output_price_per_million,
            self._cache_write_price_per_million, self._cache_read_price_per_million,
        )
        if any(p is None for p in prices):
            return None
        return (
            input_tok / 1_000_000 * self._input_price_per_million
            + output_tok / 1_000_000 * self._output_price_per_million
            + cache_write_tok / 1_000_000 * self._cache_write_price_per_million
            + cache_read_tok / 1_000_000 * self._cache_read_price_per_million
        )

    def _current_cost(self) -> float | None:
        """Cumulative cost so far, or None if pricing isn't fully configured."""
        return self._calc_cost(
            self._total_input_tokens, self._total_output_tokens,
            self._total_cache_write_tokens, self._total_cache_read_tokens,
        )

    def _record_usage(self, response_json: dict):
        """Accumulate token counts (including cache write/read), logging per-query cost at
        DEBUG and a running cumulative total, plus an INFO line once per $1 spent overall."""
        usage = response_json.get("full_model_response", {}).get("usage", {})
        in_tok = usage.get("input_tokens", 0)
        out_tok = usage.get("output_tokens", 0)
        cache_write_tok = usage.get("cache_creation_input_tokens", 0)
        cache_read_tok = usage.get("cache_read_input_tokens", 0)

        self._total_input_tokens += in_tok
        self._total_output_tokens += out_tok
        self._total_cache_write_tokens += cache_write_tok
        self._total_cache_read_tokens += cache_read_tok

        query_cost = self._calc_cost(in_tok, out_tok, cache_write_tok, cache_read_tok)
        cost = self._current_cost()
        query_cost_str = f"${query_cost:.5f}" if query_cost is not None else "n/a"
        cost_str = f"${cost:.4f}" if cost is not None else "n/a"
        logger.debug(
            "GenAI query cost: %s (%d input + %d output + %d cache-write + %d cache-read tokens) "
            "— cumulative so far: %s (%d in, %d out, %d cache-write, %d cache-read)",
            query_cost_str, in_tok, out_tok, cache_write_tok, cache_read_tok,
            cost_str, self._total_input_tokens, self._total_output_tokens,
            self._total_cache_write_tokens, self._total_cache_read_tokens,
        )

        if cost is not None and cost >= self._next_cost_log_threshold:
            logger.info(
                "GenAI usage: $%.2f spent — %d input + %d output + %d cache-write + %d cache-read tokens",
                cost, self._total_input_tokens, self._total_output_tokens,
                self._total_cache_write_tokens, self._total_cache_read_tokens,
            )
            self._next_cost_log_threshold = int(cost) + 1

    def _get_auth_token(self) -> str:
        """POST /auth/login to get a fresh JWT access token."""
        resp = self._client.post(
            self._login_url,
            json={"username": self._username, "password": self._password},
            headers={"Content-Type": "application/json"},
        )
        if resp.status_code == 200:
            self._token = resp.json().get("access_token")
            # Written back to this process's own environment so any subprocess
            # spawned afterward (e.g. main.py spawning apply.py) inherits it and
            # skips its own login — subprocess.run() inherits os.environ by default.
            if self._token:
                os.environ["GENAI_TOKEN"] = self._token
            return self._token
        logger.error("GenAI login failed (%d): %s", resp.status_code, resp.text)
        raise RuntimeError(f"GenAI login failed with status {resp.status_code}")

    def _ensure_token(self) -> str:
        """Return the current token, fetching a new one if missing."""
        if not self._token:
            logger.info("GenAI token missing, authenticating...")
            self._get_auth_token()
        return self._token

    def chat(self, prompt: str, system_role: str = None) -> str:
        """Send a prompt to the GenAI gateway and return the LLM's text response.

        Retries with exponential backoff (factor 1.5) on failure. Re-authenticates
        on 401. Raises RuntimeError after all retries are exhausted.

        Args:
            prompt:      The full prompt string to send to the LLM.
            system_role: Optional system message. Defaults to job matching assistant.

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
                        {"role": "system", "content": system_role or "You are a job matching assistant."},
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
                resp_json = resp.json()
                self._record_usage(resp_json)
                return self._parse_response(resp_json)
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

    def chat_with_history(self, query: list[dict]) -> str:
        """Send a multi-turn conversation to the GenAI gateway.

        Same retry/auth logic as chat(), but takes a pre-built message list
        for multi-turn conversations.

        Args:
            query: Full message list [{role, content}, ...] including system message.

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
                    "query": query,
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
                resp_json = resp.json()
                self._record_usage(resp_json)
                return self._parse_response(resp_json)
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

    def close(self):
        """Close the httpx client."""
        self._client.close()


class ChatSession:
    """Multi-turn conversation with the GenAI gateway.

    Maintains message history so the LLM has context from previous turns.
    """

    def __init__(self, client: GenAIClient, system_role: str):
        self._client = client
        self._system_role = system_role
        self._history: list[dict] = []

    def send(self, message: str) -> str:
        """Send a message and get a response, maintaining conversation history."""
        self._history.append({"role": "user", "content": message})
        query = [{"role": "system", "content": self._system_role}]
        query.extend(self._history)
        response_text = self._client.chat_with_history(query)
        self._history.append({"role": "assistant", "content": response_text})
        return response_text

    def reset(self):
        """Clear conversation history."""
        self._history = []
