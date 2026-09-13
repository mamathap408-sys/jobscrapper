"""
config.py — Configuration Loader
=================================
Loads settings from config.yaml and portal URLs from urls.yaml.

How it works:
  1. Reads config.yaml using PyYAML
  2. Recursively scans all string values for ${VAR_NAME} patterns
  3. Replaces them with actual environment variable values
  4. If an env var is missing, raises ValueError (fail-fast)

config.yaml contains:
  - scoring_instructions: candidate resume + scoring rubric sent to the LLM
  - genai: LLM gateway credentials and model settings
  - email: sender/recipient Gmail addresses
  - schedule: how often to check and delay between sites

urls.yaml format:
  Each line is: scraper_type | url
  Example: workday | https://company1.wd5.myworkdayjobs.com/en-US/External
  Lines starting with # are ignored (comments)
"""

import os
import re
from pathlib import Path

import yaml

# Paths are relative to this file's directory (the project root)
CONFIG_PATH = Path(__file__).parent / "config.yaml"
URLS_PATH = Path(__file__).parent / "urls.yaml"
ANSWERS_PATH = Path(__file__).parent / "workday_answers.yaml"

# Regex to match ${VAR_NAME} patterns in config values
_ENV_VAR_PATTERN = re.compile(r"\$\{(\w+)}")


def _resolve_env_vars(value):
    """Recursively replace ${VAR_NAME} placeholders with environment variable values.

    Walks through the entire config tree (dicts, lists, strings) and substitutes
    any ${VAR_NAME} with the value of that environment variable.

    Args:
        value: Can be a string, dict, list, or any other type.
               Only strings are checked for ${...} patterns.

    Returns:
        The same structure with all env var references resolved.

    Raises:
        ValueError: If a referenced env var is not set in the environment.
    """
    if isinstance(value, str):
        def _replace(match):
            var_name = match.group(1)
            env_val = os.environ.get(var_name)
            if env_val is None:
                raise ValueError(f"Environment variable '{var_name}' is not set")
            return env_val
        return _ENV_VAR_PATTERN.sub(_replace, value)
    if isinstance(value, dict):
        return {k: _resolve_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_env_vars(item) for item in value]
    return value


def load_config(path: Path = CONFIG_PATH) -> dict:
    """Load config.yaml and resolve any ${ENV_VAR} references.

    Returns:
        dict: The fully resolved configuration dictionary.
    """
    with open(path) as f:
        raw = yaml.safe_load(f)
    return _resolve_env_vars(raw)


def load_answers(path: Path = ANSWERS_PATH) -> dict:
    """Load workday_answers.yaml (personal data + Q&A for auto-apply) and resolve env vars.

    Returns:
        dict: The fully resolved answers dictionary.
    """
    with open(path) as f:
        raw = yaml.safe_load(f)
    return _resolve_env_vars(raw)


def load_base_resume(config: dict) -> str:
    """Load the base resume file referenced by resume_builder.base_resume.

    Raises:
        FileNotFoundError: If the configured resume file does not exist.
    """
    rb_cfg = config.get("resume_builder", {})
    path = Path(rb_cfg.get("base_resume", "data/resume_v5"))
    if not path.is_absolute():
        path = Path(__file__).parent.parent / path
    return path.read_text(encoding="utf-8")


def load_exp_additional_bullets(config: dict) -> str:
    """Load config/exp_additional_bullets.yaml and format it as a text block.

    Formats each role's bullets as:
        For "<role>":
        - bullet
        - bullet

    Used by both the resume tailoring prompt and the job scoring prompt, so the
    two stay in sync with a single source of truth.
    """
    rb_cfg = config.get("resume_builder", {})
    path = Path(rb_cfg.get("exp_additional_bullets", "config/exp_additional_bullets.yaml"))
    if not path.is_absolute():
        path = Path(__file__).parent.parent / path
    exp_bullets = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return "\n\n".join(
        f'For "{role}":\n' + "\n".join(f"- {b}" for b in items)
        for role, items in exp_bullets.items()
    )


def load_urls(path: Path = URLS_PATH) -> list[tuple[str, str]]:
    """Load urls.yaml and parse each line into (scraper_type, url) pairs.

    Format per line: "scraper_type | url"
    Blank lines and lines starting with # are skipped.

    Returns:
        List of (scraper_type, url) tuples.
        Example: [("workday", "https://..."), ("amazon", "https://...")]
    """
    urls = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("|", 1)
            if len(parts) != 2:
                continue
            scraper_type = parts[0].strip().lower()
            url = parts[1].strip()
            urls.append((scraper_type, url))
    return urls
