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
  - profiles: list of user's job profiles (skills, experience, location)
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

from scrapers.base import JobProfile

# Paths are relative to this file's directory (the project root)
CONFIG_PATH = Path(__file__).parent / "config.yaml"
URLS_PATH = Path(__file__).parent / "urls.yaml"

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
    config = _resolve_env_vars(raw)

    profiles_raw = config.get("profiles", [])
    config["profiles"] = [
        JobProfile(
            title=p.get("title", ""),
            skills=p.get("skills", []),
            tools=p.get("tools", []),
            experience_years=float(p.get("experience_years", 0)),
            location_preference=p.get("location_preference", ""),
            additional_criteria=p.get("additional_criteria", ""),
        )
        for p in profiles_raw
    ]

    return config


def load_urls(path: Path = URLS_PATH) -> list[tuple[str, str]]:
    """Load urls.yaml and parse each line into (scraper_type, url) pairs.

    Format per line: "scraper_type | url"
    Blank lines and lines starting with # are skipped.

    Returns:
        List of (scraper_type, url) tuples.
        Example: [("workday", "https://..."), ("generic", "https://...")]
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
