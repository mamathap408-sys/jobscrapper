"""
config.py — Configuration Loader
=================================
Loads settings from config.yaml and portal URLs from urls.yaml.

How it works:
  Reads config.yaml using PyYAML and returns it as a dict.

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

import re
from pathlib import Path

import yaml

# Paths are relative to this file's directory (the project root)
CONFIG_PATH = Path(__file__).parent / "config.yaml"
URLS_PATH = Path(__file__).parent / "urls.yaml"
ANSWERS_PATH = Path(__file__).parent / "workday_answers.yaml"


def load_config(path: Path = CONFIG_PATH) -> dict:
    """Load config.yaml.

    Returns:
        dict: The configuration dictionary.
    """
    with open(path) as f:
        return yaml.safe_load(f)


def load_answers(path: Path = ANSWERS_PATH) -> dict:
    """Load workday_answers.yaml (personal data + Q&A for auto-apply).

    Returns:
        dict: The answers dictionary.
    """
    with open(path) as f:
        return yaml.safe_load(f)


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
    Blank lines and full-line comments (starting with #) are skipped. Inline
    comments after the URL (" # note...") are stripped, so trailing notes don't
    become part of the URL.

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
            # Drop any inline comment (whitespace followed by '#' to end of line).
            # Requiring leading whitespace avoids truncating a real URL fragment.
            line = re.split(r"\s+#", line, maxsplit=1)[0].strip()
            parts = line.split("|", 1)
            if len(parts) != 2:
                continue
            scraper_type = parts[0].strip().lower()
            url = parts[1].strip()
            urls.append((scraper_type, url))
    return urls
