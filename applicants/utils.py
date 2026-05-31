"""
applicants/utils.py — Shared Utilities for All Applicants
"""

import functools
import logging
import time

logger = logging.getLogger(__name__)


def retry(max_retries=3, backoff=0, exponential=False):
    """Decorator to retry a function on exception.

    Args:
        max_retries: Total number of attempts (default 3).
        backoff:     Base sleep time in seconds between retries (default 0).
        exponential: If True, sleep = backoff * 2^attempt; otherwise constant.
    """
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_retries):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    last_exc = e
                    logger.warning("%s attempt %d/%d failed: %s", fn.__name__, attempt + 1, max_retries, e)
                    if attempt < max_retries - 1 and backoff > 0:
                        sleep_time = backoff * (2 ** attempt) if exponential else backoff
                        time.sleep(sleep_time)
            raise last_exc
        return wrapper
    return decorator
