"""
services/logging_setup.py — Shared logging setup for the CLI entry points
=========================================================================
Console gets INFO and above; <log_dir>/<name>_<run_no>.log gets everything (DEBUG and above).

run_no comes from <log_dir>/run-no.txt and goes up by one on every run, unless the caller
passes one in (main.py hands its run_no to the apply.py processes it spawns). When the file
passes 4 MB it is renamed to <name>_<run_no>.<timestamp>.log and a fresh
<name>_<run_no>.log is started. A relative log_dir is resolved from the working directory.
"""

import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

_MAX_BYTES = 4 * 1024 * 1024


def setup_logging(log_dir: str, name: str, run_no: int | None = None) -> int:
    """Attach console (INFO+) and rolling file (DEBUG+) handlers to the root logger.

    Args:
        log_dir: Folder for log files, from the --log-dir CLI argument.
        name:    Log file prefix, e.g. "apply" → apply_7.log, apply_7.2026-09-25_192302.log.
        run_no:  Reuse this run number (e.g. apply.py spawned by main.py) instead of
                 taking the next one from run-no.txt. The log file is appended to.

    Returns:
        The run number used.
    """
    path = Path(log_dir)
    path.mkdir(parents=True, exist_ok=True)
    if run_no is None:
        counter = path / "run-no.txt"
        run_no = int(counter.read_text()) + 1 if counter.exists() else 1
        counter.write_text(str(run_no))
    stem = f"{name}_{run_no}"
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s", "%Y-%m-%d %H:%M:%S")

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)

    # backupCount=1 enables rollover; the namer gives each rolled file a unique timestamped name
    file_handler = RotatingFileHandler(path / f"{stem}.log", maxBytes=_MAX_BYTES, backupCount=1, encoding="utf-8")
    file_handler.namer = lambda _: str(path / f"{stem}.{datetime.now():%Y-%m-%d_%H%M%S}.log")
    file_handler.setLevel(logging.DEBUG)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    for handler in (console, file_handler):
        handler.setFormatter(formatter)
        root.addHandler(handler)

    # httpcore/httpx log every request/response line-by-line at DEBUG (headers,
    # body chunks, connection lifecycle) — too noisy even for the file handler.
    logging.getLogger("httpcore").setLevel(logging.INFO)
    logging.getLogger("httpx").setLevel(logging.INFO)
    logging.getLogger(__name__).info("Run %d — logging to %s", run_no, path / f"{stem}.log")
    return run_no
