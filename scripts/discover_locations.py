"""
scripts/discover_locations.py — Workday Location Facet Discovery
================================================================
Queries each Workday portal's API to discover India-relevant location facet IDs,
then writes them to config/workday_locations.yaml for per-portal filtering.

Usage:
    python scripts/discover_locations.py

Why this exists:
    Workday facet IDs (locationCountry, locationRegionStateProvince, locations)
    are tenant-specific — each company's portal has its own UUIDs. A global config
    only works for some portals and silently returns unfiltered results for others.
    This script discovers the correct IDs for each portal automatically.

Approach:
    Recursively walks the entire facets JSON response looking for any object with
    a "descriptor" + "id" where the descriptor matches a known Indian location.
    Groups matches by their facetParameter so main.py can build appliedFacets.
"""

import logging
import sys
from pathlib import Path
from urllib.parse import urlparse

import httpx
import yaml

# Add project root to path so we can import config/scrapers
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import load_urls
from scrapers.workday import _build_api_base

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("discover_locations")

OUTPUT_PATH = PROJECT_ROOT / "config" / "workday_locations.yaml"

# Location names to match (case-insensitive)
# "India" needs word-boundary matching to avoid "Indiana", "Indianapolis" etc.
INDIAN_LOCATIONS = [
    # Cities
    "Bengaluru", "Bangalore",
    "Hyderabad", "Hydrabad",
    # States
    "Karnataka", "Karnātaka",
    "Telangana", "Telangāna",
    "Andhra Pradesh"
]

# Patterns that need word-boundary check (would otherwise match Indiana, Indianapolis, etc.)
_WORD_BOUNDARY_PATTERNS = ["india"]

_LOCATION_VARIANTS = [name.lower() for name in INDIAN_LOCATIONS]


def _strip_diacritics(text: str) -> str:
    """Remove diacritical marks (accents) from text.

    e.g., "Karnātaka" → "Karnataka", "Mahārāshtra" → "Maharashtra"
    """
    import unicodedata
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _is_india_location(descriptor: str) -> bool:
    """Check if a facet descriptor matches a known Indian location."""
    lower = _strip_diacritics(descriptor).lower()

    # Check city/state names (simple substring)
    if any(variant in lower for variant in _LOCATION_VARIANTS):
        return True

    # Check "India" with word boundary — must not be followed by more letters
    # Matches: "India", "India,", "India " but NOT "Indiana", "Indianapolis"
    import re
    for pattern in _WORD_BOUNDARY_PATTERNS:
        if re.search(rf'\b{pattern}\b', lower):
            return True

    return False


def _portal_key(url: str) -> str:
    """Build a readable key from a Workday portal URL.

    e.g., "https://gea.wd3.myworkdayjobs.com/en-US/GEACareers"
       -> "gea.wd3.myworkdayjobs.com/en-US/GEACareers"
    """
    parsed = urlparse(url)
    return f"{parsed.hostname}{parsed.path.rstrip('/')}"


def _fetch_facets(url: str, client: httpx.Client) -> list[dict]:
    """POST to the Workday API with empty facets to get all available facet data."""
    api_base = _build_api_base(url)
    payload = {
        "appliedFacets": {},
        "limit": 1,
        "offset": 0,
        "searchText": "",
    }
    try:
        resp = client.post(f"{api_base}/jobs", json=payload)
        resp.raise_for_status()
        data = resp.json()
        return data.get("facets", [])
    except (httpx.HTTPError, ValueError) as e:
        logger.error("Failed to fetch facets from %s: %s", url, e)
        return []


def _walk_facets(obj, current_param=None, matches=None):
    """Recursively walk the facets JSON tree and collect India-matching entries.

    Looks for any dict with "descriptor" + "id" where descriptor matches an
    Indian location. Groups results by the nearest parent facetParameter.

    Args:
        obj:           The JSON object to walk (dict, list, or primitive).
        current_param: The facetParameter of the nearest parent facet group.
        matches:       Dict of {facetParameter: [{id, descriptor}, ...]} being built up.

    Returns:
        The matches dict.
    """
    if matches is None:
        matches = {}

    if isinstance(obj, dict):
        # Track the current facetParameter as we descend
        param = obj.get("facetParameter", current_param)

        # Check if this dict is a leaf entry with descriptor + id
        descriptor = obj.get("descriptor", "")
        entry_id = obj.get("id")
        if entry_id and descriptor and _is_india_location(descriptor):
            if param:
                matches.setdefault(param, []).append({
                    "id": entry_id,
                    "descriptor": descriptor,
                })

        # Recurse into all values
        for val in obj.values():
            _walk_facets(val, param, matches)

    elif isinstance(obj, list):
        for item in obj:
            _walk_facets(item, current_param, matches)

    return matches


def _discover_for_portal(url: str, client: httpx.Client) -> dict | None:
    """Discover India-relevant location facets for one Workday portal.

    Recursively searches the entire facets response for India-matching entries.

    Returns:
        A dict of {facetParameter: [{id, descriptor}, ...]}, or None if nothing found.
    """
    facets = _fetch_facets(url, client)
    if not facets:
        return None

    matches = _walk_facets(facets)
    return matches if matches else None


def main():
    """Discover India location facets for all Workday portals and write to YAML."""
    urls = load_urls()
    workday_urls = [(t, u) for t, u in urls if t == "workday"]

    if not workday_urls:
        logger.warning("No workday URLs found in urls.yaml")
        return

    logger.info("Discovering location facets for %d Workday portals...", len(workday_urls))

    client = httpx.Client(
        timeout=30,
        verify=False,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/125.0.0.0 Safari/537.36",
        },
        follow_redirects=True,
    )

    results = {}
    summary = []

    for _, url in workday_urls:
        key = _portal_key(url)
        logger.info("Querying %s ...", key)

        portal_result = _discover_for_portal(url, client)
        if portal_result:
            results[key] = portal_result
            total = sum(len(v) for v in portal_result.values())
            params = ", ".join(portal_result.keys())
            summary.append((key, total, params))
            logger.info("  Found %d India location(s) in [%s]", total, params)
        else:
            summary.append((key, 0, "none"))
            logger.warning("  No India locations found")

    client.close()

    # Write YAML output
    header = (
        "# Auto-generated by scripts/discover_locations.py\n"
        "# Re-run to refresh: python scripts/discover_locations.py\n\n"
    )
    with open(OUTPUT_PATH, "w") as f:
        f.write(header)
        yaml.dump(results, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    logger.info("Wrote %s", OUTPUT_PATH)

    # Print summary
    print("\n" + "=" * 60)
    print("Discovery Summary")
    print("=" * 60)
    for key, count, params in summary:
        status = f"{count} locations [{params}]" if count > 0 else "NO INDIA LOCATIONS"
        print(f"  {key}: {status}")
    print(f"\nOutput: {OUTPUT_PATH}")
    print(f"Portals with India locations: {sum(1 for _, c, _ in summary if c > 0)}/{len(summary)}")


if __name__ == "__main__":
    main()
