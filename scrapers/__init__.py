"""
scrapers/__init__.py — Scraper Registry
========================================
Maps scraper type labels (from urls.txt) to their implementing classes.

When you add "workday | https://..." in urls.txt, the "workday" label is looked up
in SCRAPER_REGISTRY to find WorkdayScraper. Same for "generic" → GenericScraper.

To add a new scraper type:
  1. Create the scraper class in a new file (e.g., scrapers/greenhouse.py)
  2. Import it here
  3. Add it to SCRAPER_REGISTRY with a label name
  4. Use that label in urls.txt
"""

from scrapers.workday import WorkdayScraper
from scrapers.generic import GenericScraper
from scrapers.amazon import AmazonScraper

# Maps the label used in urls.txt → the scraper class to instantiate
SCRAPER_REGISTRY: dict[str, type] = {
    "workday": WorkdayScraper,
    "generic": GenericScraper,
    "amazon": AmazonScraper,
}


def get_scraper(scraper_type: str, **kwargs):
    """Create and return a scraper instance by its type label.

    Args:
        scraper_type: The label from urls.txt (e.g., "workday", "generic").
        **kwargs: Extra arguments passed to the scraper constructor.
                  For example, GenericScraper needs browser=playwright_browser.

    Returns:
        An instance of the matching scraper class.

    Raises:
        ValueError: If the scraper_type is not in the registry.
    """
    cls = SCRAPER_REGISTRY.get(scraper_type)
    if cls is None:
        raise ValueError(
            f"Unknown scraper type '{scraper_type}'. "
            f"Available: {list(SCRAPER_REGISTRY.keys())}"
        )
    return cls(**kwargs)
