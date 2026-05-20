"""
applicants/__init__.py — Applicant Registry
=============================================
Maps applicant type labels to their implementing classes.
Detects applicant type from job URLs.

To add a new applicant type:
  1. Create the class in a new file (e.g., applicants/successfactors.py)
  2. Import it here
  3. Add it to APPLICANT_REGISTRY
  4. Add URL pattern to _URL_PATTERNS
"""

from applicants.workday import WorkdayApplicant
from applicants.successfactors import SuccessFactorsApplicant

APPLICANT_REGISTRY: dict[str, type] = {
    "workday": WorkdayApplicant,
    "successfactors": SuccessFactorsApplicant,
}

# URL substring → applicant type. Checked in order.
_URL_PATTERNS: list[tuple[str, str]] = [
    ("myworkdayjobs.com", "workday"),
    ("careers.hcltech.com", "successfactors"),
    ("careers.wipro.com", "successfactors"),
    ("careers.capgemini.com", "successfactors"),
]


def detect_applicant_type(url: str) -> str | None:
    """Detect the applicant type from a job URL.

    Returns:
        The applicant type string (e.g., "workday"), or None if unknown.
    """
    url_lower = url.lower()
    for pattern, applicant_type in _URL_PATTERNS:
        if pattern in url_lower:
            return applicant_type
    return None


def get_applicant(applicant_type: str, **kwargs):
    """Create and return an applicant instance by its type label.

    Args:
        applicant_type: The label (e.g., "workday").
        **kwargs: Arguments passed to the applicant constructor.

    Returns:
        An instance of the matching applicant class.

    Raises:
        ValueError: If the applicant_type is not in the registry.
    """
    cls = APPLICANT_REGISTRY.get(applicant_type)
    if cls is None:
        raise ValueError(
            f"Unknown applicant type '{applicant_type}'. "
            f"Available: {list(APPLICANT_REGISTRY.keys())}"
        )
    return cls(**kwargs)
