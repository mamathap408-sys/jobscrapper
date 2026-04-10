"""
applicants/workday.py — Workday Auto-Apply Engine
===================================================
Automates job application submission on Workday career portals using Playwright.

Flow per job:
  1. Navigate to job URL → click "Apply" → "Autofill with Resume"
  2. Handle sign-in (auto via career_sites_credentials.yaml or manual)
  3. Upload resume PDF
  4. Fill pages dynamically: workday_fields mapping → AI fallback for unknowns
  5. Submit on final review page

Form filling uses a generic page scanner that:
  - Detects all form fields (text, select, radio, textarea)
  - Fills known fields from workday_fields in answers.yaml (keyed by data-automation-id)
  - Skips optional fields not in the mapping
  - Falls back to LLM for unknown required fields

On any failure: raises WorkdayApplyError → caller skips to next job.
"""

import json
import logging
import re
import time
import unicodedata
from pathlib import Path
from urllib.parse import urlparse

import httpx
import yaml
from playwright.sync_api import sync_playwright, Page, TimeoutError as PlaywrightTimeout

from services.genai_client import GenAIClient

logger = logging.getLogger(__name__)


# Default timeout for waiting on Workday elements (ms)
_TIMEOUT = 30_000

# ── Workday Selectors ──────────────────────────────────────────
# Keyed by logical name. These are data-automation-id based selectors
# common across most Workday tenants. Update as needed when testing
# against new portals.
_SEL = {
    # Job page
    "apply_btn":        '[data-automation-id="jobPostingApplyButton"], [data-automation-id="adventureButton"]',
    "continue_btn":     '[data-automation-id="continueButton"]',
    "autofill_resume":  '[data-automation-id="autofillWithResume"]',

    # Candidate Home (for deleting existing applications)
    "candidate_home":   '[data-automation-id="navigationItem-Candidate Home"]',
    "action_menu":      '[data-automation-id="actionMenuTarget"]',
    "delete_app":       '[data-automation-id="deleteApplication"]',

    # Auth
    "header_sign_in":   '[data-automation-id="utilityButtonSignIn"]',
    "auth_form":        '[data-automation-id="signInContent"]',
    "auth_email":       '[data-automation-id="email"]',
    "auth_password":    '[data-automation-id="password"]',
    "sign_in_submit":   '[data-automation-id="click_filter"]',

    # Navigation
    "next_btn":         '[data-automation-id="bottom-navigation-next-button"], [data-automation-id="pageFooterNextButton"]',

    # Page types
    "info_page":        '[data-automation-id="applyFlowMyInfoPage"]',
    "exp_page":         '[data-automation-id="applyFlowMyExpPage"]',

    # Resume upload
    "resume_page":      '[data-automation-id="resumeUpload"]',
    "resume_upload":    'input[data-automation-id="file-upload-input-ref"]',
    "resume_uploaded":  '[data-automation-id="file-upload-item"]',

    # Experience page fields
    "exp_job_title":        '[data-automation-id="formField-jobTitle"]',
    "exp_school":           '[data-automation-id="formField-school"]',
    "exp_degree":           '[data-automation-id="formField-degree"]',
    "exp_currently_here":   '[data-automation-id="formField-currentlyWorkHere"] input[type="checkbox"]',
    "exp_role_desc":        '[data-automation-id="formField-roleDescription"] textarea',
    "exp_skills":           '[data-automation-id="formField-skills"]',
    "exp_language":         '[data-automation-id="formField-language"]',
    "exp_native":           '[data-automation-id="formField-native"] input[type="checkbox"]',
    "lang_panels":          '[role="group"][aria-labelledby*="Languages-"]',
    "form_field_all":       '[data-automation-id^="formField-"]',

    # Common widget selectors
    "multiselect":          '[data-automation-id="multiSelectContainer"]',
    "selected_item":        '[data-automation-id="selectedItem"]',
    "active_list":          '[data-automation-id="activeListContainer"]',
    "menu_item":            '[data-automation-id="menuItem"]',
    "prompt_option":        '[data-automation-id="promptOption"]',
    "back_button":          '[data-automation-id="backButton"]',
    "add_button":           '[data-automation-id="add-button"]',
    "confirm_button":       '[data-automation-id="confirmButton"], button:has-text("Delete")',
    "date_month":           '[data-automation-id="dateSectionMonth-input"]',
    "date_year":            '[data-automation-id="dateSectionYear-input"]',
    "dropdown_btn":         'button[aria-haspopup="listbox"]',
    "dropdown_listbox":     '[data-popper-placement] [role="listbox"]',
    "dropdown_option":      '[role="option"]',
    "dropdown_real_option": '[data-popper-placement] [role="option"]:not([aria-disabled])',

    # Application Questions page
    "questions_page":       '[data-automation-id="applyFlowPrimaryQuestionsPage"]',
    "legend_richtext":      'legend [data-automation-id="richText"]',
}

def _parse_job_url(url: str) -> tuple[str, str]:
    """Parse a full Workday job page URL into (api_base, external_path).

    Example:
        Input:  https://wf.wd1.myworkdayjobs.com/WellsFargoJobs/job/Bengaluru-India/Finance_R-123
        Output: ("https://wf.wd1.myworkdayjobs.com/wday/cxs/wf/WellsFargoJobs",
                 "/job/Bengaluru-India/Finance_R-123")
    """
    parsed = urlparse(url)
    company = parsed.hostname.split(".")[0]
    path_parts = [p for p in parsed.path.strip("/").split("/") if p]
    # Filter out locale segments like "en-US"
    path_parts = [p for p in path_parts if not re.match(r"^[a-z]{2}-[A-Z]{2}$", p)]
    if "job" in path_parts:
        job_idx = path_parts.index("job")
        site = path_parts[job_idx - 1] if job_idx > 0 else path_parts[0]
        external_path = "/" + "/".join(path_parts[job_idx:])
    else:
        site = path_parts[0] if path_parts else "External"
        external_path = ""
    api_base = f"{parsed.scheme}://{parsed.hostname}/wday/cxs/{company}/{site}"
    return api_base, external_path


_SYSTEM_ROLE = (
    "You are a job application assistant. Answer application questions "
    "truthfully and concisely based on the provided candidate profile."
)


_SESSIONS_DIR = Path(__file__).parent.parent / "data" / "workday_sessions"
_CREDS_PATH = Path(__file__).parent.parent / "config" / "career_sites_credentials.yaml"
_WORKDAY_ANSWERS_PATH = Path(__file__).parent.parent / "config" / "workday_answers.yaml"


def _load_credentials() -> dict:
    """Load per-tenant credentials from career_sites_credentials.yaml."""
    if _CREDS_PATH.exists():
        with open(_CREDS_PATH) as f:
            return yaml.safe_load(f) or {}
    return {}


def _load_workday_answers() -> dict:
    """Load field mappings from workday_answers.yaml."""
    if _WORKDAY_ANSWERS_PATH.exists():
        with open(_WORKDAY_ANSWERS_PATH) as f:
            return yaml.safe_load(f) or {}
    return {}


def _get_tenant(url: str) -> str:
    """Extract tenant name from a Workday job URL (first subdomain segment)."""
    return urlparse(url).hostname.split(".")[0]


class WorkdayApplyError(Exception):
    """Raised when a Workday application step fails (non-retryable)."""


class WorkdayApplicant:
    """Automates Workday job application form submission."""

    def __init__(self, config: dict, answers: dict, answers_raw: str = ""):
        if not answers_raw:
            raise WorkdayApplyError("answers_raw is required — raw answers.yaml content must be provided")
        self._apply_cfg = config.get("apply", {})
        self._genai_cfg = config.get("genai", {})
        self._profiles = config.get("profiles", [])
        self._answers = answers
        self._answers_raw = answers_raw
        self._personal = answers.get("personal", {})
        self._work_exp = answers.get("work_experience", [])
        self._education = answers.get("education", [])
        self._credentials = _load_credentials()
        self._workday_fields = _load_workday_answers()
        if "education" not in self._workday_fields:
            raise WorkdayApplyError("'education' section missing from workday_answers.yaml")
        self._confidence_threshold = self._apply_cfg.get("answer_confidence_threshold", 7)
        self._apply_email = self._personal.get("email", "")

        self._pw = None
        self._browser = None
        self._context = None
        self._page: Page | None = None
        self._genai: GenAIClient | None = None
        self._current_tenant: str | None = None
        self._http = httpx.Client(
            timeout=15,
            verify=False,
            headers={
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            },
        )

    # ── Lifecycle ──────────────────────────────────────────────

    def start(self):
        """Launch the Playwright browser."""
        headless = self._apply_cfg.get("headless", False)
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(headless=headless)
        self._genai = GenAIClient(self._genai_cfg)
        logger.info("Browser launched (headless=%s)", headless)

    def _get_session_path(self, tenant: str) -> Path:
        """Return the session file path for a tenant."""
        return _SESSIONS_DIR / f"{tenant}.json"

    def _load_context(self, tenant: str):
        """Create a browser context, loading saved session if available."""
        session_path = self._get_session_path(tenant)
        if session_path.exists():
            logger.info("Loading saved session for tenant: %s", tenant)
            self._context = self._browser.new_context(storage_state=str(session_path))
        else:
            self._context = self._browser.new_context()
        self._page = self._context.new_page()
        self._current_tenant = tenant

    def _save_session(self):
        """Save the current browser context session to disk."""
        if not self._current_tenant or not self._context:
            return
        _SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
        session_path = self._get_session_path(self._current_tenant)
        self._context.storage_state(path=str(session_path))
        logger.info("Session saved for tenant: %s", self._current_tenant)

    def close(self):
        """Shut down browser and GenAI client."""
        if self._genai:
            self._genai.close()
        if self._browser:
            self._browser.close()
        if self._pw:
            self._pw.stop()
        if self._http:
            self._http.close()
        logger.info("Browser closed")

    def is_job_valid(self, job_url: str) -> bool:
        """Check if a Workday job posting is still active via the JSON API.

        Workday returns 200 with jobPostingInfo for active jobs, and 403 for
        expired/removed postings.

        Args:
            job_url: Full Workday job page URL.

        Returns:
            True if the job is still active, False if expired/removed/unreachable.
        """
        api_base, external_path = _parse_job_url(job_url)
        if not external_path:
            logger.warning("Could not parse external_path from URL: %s", job_url)
            return False
        try:
            resp = self._http.get(f"{api_base}{external_path}", follow_redirects=False)
            if resp.status_code in (403, 404):
                logger.info("Job expired (HTTP %d): %s", resp.status_code, job_url)
                return False
            if resp.status_code != 200:
                # Redirects (303), server errors (5xx), etc. — not definitively expired
                logger.warning("Job check inconclusive (HTTP %d): %s — assuming valid", resp.status_code, job_url)
                return True
            data = resp.json()
            return "jobPostingInfo" in data
        except (httpx.HTTPError, ValueError) as e:
            logger.warning("Job validity check failed for %s: %s — assuming valid", job_url, e)
            return True

    # ── Main orchestration ─────────────────────────────────────

    def apply(self, job: dict, pdf_path: Path, resume_data: dict) -> bool:
        """Apply to a single Workday job. Returns True on success.

        Args:
            job:         Dict from DB with keys: job_id, url, title, company,
                         job_description, resume_name.
            pdf_path:    Path to the resume PDF to upload.
            resume_data: Parsed resume data (work_experience, education) from resume_parser.

        Raises:
            WorkdayApplyError: On any application step failure.
        """
        if not job.get("job_description"):
            raise WorkdayApplyError(
                f"Job description is missing for '{job.get('title', '')}' — cannot apply without it"
            )

        job_url = job["url"]
        tenant = _get_tenant(job_url)

        logger.info("Applying to: %s at %s (%s)", job["title"], job["company"], job_url)

        # Load context with saved session for this tenant
        if self._current_tenant != tenant:
            if self._context:
                self._context.close()
            self._load_context(tenant)

        try:
            # Step 1: Navigate to job page and ensure signed in
            self._page.goto(job_url, wait_until="networkidle", timeout=_TIMEOUT)
            self._ensure_signed_in()

            # Step 2: Click Apply
            self._click_apply_button()

            # Step 3: Fill the multi-step application form
            self._fill_application(job, pdf_path, resume_data)

            logger.info("Successfully applied to: %s at %s", job["title"], job["company"])
            return True

        except PlaywrightTimeout as e:
            raise WorkdayApplyError(f"Timeout waiting for element: {e}") from e

    # ── Pre-auth: Sign in from job page header ──────────────────

    def _ensure_signed_in(self):
        """Sign in from the job page header if not already authenticated."""
        if not self._page.query_selector(_SEL["header_sign_in"]):
            logger.info("Already signed in")
            return

        logger.info("Not signed in — signing in from header")
        self._page.click(_SEL["header_sign_in"])
        self._page.wait_for_selector(_SEL["auth_form"], timeout=_TIMEOUT)

        creds = self._credentials.get(self._current_tenant)
        if creds:
            self._auto_sign_in(creds)
        else:
            logger.info("No credentials for tenant: %s — manual sign-in required", self._current_tenant)
            input("Press Enter after you have signed in manually...")

        self._page.reload(wait_until="networkidle")
        self._save_session()

    # ── Step 1: Click Apply ────────────────────────────────────

    def _click_apply_button(self):
        """Find and click the Apply button, then select 'Autofill with Resume'.

        If a previous unsubmitted application exists (Continue Application),
        delete it first, then apply fresh.
        """
        # Wait for either Apply or Continue button
        self._page.wait_for_selector(
            f'{_SEL["apply_btn"]}, {_SEL["continue_btn"]}', timeout=_TIMEOUT
        )

        # If "Continue Application" exists, delete existing application first
        if self._page.query_selector(_SEL["continue_btn"]):
            logger.info("Existing application found — deleting before re-applying")
            self._delete_existing_application()
            # Go back to job page and reload to get fresh state
            self._page.go_back()
            self._page.wait_for_load_state("networkidle", timeout=_TIMEOUT)
            self._page.reload(wait_until="networkidle")
            self._page.wait_for_selector(_SEL["apply_btn"], timeout=_TIMEOUT)

        self._page.query_selector(_SEL["apply_btn"]).click()

        # Handle "Start Your Application" modal
        autofill_btn = self._page.wait_for_selector(_SEL["autofill_resume"], timeout=_TIMEOUT)
        logger.info("Application modal detected — choosing 'Autofill with Resume'")
        autofill_btn.click()
        self._page.wait_for_load_state("domcontentloaded")

    def _delete_existing_application(self):
        """Navigate to Candidate Home and delete the existing application."""
        logger.info("Navigating to Candidate Home")
        self._page.click(_SEL["candidate_home"])
        self._page.wait_for_load_state("networkidle", timeout=_TIMEOUT)

        # Click action menu (three dots) on the first application row
        self._page.wait_for_selector(_SEL["action_menu"], timeout=_TIMEOUT)
        self._page.click(_SEL["action_menu"])

        # Click "Delete Application"
        self._page.wait_for_selector(_SEL["delete_app"], timeout=_TIMEOUT)
        self._page.click(_SEL["delete_app"])

        # Handle confirmation dialog if any
        confirm_btn = self._page.query_selector(_SEL["confirm_button"])
        if confirm_btn:
            confirm_btn.click()

        self._page.wait_for_load_state("networkidle", timeout=_TIMEOUT)
        logger.info("Existing application deleted")

    def _auto_sign_in(self, creds: dict):
        """Automatically sign in using stored credentials."""
        logger.info("Auto sign-in for tenant: %s", self._current_tenant)

        # Fill email and password (click to focus, then type for reliability)
        self._page.click(_SEL["auth_email"])
        self._page.fill(_SEL["auth_email"], "")
        self._page.type(_SEL["auth_email"], creds["email"])
        self._page.click(_SEL["auth_password"])
        self._page.fill(_SEL["auth_password"], "")
        self._page.type(_SEL["auth_password"], creds["password"])


        # Submit — click and retry until auth form disappears
        while self._page.query_selector(_SEL["auth_form"]):
            time.sleep(1)
            self._page.click(_SEL["sign_in_submit"])
            try:
                self._page.wait_for_selector(_SEL["auth_form"], state="hidden", timeout=5000)
                break
            except PlaywrightTimeout:
                continue  # click didn't register, retry
        self._page.wait_for_load_state("networkidle", timeout=_TIMEOUT)

    # ── Step 3: Multi-step form filling ────────────────────────

    def _click_next(self):
        """Click the Save and Continue button and wait for next page to load."""
        btn = self._page.wait_for_selector(_SEL["next_btn"], timeout=_TIMEOUT)
        btn.click()
        self._page.wait_for_load_state("networkidle")
        # Workday SPA may auto-advance past pages (e.g. My Experience after autofill);
        # allow time for any auto-transitions to settle before re-detecting page type.
        time.sleep(10)

    def _is_submit_page(self) -> bool:
        """Check if the current page has a Submit button."""
        btn = self._page.query_selector(_SEL["next_btn"])
        if btn:
            text = btn.inner_text().strip().lower()
            return "submit" in text
        return False

    def _fill_application(self, job: dict, pdf_path: Path, resume_data: dict):
        """Walk through the Workday application form dynamically."""

        # Step 1: Resume upload — wait 30s after upload for autofill to populate skills etc.
        self._page.wait_for_selector(_SEL["resume_page"], timeout=_TIMEOUT)
        self._upload_resume(pdf_path)
        time.sleep(5)
        self._click_next()

        # Steps 2+: Fill pages until Submit
        while True:
            self._page.wait_for_load_state("networkidle", timeout=_TIMEOUT)
            self._page.wait_for_selector(_SEL["next_btn"], timeout=_TIMEOUT)

            if self._is_submit_page():
                self._page.query_selector(_SEL["next_btn"]).click()
                self._page.wait_for_load_state("networkidle")
                logger.info("Application submitted")
                return

            # Route to correct page handler
            if self._page.query_selector(_SEL["info_page"]):
                self._fill_form_page(job)
            elif self._page.query_selector(_SEL["exp_page"]):
                self._fill_experience_page(resume_data)
            else:
                self._fill_form_page(job)
            self._click_next()

    def _upload_resume(self, pdf_path: Path):
        """Upload the resume PDF file."""
        logger.info("Uploading resume: %s", pdf_path.name)
        self._page.set_input_files(_SEL["resume_upload"], str(pdf_path))
        self._page.wait_for_selector(_SEL["resume_uploaded"], timeout=_TIMEOUT)

    # ── My Experience Page ──────────────────────────────────────

    def _clear_section(self, section_id: str):
        """Delete all existing entries in a section (e.g. autofill leftovers)."""
        section = self._page.query_selector(f'[aria-labelledby="{section_id}-section"]')
        if not section:
            return
        while True:
            delete_btn = section.query_selector("button:has(svg.wd-icon-trash)")
            if not delete_btn:
                break
            delete_btn.click()
            self._page.wait_for_load_state("networkidle", timeout=_TIMEOUT)

    def _fill_experience_page(self, resume_data: dict):
        """Fill the My Experience page.

        Work experience comes from resume_data (dynamic per job).
        Education comes from workday_answers.yaml (static).
        """
        logger.info("Filling My Experience page")

        # Clear autofill entries (keep skills and resume untouched)
        self._clear_section("Work-Experience")
        self._clear_section("Education")
        self._clear_section("Languages")

        work_exp = resume_data.get("work_experience", [])

        # Fill work experience entries
        for i, exp in enumerate(work_exp):
            logger.info("  Adding work experience %d: %s at %s", i + 1, exp.get("jobTitle", ""), exp.get("companyName", ""))
            self._click_section_add("Work-Experience")
            self._page.wait_for_selector(_SEL["exp_job_title"], timeout=_TIMEOUT)
            self._fill_work_experience_entry(exp)

        # Fill education (from workday_answers.yaml)
        edu = self._workday_fields["education"]
        logger.info("  Adding education: %s from %s", edu.get("degree", ""), edu.get("school", ""))
        self._click_section_add("Education")
        self._page.wait_for_selector(_SEL["exp_school"], timeout=_TIMEOUT)
        self._fill_education_entry(edu)

        # Fill languages (from workday_answers.yaml)
        languages = self._workday_fields.get("languages", [])
        for i, lang in enumerate(languages):
            logger.info("  Adding language %d: %s", i + 1, lang.get("language", ""))
            self._click_section_add("Languages")
            self._page.wait_for_selector(_SEL["exp_language"], timeout=_TIMEOUT)
            self._fill_language_entry(lang)

        # Verify skills and resume are populated (from autofill)
        skills_container = self._page.query_selector(_SEL["exp_skills"])
        if skills_container:
            selected_skills = skills_container.query_selector(_SEL["selected_item"])
            if not selected_skills:
                raise WorkdayApplyError("Skills section is empty — autofill did not populate skills")
        resume_uploaded = self._page.query_selector(_SEL["resume_uploaded"])
        if not resume_uploaded:
            raise WorkdayApplyError("Resume not found on experience page — upload may have failed")

    def _click_section_add(self, section_id: str):
        """Click the 'Add' button within a section group."""
        section = self._page.query_selector(f'[aria-labelledby="{section_id}-section"]')
        if not section:
            raise WorkdayApplyError(f"Section not found: '{section_id}'")
        add_btn = section.query_selector(_SEL["add_button"])
        if not add_btn:
            raise WorkdayApplyError(f"Add button not found in section: '{section_id}'")
        add_btn.click()
        self._page.wait_for_load_state("networkidle", timeout=_TIMEOUT)

    def _fill_work_experience_entry(self, exp: dict):
        """Fill a single work experience entry after clicking Add."""
        # Text fields
        self._fill_experience_field("jobTitle", exp.get("jobTitle", ""))
        self._fill_experience_field("companyName", exp.get("companyName", ""))
        if exp.get("location"):
            self._fill_experience_field("location", exp["location"])

        # Checkbox: currently work here
        if exp.get("currentlyWorkHere"):
            checkbox = self._page.query_selector(_SEL["exp_currently_here"])
            if checkbox and checkbox.get_attribute("aria-checked") != "true":
                checkbox.click()

        # Date fields
        self._fill_date_field("startDate", exp.get("startDate", ""))
        if not exp.get("currentlyWorkHere") and exp.get("endDate", "").lower() != "present":
            self._fill_date_field("endDate", exp.get("endDate", ""))

        # Role description (optional)
        if exp.get("roleDescription"):
            desc = self._page.query_selector(_SEL["exp_role_desc"])
            if desc:
                desc.fill(exp["roleDescription"])

    def _fill_education_entry(self, edu: dict):
        """Fill a single education entry after clicking Add.

        Fields: school (searchable multiselect), degree (dropdown),
        fieldOfStudy (searchable multiselect), dates.
        """
        # School — searchable multiselect, try each name in priority list
        if edu.get("school"):
            self._select_from_searchable("school", edu["school"])

        # Degree — custom dropdown
        if edu.get("degree"):
            container = self._page.query_selector(_SEL["exp_degree"])
            if container:
                dropdown_btn = container.query_selector(_SEL["dropdown_btn"])
                if dropdown_btn:
                    field = {
                        "field_name": "degree",
                        "element": dropdown_btn,
                    }
                    self._select_from_dropdown(field, edu["degree"])

        # Field of Study — searchable multiselect
        if edu.get("fieldOfStudy"):
            self._select_from_searchable("fieldOfStudy", edu["fieldOfStudy"])

        # GPA
        if edu.get("gradeAverage"):
            self._fill_experience_field("gradeAverage", edu["gradeAverage"])

        # Date fields
        if edu.get("startDate"):
            self._fill_date_field("startDate", edu["startDate"])
        if edu.get("endDate"):
            self._fill_date_field("endDate", edu["endDate"])

    def _fill_language_entry(self, lang: dict):
        """Fill a single language entry after clicking Add."""
        # Find the last language panel (the one just added)
        panels = self._page.query_selector_all(_SEL["lang_panels"])
        if not panels:
            raise WorkdayApplyError("No language panel found after clicking Add")
        panel = panels[-1]

        # Language — dropdown (scoped to panel)
        if lang.get("language"):
            container = panel.query_selector(_SEL["exp_language"])
            if container:
                dropdown_btn = container.query_selector(_SEL["dropdown_btn"])
                if dropdown_btn:
                    field = {"field_name": "language", "element": dropdown_btn}
                    self._select_from_dropdown(field, lang["language"])

        # Native checkbox (scoped to panel)
        if lang.get("native"):
            checkbox = panel.query_selector(_SEL["exp_native"])
            if checkbox and checkbox.get_attribute("aria-checked") != "true":
                checkbox.click()

        # Reading/Speaking/Writing — find dropdowns by label within panel
        for proficiency in ["Reading", "Speaking", "Writing"]:
            value = lang.get(proficiency.lower())
            if not value:
                continue
            containers = panel.query_selector_all(_SEL["form_field_all"])
            for c in containers:
                label_el = c.query_selector("label")
                if label_el and label_el.inner_text().strip().rstrip("*").strip() == proficiency:
                    btn = c.query_selector(_SEL["dropdown_btn"])
                    if btn:
                        field = {"field_name": proficiency.lower(), "element": btn}
                        self._select_from_dropdown(field, value, substring_match=True)
                    break

    def _select_from_searchable(self, field, value):
        """Select a value from a type-to-search multiselect (server-filtered results).

        Used for fields with a text input that filters/fetches options as you type.
        Selection creates a "pill" chip in the input. The full option list is never
        visible at once — results are fetched server-side based on typed text.

        Examples:
            - School: type "JNTUA" → Enter auto-selects → pill "JNTUA College of Engineering"
            - Field of Study: type "Electronics" → dropdown appears → click exact match → pill
            - Country Phone Code: type "+91" → filtered list → Enter/click → pill "+91 India"
            - Source: type "Company" → shows "Company career website" → select → pill appears

        Args:
            field: Either a field_name string (e.g. "school") to look up by
                   data-automation-id="formField-{name}", or a field dict with
                   'element' (multiSelectContainer), 'container', and 'field_name'.
            value: String or list of strings (priority order, first match wins).
        """
        # Resolve container and field_name from either a string or field dict
        if isinstance(field, str):
            field_name = field
            container = self._page.query_selector(f'[data-automation-id="formField-{field_name}"]')
            if not container:
                raise WorkdayApplyError(f"Searchable field not found: '{field_name}'")
            multiselect = container.query_selector(_SEL["multiselect"])
        else:
            field_name = field["field_name"]
            container = field["container"]
            multiselect = field["element"]

        if not multiselect:
            raise WorkdayApplyError(f"No multiselect in field: '{field_name}'")

        search_input = multiselect.query_selector("input")
        if not search_input:
            raise WorkdayApplyError(f"No search input in field: '{field_name}'")

        candidates = value if isinstance(value, list) else [value]

        for candidate in candidates:
            # Type candidate and press Enter to search/select
            search_input.click(force=True)
            search_input.fill("")
            search_input.type(candidate)
            search_input.press("Enter")

            # Check if Enter auto-selected (pill appeared)
            try:
                container.wait_for_selector(_SEL["selected_item"], timeout=3000)
                logger.info("  Searchable '%s': selected with '%s'", field_name, candidate)
                return
            except PlaywrightTimeout:
                pass  # Enter didn't auto-select — try clicking from dropdown

            # Fallback: look for filtered dropdown and click matching item
            dropdown = self._page.query_selector(_SEL["active_list"])
            if not dropdown:
                search_input.fill("")
                logger.info("  Searchable '%s': no dropdown for '%s'", field_name, candidate)
                continue

            menu_items = dropdown.query_selector_all(_SEL["menu_item"])
            for item in menu_items:
                label_el = item.query_selector(_SEL["prompt_option"])
                if label_el:
                    label = label_el.get_attribute("data-automation-label") or label_el.inner_text().strip()
                    if self._normalize_text(candidate) == self._normalize_text(label):
                        item.click()
                        logger.info("  Searchable '%s': selected '%s'", field_name, label)

                        # Check if a sub-menu appeared (back button = category expanded)
                        try:
                            self._page.wait_for_selector(_SEL["back_button"], timeout=3000)
                            sub_dropdown = self._page.query_selector(_SEL["active_list"])
                            if sub_dropdown:
                                sub_items = sub_dropdown.query_selector_all(_SEL["menu_item"])
                                for sub_item in sub_items:
                                    sub_label_el = sub_item.query_selector(_SEL["prompt_option"])
                                    if sub_label_el:
                                        sub_label = sub_label_el.get_attribute("data-automation-label") or sub_label_el.inner_text().strip()
                                        if self._normalize_text(candidate) == self._normalize_text(sub_label):
                                            sub_item.click()
                                            logger.info("  Searchable '%s': selected leaf '%s'", field_name, sub_label)
                                            return
                                if sub_items:
                                    sub_items[0].click()
                                    logger.info("  Searchable '%s': selected first sub-item", field_name)
                        except PlaywrightTimeout:
                            pass  # No sub-menu — selection was made directly

                        return

            # No match in results — clear and try next candidate
            search_input.fill("")
            logger.info("  Searchable '%s': no match for '%s'", field_name, candidate)

        raise WorkdayApplyError(
            f"No matching option for searchable field '{field_name}'. Tried: {candidates}"
        )

    def _fill_experience_field(self, field_name: str, value: str):
        """Fill a text field by its formField automation ID on the experience page."""
        container = self._page.query_selector(f'[data-automation-id="formField-{field_name}"]')
        if not container:
            return
        text_input = container.query_selector("input[type='text']")
        if text_input:
            text_input.fill(value)

    def _fill_date_field(self, field_name: str, value: str):
        """Fill a date field (MM/YYYY format) by its formField automation ID."""
        if not value or value.lower() == "present":
            return
        container = self._page.query_selector(f'[data-automation-id="formField-{field_name}"]')
        if not container:
            return
        parts = value.split("/")
        if len(parts) != 2:
            return
        month, year = parts
        month_input = container.query_selector(_SEL["date_month"])
        year_input = container.query_selector(_SEL["date_year"])
        if month_input:
            month_input.fill(month)
        if year_input:
            year_input.fill(year)

    # ── Generic Page Filler ────────────────────────────────────

    def _get_dropdown_options(self, field: dict) -> list[str]:
        """Open a dropdown, read available options, close it, return option texts."""
        btn = field["element"]
        btn.click()
        try:
            listbox = self._page.wait_for_selector(_SEL["dropdown_listbox"], timeout=5000)
            self._page.wait_for_selector(_SEL["dropdown_real_option"], timeout=5000)
            options = listbox.query_selector_all(_SEL["dropdown_option"])
            texts = [opt.inner_text().strip() for opt in options if opt.inner_text().strip()]
        except PlaywrightTimeout:
            texts = []
        # Close dropdown by clicking button again (toggle) and wait for it to disappear
        btn.click()
        self._page.wait_for_selector(_SEL["dropdown_listbox"], state="hidden", timeout=3000)
        return texts

    def _fill_form_page(self, job: dict):
        """Detect and fill all form fields on the current page."""
        fields = self._scan_page_fields()
        logger.info("Page has %d fillable field(s)", len(fields))

        ai_fields = []  # Fields that need LLM answers

        for field in fields:
            name = field["field_name"]
            # Known field → fill directly from workday_fields
            if name in self._workday_fields:
                value = self._workday_fields[name]
                logger.info("  Filling '%s' → '%s'", name, str(value)[:30])
                try:
                    self._fill_field_by_type(field, value)
                except WorkdayApplyError as e:
                    if field["required"]:
                        raise
                    logger.warning("  Skipping optional field '%s': %s", name, e)
                continue

            # Skip optional fields we don't have data for
            if not field["required"]:
                logger.debug("  Skipping optional: '%s'", name)
                continue

            # Guard: fail if label is blank or too short (bad parse)
            if not field["label"] or len(field["label"].strip()) < 5:
                raise WorkdayApplyError(
                    f"Cannot answer field '{name}': question text is blank or too short to interpret"
                )

            # For dropdowns, pre-fetch available options so LLM knows valid choices
            options = field.get("options", [])
            if field["input_type"] == "dropdown" and not options:
                options = self._get_dropdown_options(field)
                field["options"] = options

            ai_fields.append(field)

        # Batch AI answering (up to 10 per call)
        if ai_fields:
            logger.info("  Sending %d question(s) to AI in batch", len(ai_fields))
            answers = self._ask_llm_batch(ai_fields, job)
            for field, answer in zip(ai_fields, answers):
                # Re-query element from DOM to avoid stale references after scan re-renders
                container = self._page.query_selector(
                    f'[data-automation-id="formField-{field["field_name"]}"]'
                )
                if not container:
                    raise WorkdayApplyError(f"Field '{field['field_name']}' not found on page")
                refreshed = self._classify_field(container, field["field_name"])
                if not refreshed:
                    raise WorkdayApplyError(f"Field '{field['field_name']}' could not be re-classified")
                self._fill_field_by_type(refreshed, answer)

    def _scan_page_fields(self) -> list[dict]:
        """Scan the current page for all fillable form fields.

        Workday wraps each field in a div with data-automation-id="formField-{fieldName}".
        Inside, the actual input can be:
          - <input type="text"> (text fields)
          - <textarea> (text areas)
          - <button aria-haspopup="listbox"> (custom dropdowns)
          - <input type="radio"> (radio groups)
          - div[data-automation-id="multiSelectContainer"] (multiselects)

        Returns list of field descriptors:
            {field_name, label, input_type, required, container, element, options}
        """
        fields = []
        containers = self._page.query_selector_all('[data-automation-id^="formField-"]')

        for container in containers:
            aid = container.get_attribute("data-automation-id") or ""
            field_name = aid.replace("formField-", "", 1)

            # Detect field type and check if already filled
            field = self._classify_field(container, field_name)
            if field:
                fields.append(field)

        return fields

    def _classify_field(self, container, field_name: str) -> dict | None:
        """Classify a formField container and return a field descriptor."""

        # Check for radio buttons
        radios = container.query_selector_all("input[type='radio']")
        if radios:
            options = []
            for r in radios:
                r_id = r.get_attribute("id") or ""
                label_el = container.query_selector(f'label[for="{r_id}"]')
                if label_el:
                    options.append(label_el.inner_text().strip())
            return {
                "field_name": field_name,
                "label": self._get_container_label(container),
                "input_type": "radio",
                "required": self._container_is_required(container),
                "container": container,
                "element": radios[0],
                "options": options,
            }

        # Check for multiselect widget
        multiselect = container.query_selector(_SEL["multiselect"])
        if multiselect:
            return {
                "field_name": field_name,
                "label": self._get_container_label(container),
                "input_type": "multiselect",
                "required": self._container_is_required(container),
                "container": container,
                "element": multiselect,
                "options": [],
            }

        # Check for custom dropdown (button with aria-haspopup="listbox")
        dropdown_btn = container.query_selector(_SEL["dropdown_btn"])
        if dropdown_btn:
            return {
                "field_name": field_name,
                "label": self._get_container_label(container),
                "input_type": "dropdown",
                "required": self._container_is_required(container),
                "container": container,
                "element": dropdown_btn,
                "options": [],
            }

        # Check for textarea
        textarea = container.query_selector("textarea")
        if textarea:
            return {
                "field_name": field_name,
                "label": self._get_container_label(container),
                "input_type": "textarea",
                "required": self._container_is_required(container),
                "container": container,
                "element": textarea,
                "options": [],
            }

        # Check for text input
        text_input = container.query_selector("input[type='text']")
        if text_input:
            return {
                "field_name": field_name,
                "label": self._get_container_label(container),
                "input_type": "text",
                "required": self._container_is_required(container),
                "container": container,
                "element": text_input,
                "options": [],
            }

        return None

    def _get_container_label(self, container) -> str:
        """Extract the label text from a formField container."""
        label = container.query_selector("label")
        if label:
            return label.inner_text().strip().rstrip("*").strip()
        # Application Questions page: text is inside legend > div[data-automation-id="richText"]
        rich_text = container.query_selector(_SEL["legend_richtext"])
        if rich_text:
            return rich_text.inner_text().strip().rstrip("*").strip()
        legend = container.query_selector("legend")
        if legend:
            return legend.inner_text().strip().rstrip("*").strip()
        return ""

    def _container_is_required(self, container) -> bool:
        """Check if a formField container has a required field."""
        # Check for aria-required on inputs inside
        required_el = container.query_selector("[aria-required='true']")
        if required_el:
            return True
        # Check for required abbreviation marker (*)
        abbr = container.query_selector("abbr")
        if abbr:
            return True
        return False

    def _fill_field_by_type(self, field: dict, value):
        """Fill a field based on its input type."""
        input_type = field["input_type"]

        if input_type == "text" or input_type == "textarea":
            # Text fields always get a string
            fill_value = value[0] if isinstance(value, list) else value
            field["element"].fill(fill_value)

        elif input_type == "dropdown":
            self._select_from_dropdown(field, value)

        elif input_type == "multiselect":
            self._select_from_searchable(field, value)

        elif input_type == "radio":
            fill_value = value[0] if isinstance(value, list) else value
            self._fill_radio(field, fill_value)

    @staticmethod
    def _normalize_text(text: str) -> str:
        """Strip diacriticals and lowercase for comparison."""
        nfkd = unicodedata.normalize("NFKD", text)
        return "".join(c for c in nfkd if not unicodedata.combining(c)).lower().strip()

    def _select_from_dropdown(self, field: dict, value, substring_match: bool = False):
        """Select a value from a click-to-open dropdown (all options pre-loaded).

        Used for fields that show a button (e.g. "Select One") which, when clicked,
        opens a popup listbox with all available options visible at once. No typing/search.

        Examples:
            - Degree: click → listbox shows "Bachelor of Technology", "Master's", etc.
            - Language: click → listbox shows "English", "Hindi", "Telugu", etc.
            - Proficiency (Reading/Speaking/Writing): click → "1 - Beginner", "3 - Fluent", etc.

        Args:
            field: Field descriptor with 'element' (the dropdown button) and 'field_name'.
            value: String or list of strings (priority order, first match wins).
            substring_match: If True, also matches candidates as substrings of option text.
                             E.g. "Fluent" matches "3 - Fluent". Default False.
        """
        btn = field["element"]
        btn.click()
        # Wait for dropdown popup listbox and real options to load
        listbox = self._page.wait_for_selector(_SEL["dropdown_listbox"], timeout=_TIMEOUT)
        self._page.wait_for_selector(_SEL["dropdown_real_option"], timeout=_TIMEOUT)
        # Collect available options
        options = listbox.query_selector_all(_SEL["dropdown_option"])
        available = []
        option_map = {}
        for opt in options:
            opt_text = opt.inner_text().strip()
            available.append(opt_text)
            option_map[self._normalize_text(opt_text)] = opt

        # Normalize value to priority list
        candidates = value if isinstance(value, list) else [value]

        # Try each candidate in priority order
        for candidate in candidates:
            norm_candidate = self._normalize_text(candidate)
            # Exact match
            if norm_candidate in option_map:
                option_map[norm_candidate].click()
                self._page.wait_for_selector(
                    _SEL["dropdown_listbox"], state="hidden", timeout=_TIMEOUT
                )
                return
            # Substring match
            if substring_match:
                for opt_key, opt_el in option_map.items():
                    if norm_candidate in opt_key:
                        opt_el.click()
                        self._page.wait_for_selector(
                            _SEL["dropdown_listbox"], state="hidden", timeout=_TIMEOUT
                        )
                        return

        raise WorkdayApplyError(
            f"No matching option for dropdown '{field['field_name']}': {candidates}. Available: {available}"
        )


    def _fill_radio(self, field: dict, value: str):
        """Fill a radio button group by clicking the exact matching option."""
        container = field["container"]
        radios = container.query_selector_all("input[type='radio']")
        available = []
        for radio in radios:
            r_id = radio.get_attribute("id") or ""
            label_el = container.query_selector(f'label[for="{r_id}"]')
            if label_el:
                label_text = label_el.inner_text().strip()
                available.append(label_text)
                if label_text.lower() == value.lower():
                    radio.click()
                    return
        raise WorkdayApplyError(
            f"No exact radio match for '{field['field_name']}': '{value}'. Available: {available}"
        )


    def _ask_llm_batch(self, fields: list[dict], job: dict) -> list[str]:
        """Ask the LLM to answer multiple screening questions in one call.

        Sends up to 10 questions per batch. Returns answers in the same order.
        """
        profile_text = self._build_profile_text()
        answers_content = self._answers_raw
        all_answers = []

        for i in range(0, len(fields), 10):
            batch = fields[i:i + 10]
            questions_block = ""
            for idx, field in enumerate(batch, 1):
                options_text = ""
                if field.get("options"):
                    options_text = f"  Options: {json.dumps(field['options'])}"
                questions_block += (
                    f"\n{idx}. Question: \"{field['label']}\"\n"
                    f"   Input type: {field['input_type']}\n"
                    f"{options_text}\n"
                )

            prompt = f"""You are filling out a job application for the following position:
  Title: {job.get('title', '')}
  Company: {job.get('company', '')}
  Description: {job['job_description']}

The applicant's profile:
{profile_text}

The applicant's answers reference (use this to inform your responses):
{answers_content}

Answer ALL of the following application questions concisely and truthfully.
Return a JSON array with one object per question, in order. Each object must have:
  "answer": the text to type or option to select (string, 1-3 sentences max for text fields)
  "confidence": how confident you are this is a good answer (1-10 integer)

IMPORTANT:
- If a question seems blank, garbled, incomplete, or impossible to understand, set confidence to 1.
- If the input type is "dropdown" or "radio", the answer MUST be one of the available options exactly.
- Any answer with confidence below {self._confidence_threshold} will FAIL the application and require manual review. Only set confidence below {self._confidence_threshold} if you are truly unsure.
- For salary/compensation questions, always give the full numeric value (e.g. "900000") unless the question specifically asks for lakhs or LPA format. If unsure about the amount, default to "800000" with confidence {self._confidence_threshold}. Never fail on compensation.

Questions:
{questions_block}
Return ONLY the JSON array, no markdown fences or extra text."""

            try:
                response = self._genai.chat(prompt, system_role=_SYSTEM_ROLE)
                parsed = self._parse_llm_batch_response(response, len(batch))

                for idx, (field, item) in enumerate(zip(batch, parsed)):
                    answer = item.get("answer", "")
                    confidence = int(item.get("confidence", 0))
                    logger.info("  LLM batch answer %d (confidence=%d): '%s'",
                                idx + 1, confidence, str(answer)[:60])

                    if confidence < self._confidence_threshold:
                        raise WorkdayApplyError(
                            f"LLM confidence too low ({confidence}) for question: "
                            f"{field['label'][:80]}"
                        )
                    all_answers.append(str(answer))

            except (json.JSONDecodeError, KeyError, ValueError) as e:
                raise WorkdayApplyError(f"Failed to parse LLM batch response: {e}") from e

        return all_answers

    @staticmethod
    def _parse_llm_batch_response(text: str, expected_count: int) -> list[dict]:
        """Parse a JSON array from LLM batch response."""
        text = text.strip()
        if text.startswith("```"):
            text = re.sub(r"^```\w*\n?", "", text)
            text = re.sub(r"\n?```$", "", text)
            text = text.strip()
        # Find JSON array
        start = text.find("[")
        end = text.rfind("]")
        if start != -1 and end != -1:
            parsed = json.loads(text[start:end + 1])
        else:
            parsed = json.loads(text)
        if not isinstance(parsed, list) or len(parsed) != expected_count:
            raise ValueError(
                f"Expected {expected_count} answers, got {len(parsed) if isinstance(parsed, list) else 'non-array'}"
            )
        return parsed

    def _build_profile_text(self) -> str:
        """Build a profile summary from config for LLM context."""
        parts = []
        if self._profiles:
            p = self._profiles[0]
            parts.append(f"  Title: {p.title}")
            parts.append(f"  Skills: {', '.join(p.skills[:15])}")
            parts.append(f"  Experience: {p.experience_years} years")
            parts.append(f"  Location: {p.location_preference}")
            if p.additional_criteria:
                parts.append(f"  Background: {p.additional_criteria.strip()}")

        for exp in self._work_exp:
            parts.append(f"  Work: {exp['title']} at {exp['company']} ({exp.get('start_date', '')} - {exp.get('end_date', 'present')})")

        for edu in self._education:
            parts.append(f"  Education: {edu['degree']} in {edu.get('field', '')} from {edu['institution']} ({edu.get('graduation_year', '')})")

        return "\n".join(parts)

