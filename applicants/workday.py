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
  - Fills known fields from workday_fields in workday_answers.yaml (keyed by data-automation-id)
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

from services.genai_client import GenAIClient, ChatSession
from applicants.eligibility_prompt import ELIGIBILITY_CRITERIA, ELIGIBILITY_PROMPT_TEMPLATE

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
            raise WorkdayApplyError("answers_raw is required — raw workday_answers.yaml content must be provided")
        self._apply_cfg = config.get("apply", {})
        self._genai_cfg = config.get("genai", {})
        self._profiles = config.get("profiles", [])
        self._answers = answers
        self._answers_raw = answers_raw
        self._work_exp = answers.get("work_experience", [])
        self._credentials = _load_credentials()
        self._workday_fields = _load_workday_answers()
        if "education" not in self._workday_fields:
            raise WorkdayApplyError("'education' section missing from workday_answers.yaml")
        self._confidence_threshold = self._apply_cfg.get("answer_confidence_threshold", 7)
        self._test_mode = self._apply_cfg.get("test_mode", False)
        self._apply_email = answers.get("workday_account", {}).get("email", "")

        self._answer_reasoning: str = ""
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

    def _load_context(self, tenant: str):
        """Create a fresh browser context."""
        self._context = self._browser.new_context()
        self._page = self._context.new_page()
        self._current_tenant = tenant

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

    def _review_before_submit(self, job: dict):
        """Send the review page content to LLM for eligibility check before submitting."""
        review_page = self._page.query_selector('[data-automation-id="applyFlowReviewPage"]')
        if not review_page:
            review_content = self._page.inner_text("body")
        else:
            review_content = review_page.inner_text()

        workday_answers_content = _WORKDAY_ANSWERS_PATH.read_text(encoding="utf-8")

        prompt = ELIGIBILITY_PROMPT_TEMPLATE.format(
            criteria=ELIGIBILITY_CRITERIA,
            answers_content=self._answers_raw,
            workday_answers_content=workday_answers_content,
            job_title=job.get("title", ""),
            job_company=job.get("company", ""),
            job_description=job.get("job_description", ""),
            review_content=review_content,
        )

        logger.info("Reviewing application with LLM before submit...")
        response = self._genai.chat(prompt, system_role=_SYSTEM_ROLE)

        # Parse response
        text = response.strip()
        if text.startswith("```"):
            text = re.sub(r"^```\w*\n?", "", text)
            text = re.sub(r"\n?```$", "", text)
            text = text.strip()

        try:
            result = json.loads(text)
        except json.JSONDecodeError:
            raise WorkdayApplyError(f"LLM eligibility review returned unparseable response: {text[:200]}")

        verdict = result.get("verdict", "").lower()
        self._answer_reasoning = result.get("reasoning", "")
        if verdict != "eligible":
            reasons = result.get("reasons", ["Unknown reason"])
            reasons_str = "; ".join(reasons)
            raise WorkdayApplyError(f"LLM eligibility check FAILED: {reasons_str}")

        logger.info("LLM eligibility check PASSED")

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
                self._review_before_submit(job)
                if self._test_mode:
                    raise WorkdayApplyError("TEST MODE — stopping before final submit")
                self._page.query_selector(_SEL["next_btn"]).click()
                self._page.wait_for_load_state("networkidle")
                logger.info("Application submitted")
                return

            # Route to correct page handler
            if self._page.query_selector(_SEL["info_page"]):
                self._fill_form_page(job)
            elif self._page.query_selector(_SEL["exp_page"]):
                self._fill_experience_page(resume_data, job, pdf_path)
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

    def _fill_experience_page(self, resume_data: dict, job: dict, pdf_path: Path = None):
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
        edu_section = self._page.query_selector('[aria-labelledby="Education-section"]')
        if edu_section:
            edu = self._workday_fields.get("education", {})
            logger.info("  Adding education entry")
            self._click_section_add("Education")
            time.sleep(1)
            panel = self._get_last_panel("Education")
            self._fill_section_panel(panel, edu)
        else:
            logger.info("  No Education section on this portal — skipping")

        # Fill languages (from workday_answers.yaml)
        lang_section = self._page.query_selector('[aria-labelledby="Languages-section"]')
        if lang_section:
            languages = self._workday_fields.get("languages", [])
            for i, lang in enumerate(languages):
                logger.info("  Adding language %d: %s", i + 1, lang.get("language", ""))
                self._click_section_add("Languages")
                time.sleep(1)
                panel = self._get_last_panel("Languages")
                self._fill_section_panel(panel, lang)
        else:
            logger.info("  No Languages section on this portal — skipping")

        # Skills: always review with LLM (remove irrelevant, add missing)
        skills_container = self._page.query_selector(_SEL["exp_skills"])
        if skills_container:
            self._fill_skills_with_llm(skills_container, job, pdf_path)
        resume_uploaded = self._page.query_selector(_SEL["resume_uploaded"])
        if not resume_uploaded:
            raise WorkdayApplyError("Resume not found on experience page — upload may have failed")

        # Fill any remaining unfilled fields (Social Network URLs, etc.) via known answers + AI fallback
        self._fill_remaining_fields(job)

    def _fill_remaining_fields(self, job: dict):
        """Fill any unfilled fields on the experience page using known answers + AI fallback."""
        fields = self._scan_page_fields(scope=self._page)
        unfilled = [f for f in fields if not self._is_field_filled(f)]
        if not unfilled:
            return

        logger.info("  Found %d unfilled field(s) on experience page", len(unfilled))

        # First pass: fill from workday_fields
        ai_fields = []
        for field in unfilled:
            name = field["field_name"]
            if name in self._workday_fields:
                value = self._workday_fields[name]
                logger.info("  Filling remaining '%s' → '%s'", name, str(value)[:30])
                try:
                    self._fill_field_by_type(field, value)
                except WorkdayApplyError as e:
                    logger.warning("  Failed to fill '%s': %s", name, e)
            else:
                ai_fields.append(field)

        # Second pass: AI fallback for the rest
        if ai_fields:
            logger.info("  Sending %d remaining field(s) to AI", len(ai_fields))
            for field in ai_fields:
                if field["input_type"] == "dropdown" and not field.get("options"):
                    field["options"] = self._get_dropdown_options(field)
            answers = self._ask_llm_batch(ai_fields, job)
            for field, answer in zip(ai_fields, answers):
                if answer == "SKIP":
                    logger.info("  Skipping field '%s' (LLM said SKIP)", field["field_name"])
                    continue
                try:
                    self._fill_field_by_type(field, answer)
                except WorkdayApplyError as e:
                    logger.warning("  Failed to fill '%s': %s", field["field_name"], e)

    def _is_field_filled(self, field: dict) -> bool:
        """Check if a field already has a value."""
        container = field["container"]
        # Multiselect with selected items
        selected_items = container.query_selector_all('[data-automation-id="selectedItem"]')
        if selected_items:
            return True
        # Dropdown with a selection
        btn = container.query_selector('button[aria-haspopup="listbox"]')
        if btn:
            text = btn.inner_text().strip()
            return text != "" and text.lower() not in ("select", "select one", "-- select --", "")
        # Text input
        inp = container.query_selector("input")
        if inp:
            val = inp.get_attribute("value") or ""
            return val.strip() != ""
        # Textarea
        textarea = container.query_selector("textarea")
        if textarea:
            val = textarea.input_value() or ""
            return val.strip() != ""
        return False

    # ── Skills Filling (LLM + Workday API) ──────────────────────

    _SKILLS_SYSTEM_ROLE = (
        "You are a skill-matching assistant for job applications. "
        "You help curate the Skills section by removing irrelevant skills and adding relevant ones. "
        "Always return ONLY valid JSON. No markdown fences, no explanation."
    )

    def _fill_skills_with_llm(self, container, job: dict, pdf_path: Path = None):
        """Review and curate skills using LLM + Workday skills API.

        Always runs — reads existing skills, asks LLM what to remove/add,
        then verifies additions against Workday's API with back-and-forth retries.
        """
        skills_min = self._apply_cfg.get("skills_min", 8)
        skills_max = self._apply_cfg.get("skills_max", 20)
        max_retries = self._apply_cfg.get("skills_retry_count", 3)

        # Read currently selected skills from the UI
        existing_skills = self._get_selected_skills(container)
        logger.info("  Skills: %d currently selected: %s", len(existing_skills), existing_skills)

        # Clean skill names for LLM (strip "(Suggested)" suffix)
        clean_skills = [s.replace(" (Suggested)", "").strip() for s in existing_skills]

        # Read resume tex for context (no bias from config skills list)
        resume_content = ""
        if pdf_path:
            tex_path = pdf_path.with_suffix(".tex")
            if tex_path.exists():
                resume_content = tex_path.read_text(encoding="utf-8")

        session = ChatSession(self._genai, system_role=self._SKILLS_SYSTEM_ROLE)

        initial_prompt = (
            f"You are managing the Skills section of a job application. "
            f"Review the current skills and decide what changes are needed.\n\n"
            f"Job Title: {job.get('title', '')}\n"
            f"Company: {job.get('company', '')}\n"
            f"Job Description: {job.get('job_description', '')}\n\n"
            f"Candidate's Resume:\n{resume_content}\n\n"
            f"Currently Selected Skills in Application: {clean_skills}\n\n"
            f"Target: {skills_min} to {skills_max} relevant skills for this specific job.\n\n"
            f"Instructions:\n"
            f"- REMOVE skills that are irrelevant to this job (e.g. 'Protein A' for a software role)\n"
            f"- ADD skills that are relevant and the candidate actually has based on the resume\n"
            f"- NEVER add skills that are already in the Currently Selected list above\n"
            f"- Use exact, standard skill names (1-3 words, e.g. 'Java', 'Python', 'SQL', 'Docker')\n"
            f"- Prioritize skills mentioned in the job description\n"
            f"- Keep skills that are already selected AND relevant\n\n"
            f"Return ONLY a JSON object:\n"
            f'{{"remove": ["Skill to uncheck", ...], "add": ["Skill to add", ...]}}\n\n'
            f"If no changes needed, return: {{\"remove\": [], \"add\": []}}"
        )

        response = session.send(initial_prompt)
        changes = self._parse_skills_changes(response)
        to_remove = changes.get("remove", [])
        to_add = changes.get("add", [])
        logger.info("  LLM says remove %d, add %d", len(to_remove), len(to_add))

        # Remove irrelevant skills
        for skill in to_remove:
            self._uncheck_skill_in_ui(container, skill)

        # Add skills — verify against Workday API first
        added = []
        not_found_with_options = {}  # {skill: [valid_options_from_api]}

        for skill in to_add:
            if len(existing_skills) - len(to_remove) + len(added) >= skills_max:
                break
            match, valid_options = self._search_skill_api(skill)
            if match:
                self._check_skill_in_ui(container, match)
                added.append(match)
            else:
                not_found_with_options[skill] = valid_options

        # Back-and-forth: ask LLM for alternatives with valid options from API
        retry = 0
        current_count = len(existing_skills) - len(to_remove) + len(added)
        while not_found_with_options and retry < max_retries:
            retry += 1
            logger.info("  Skills retry %d/%d: %d total, %d not found",
                        retry, max_retries, current_count, len(not_found_with_options))

            # Build retry prompt with valid options for each failed skill
            options_text = ""
            for skill, options in not_found_with_options.items():
                options_text += f"  '{skill}' → not found. Valid options returned: {options[:10]}\n"

            retry_prompt = (
                f"These skills were NOT found as exact matches in Workday's database:\n"
                f"{options_text}\n"
                f"Currently have {current_count} skills, need at least {skills_min}.\n"
                f"Pick from the valid options listed above, or suggest {skills_max - current_count} "
                f"other standard skill names.\n"
                f"Return ONLY a JSON array of skill names to try."
            )
            response = session.send(retry_prompt)
            alt_skills = self._parse_skills_json(response)
            logger.info("  LLM alternatives: %s", alt_skills)
            not_found_with_options = {}

            for skill in alt_skills:
                if current_count >= skills_max:
                    break
                if skill.lower() in [a.lower() for a in added]:
                    continue
                match, valid_options = self._search_skill_api(skill)
                if match:
                    if match.lower() not in [a.lower() for a in added]:
                        self._check_skill_in_ui(container, match)
                        added.append(match)
                        current_count += 1
                else:
                    not_found_with_options[skill] = valid_options

        final_count = len(existing_skills) - len(to_remove) + len(added)
        if final_count < skills_min:
            logger.warning("  Skills: only %d (minimum: %d)", final_count, skills_min)
        logger.info("  Skills done: removed %d, added %d — total: %d",
                    len(to_remove), len(added), final_count)

    def _get_selected_skills(self, container) -> list[str]:
        """Read currently selected skill names from the UI."""
        pills = container.query_selector_all(_SEL["selected_item"])
        skills = []
        for pill in pills:
            # Selected items typically show the skill name as text
            text = pill.inner_text().strip()
            if text:
                skills.append(text)
        return skills

    def _parse_skills_changes(self, response: str) -> dict:
        """Parse {remove: [], add: []} JSON from LLM response."""
        import json as _json
        text = response.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        try:
            result = _json.loads(text)
            if isinstance(result, dict) and "remove" in result and "add" in result:
                return result
        except _json.JSONDecodeError:
            # Try to find JSON object in response
            start = text.find("{")
            end = text.rfind("}")
            if start != -1 and end != -1:
                try:
                    result = _json.loads(text[start:end + 1])
                    if isinstance(result, dict):
                        return result
                except _json.JSONDecodeError:
                    pass
        logger.warning("  Could not parse skills changes JSON: %s", text[:100])
        return {"remove": [], "add": []}

    def _parse_skills_json(self, response: str) -> list[str]:
        """Parse a JSON array of skill names from LLM response."""
        import json as _json
        text = response.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        try:
            skills = _json.loads(text)
            if isinstance(skills, list):
                return [s for s in skills if isinstance(s, str)]
        except _json.JSONDecodeError:
            # Try to find array in response
            start = text.find("[")
            end = text.rfind("]")
            if start != -1 and end != -1:
                try:
                    skills = _json.loads(text[start:end + 1])
                    return [s for s in skills if isinstance(s, str)]
                except _json.JSONDecodeError:
                    pass
        logger.warning("  Could not parse skills JSON from LLM: %s", text[:100])
        return []

    def _search_skill_api(self, skill_name: str) -> tuple[str | None, list[str]]:
        """Search Workday's skills API for an exact match.

        Returns:
            (exact_match_or_None, valid_options_list)
            valid_options excludes crowdsourced garbage.
        """
        base_url = self._get_cxs_base_url()
        url = f"{base_url}/skillsearch"

        try:
            resp = self._http.get(url, params={"search": skill_name.lower()})
            if resp.status_code != 200:
                logger.debug("  Skill API returned %d for '%s'", resp.status_code, skill_name)
                return None, []

            results = resp.json()
            if not results:
                return None, []

            # If only 1 result returned, accept it directly
            if len(results) == 1:
                return skill_name, []

            # Multiple results: find exact match, collect valid options
            valid_options = []
            exact_match = None

            for item in results:
                desc = item.get("descriptor", "")
                item_id = item.get("id", "")
                # Skip crowdsourced garbage: id == descriptor and starts lowercase
                if item_id == desc and desc[0:1].islower():
                    continue
                valid_options.append(desc)
                if desc.lower() == skill_name.lower() and exact_match is None:
                    exact_match = desc

            return exact_match, valid_options
        except Exception as e:
            logger.debug("  Skill API error for '%s': %s", skill_name, e)
        return None, []

    def _check_skill_in_ui(self, container, skill_name: str):
        """Search for a skill and add it. Raises WorkdayApplyError if not added."""
        multiselect = container.query_selector(_SEL["multiselect"])
        if not multiselect:
            raise WorkdayApplyError(f"Skills multiselect not found when adding '{skill_name}'")
        search_input = multiselect.query_selector("input")
        if not search_input:
            raise WorkdayApplyError(f"Skills search input not found when adding '{skill_name}'")

        # Type and search
        search_input.click(force=True)
        search_input.fill("")
        search_input.type(skill_name)
        search_input.press("Enter")
        # Wait for real search results (multiSelectHeader), not the instant "No Items." placeholder
        try:
            self._page.wait_for_selector('[data-automation-id="multiSelectHeader"]', timeout=8000)
        except Exception:
            pass
        active_list = self._page.query_selector('[data-automation-id="activeListContainer"]')
        search_scope = active_list if active_list else container

        items = search_scope.query_selector_all(_SEL["menu_item"])
        for item in items:
            label_el = item.query_selector(_SEL["prompt_option"])
            if label_el:
                label = (label_el.get_attribute("data-automation-label") or "").lower()
                if label == skill_name.lower():
                    leaf_node = item.query_selector('[data-automation-id="promptLeafNode"]')
                    already_checked = (leaf_node.get_attribute("data-automation-checked") or "").lower() == "checked" if leaf_node else False
                    if leaf_node and not already_checked:
                        leaf_node.click()
                    time.sleep(0.5)
                    break

        # Verify skill was added
        time.sleep(0.5)
        selected = self._get_selected_skills(container)
        if any(skill_name.lower() in s.lower() for s in selected):
            search_input.fill("")
            return
        raise WorkdayApplyError(f"Skill '{skill_name}' was not added after attempt")

    def _uncheck_skill_in_ui(self, container, skill_name: str):
        """Remove a skill by clicking the X (DELETE_charm) on its pill."""
        pills = container.query_selector_all('[data-automation-id="selectedItem"]')
        skill_lower = skill_name.lower().replace(" (suggested)", "")
        for pill in pills:
            label = (pill.get_attribute("title") or "").lower().replace(" (suggested)", "")
            if label == skill_lower:
                delete_btn = pill.query_selector('[data-automation-id="DELETE_charm"]')
                if delete_btn:
                    delete_btn.click()
                    logger.info("    Removed skill: '%s'", skill_name)
                    time.sleep(0.3)
                return
        logger.debug("    Skill pill not found for removal: '%s'", skill_name)

    def _get_cxs_base_url(self) -> str:
        """Get the CXS API base URL from the current browser URL."""
        parsed = urlparse(self._page.url)
        tenant = self._current_tenant
        return f"{parsed.scheme}://{parsed.hostname}/wday/cxs/{tenant}"

    # ── Section Helpers ──────────────────────────────────────────

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

    def _get_last_panel(self, section_id: str):
        """Return the last (just-added) panel within a section."""
        section = self._page.query_selector(f'[aria-labelledby="{section_id}-section"]')
        if not section:
            return None
        panels = section.query_selector_all('[role="group"][aria-labelledby*="-panel"]')
        return panels[-1] if panels else section

    def _fill_section_panel(self, panel, answers: dict):
        """Dynamically scan fields in a panel and fill from answers dict."""
        fields = self._scan_page_fields(scope=panel)
        logger.info("    Panel has %d fillable field(s)", len(fields))

        for field in fields:
            name = field["field_name"]
            value = self._resolve_panel_value(name, field["label"], answers)
            if value is not None:
                logger.info("    Filling '%s' (%s) → '%s'", name, field["label"], str(value)[:30])
                self._fill_field_by_type(field, value)
            elif field["required"]:
                logger.warning("    No answer for required field '%s' (%s)", name, field["label"])
            else:
                logger.debug("    Skipping optional: '%s'", name)

    def _resolve_panel_value(self, field_name: str, label: str, answers: dict):
        """Find a value for a field by name or label (exact match only)."""
        # Direct match by field_name
        if field_name in answers:
            return answers[field_name]
        if field_name in self._workday_fields:
            return self._workday_fields[field_name]

        # Exact label match against answers keys
        label_lower = label.lower().strip()
        if label_lower in answers:
            return answers[label_lower]

        return None

    def _fill_work_experience_entry(self, exp: dict):
        """Fill a single work experience entry after clicking Add."""
        panel = self._get_last_panel("Work-Experience")

        # Text fields
        self._fill_experience_field("jobTitle", exp.get("jobTitle", ""), scope=panel)
        self._fill_experience_field("companyName", exp.get("companyName", ""), scope=panel)
        if exp.get("location"):
            self._fill_experience_field("location", exp["location"], scope=panel)

        # Checkbox: currently work here
        if exp.get("currentlyWorkHere"):
            checkbox = panel.query_selector(_SEL["exp_currently_here"])
            if checkbox and checkbox.get_attribute("aria-checked") != "true":
                checkbox.click()

        # Date fields
        self._fill_date_field("startDate", exp.get("startDate", ""), scope=panel)
        if not exp.get("currentlyWorkHere") and exp.get("endDate", "").lower() != "present":
            self._fill_date_field("endDate", exp.get("endDate", ""), scope=panel)

        # Role description (optional)
        if exp.get("roleDescription"):
            desc = panel.query_selector(_SEL["exp_role_desc"])
            if desc:
                desc.fill(exp["roleDescription"])

    def _fill_education_entry(self, edu: dict):
        """Fill a single education entry after clicking Add.

        Fields: school (searchable multiselect), degree (dropdown),
        fieldOfStudy (searchable multiselect), dates.
        """
        panel = self._get_last_panel("Education")

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
            self._fill_experience_field("gradeAverage", edu["gradeAverage"], scope=panel)

        # Date fields
        if edu.get("startDate"):
            self._fill_date_field("startDate", edu["startDate"], scope=panel)
        if edu.get("endDate"):
            self._fill_date_field("endDate", edu["endDate"], scope=panel)

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
                # Dismiss any lingering search popup
                if self._page.query_selector(_SEL["dropdown_listbox"]):
                    self._page.keyboard.press("Escape")
                if self._page.query_selector(_SEL["dropdown_listbox"]):
                    self._page.keyboard.press("Tab")
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

    def _fill_experience_field(self, field_name: str, value: str, scope):
        """Fill a text field by its formField automation ID within a scoped panel."""
        container = scope.query_selector(f'[data-automation-id="formField-{field_name}"]')
        if not container:
            return
        text_input = container.query_selector("input[type='text']")
        if text_input:
            text_input.fill(value)

    def _fill_date_field(self, field_name: str, value: str, scope):
        """Fill a date field (MM/YYYY format) by its formField automation ID within a scoped panel."""
        if not value or value.lower() == "present":
            return
        container = scope.query_selector(f'[data-automation-id="formField-{field_name}"]')
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
        fields = self._scan_page_fields(scope=self._page)
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
                if answer == "SKIP":
                    logger.info("  Skipping optional field '%s' (LLM said SKIP)", field["field_name"])
                    continue
                self._fill_field_by_type(field, answer)

    def _scan_page_fields(self, scope) -> list[dict]:
        """Scan for all fillable form fields within scope.

        Workday wraps each field in a div with data-automation-id="formField-{fieldName}".
        Inside, the actual input can be:
          - <input type="text"> (text fields)
          - <textarea> (text areas)
          - <button aria-haspopup="listbox"> (custom dropdowns)
          - <input type="radio"> (radio groups)
          - div[data-automation-id="multiSelectContainer"] (multiselects)

        Args:
            scope: Element to search within (panel, section, or self._page).

        Returns list of field descriptors:
            {field_name, label, input_type, required, container, element, options}
        """
        fields = []
        containers = scope.query_selector_all('[data-automation-id^="formField-"]')

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

        # Check for checkbox
        checkbox = container.query_selector("input[type='checkbox']")
        if checkbox:
            return {
                "field_name": field_name,
                "label": self._get_container_label(container),
                "input_type": "checkbox",
                "required": self._container_is_required(container),
                "container": container,
                "element": checkbox,
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

        # Check for date year spinbutton (e.g. firstYearAttended, lastYearAttended)
        year_input = container.query_selector('[data-automation-id="dateSectionYear-input"]')
        if year_input:
            return {
                "field_name": field_name,
                "label": self._get_container_label(container),
                "input_type": "text",
                "required": self._container_is_required(container),
                "container": container,
                "element": year_input,
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

        elif input_type == "checkbox":
            fill_value = value[0] if isinstance(value, list) else value
            normalized = str(fill_value).lower()
            if normalized in ("true", "yes", "1"):
                if field["element"].get_attribute("aria-checked") != "true":
                    field["element"].click()
            elif normalized not in ("false", "no", "0", "skip"):
                raise WorkdayApplyError(
                    f"Invalid checkbox value '{fill_value}' for field '{field['field_name']}'. Expected Yes/No."
                )

    @staticmethod
    def _normalize_text(text: str) -> str:
        """Strip diacriticals and lowercase for comparison."""
        nfkd = unicodedata.normalize("NFKD", text)
        return "".join(c for c in nfkd if not unicodedata.combining(c)).lower().strip()

    def _select_from_dropdown(self, field: dict, value, substring_match: bool = False):
        """Select a value from a click-to-open dropdown (all options pre-loaded).

        Used for fields that show a button (e.g. "Select One") which, when clicked,
        opens a popup listbox with all available options visible at once. No typing/search.

        Retries up to 3 times if the selection doesn't stick (React re-renders can reset it).

        Args:
            field: Field descriptor with 'element' (the dropdown button) and 'field_name'.
            value: String or list of strings (priority order, first match wins).
            substring_match: If True, also matches candidates as substrings of option text.
                             E.g. "Fluent" matches "3 - Fluent". Default False.
        """
        btn = field["element"]
        candidates = value if isinstance(value, list) else [value]

        for attempt in range(3):
            # Dismiss any stale listbox left by a previous field
            if self._page.query_selector(_SEL["dropdown_listbox"]):
                self._page.keyboard.press("Escape")
            if self._page.query_selector(_SEL["dropdown_listbox"]):
                self._page.keyboard.press("Tab")
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

            # Try each candidate in priority order
            selected_text = None
            for candidate in candidates:
                norm_candidate = self._normalize_text(candidate)
                # Exact match
                if norm_candidate in option_map:
                    option_map[norm_candidate].click()
                    selected_text = candidate
                    break
                # Substring match
                if substring_match:
                    for opt_key, opt_el in option_map.items():
                        if norm_candidate in opt_key:
                            opt_el.click()
                            selected_text = candidate
                            break
                    if selected_text:
                        break

            if not selected_text:
                raise WorkdayApplyError(
                    f"No matching option for dropdown '{field['field_name']}': {candidates}. Available: {available}"
                )

            self._page.wait_for_selector(
                _SEL["dropdown_listbox"], state="hidden", timeout=_TIMEOUT
            )
            time.sleep(0.5)

            # Verify the selection stuck by checking button value
            btn_value = btn.get_attribute("value") or btn.inner_text().strip()
            if btn_value and btn_value != "Select One":
                logger.debug("Dropdown '%s' selected '%s' on attempt %d", field["field_name"], selected_text, attempt + 1)
                return

            logger.warning("Dropdown '%s' selection didn't stick (attempt %d/3), retrying...", field["field_name"], attempt + 1)
            time.sleep(1)

        raise WorkdayApplyError(
            f"Dropdown '{field['field_name']}' selection failed after 3 attempts"
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
                    # Filter out placeholder values for required dropdowns
                    opts = field["options"]
                    if field["required"]:
                        opts = [o for o in opts if o.lower() not in ("select one", "-- select --", "select", "")]
                    options_text = f"  Options: {json.dumps(opts)}"
                required_text = "Required" if field["required"] else "Optional"
                questions_block += (
                    f"\n{idx}. Question: \"{field['label']}\"\n"
                    f"   Input type: {field['input_type']}\n"
                    f"   {required_text}\n"
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
- If the input type is "checkbox", the answer MUST be exactly "Yes" or "No" (nothing else).
- Any answer with confidence below {self._confidence_threshold} will FAIL the application and require manual review. Only set confidence below {self._confidence_threshold} if you are truly unsure.
- For salary/compensation questions, always give the full numeric value (e.g. "900000") unless the question specifically asks for lakhs or LPA format. If unsure about the amount, default to "800000" with confidence {self._confidence_threshold}. Never fail on compensation.
- For optional fields: if you don't have enough information to answer or the field is not relevant, set answer to "SKIP" with confidence 10. Do NOT fail on optional fields.
- Always SKIP optional name fields such as "local given name", "local last name", "preferred name", "middle name", "nickname", or any variant of these. Set answer to "SKIP" with confidence 10.

KNOWN WORKDAY BUGS:
- Language proficiency sections often include garbage/undefined dropdown fields with labels like "4 - Other", "5 - Unknown", "undefined", or numbered labels that make no sense. These are mandatory but meaningless. For these, always select "Fluent" (or the highest proficiency option available). Never select placeholder values like "Select One" for these fields. Set confidence to 8.

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

        edu = self._workday_fields.get("education", {})
        if edu:
            degree = edu.get("degree", [""])[0] if isinstance(edu.get("degree"), list) else edu.get("degree", "")
            field = edu.get("fieldOfStudy", [""])[0] if isinstance(edu.get("fieldOfStudy"), list) else edu.get("fieldOfStudy", "")
            school = edu.get("school", [""])[0] if isinstance(edu.get("school"), list) else edu.get("school", "")
            year = edu.get("lastYearAttended", edu.get("endDate", ""))
            parts.append(f"  Education: {degree} in {field} from {school} ({year})")

        return "\n".join(parts)

