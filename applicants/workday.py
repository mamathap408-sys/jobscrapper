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
from pathlib import Path
from urllib.parse import urlparse

import httpx
import yaml
from playwright.sync_api import sync_playwright, Page, TimeoutError as PlaywrightTimeout

from services.genai_client import GenAIClient

logger = logging.getLogger(__name__)

def _parse_llm_json(text: str) -> dict:
    """Extract JSON object from LLM response (handles markdown fences)."""
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```\w*\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
        text = text.strip()
    matches = list(re.finditer(r"\{[^{}]*\}", text))
    if matches:
        return json.loads(matches[-1].group())
    return json.loads(text)


# Default timeout for waiting on Workday elements (ms)
_TIMEOUT = 15_000

# ── Workday Selectors ──────────────────────────────────────────
# Keyed by logical name. These are data-automation-id based selectors
# common across most Workday tenants. Update as needed when testing
# against new portals.
_SEL = {
    # Job page
    "apply_btn":        '[data-automation-id="jobPostingApplyButton"], [data-automation-id="adventureButton"]',
    "autofill_resume":  '[data-automation-id="autofillWithResume"]',

    # Auth page
    "auth_form":        '[data-automation-id="signInContent"]',
    "sign_in_btn":      '[data-automation-id="signInLink"]',
    "auth_email":       '[data-automation-id="email"]',
    "auth_password":    '[data-automation-id="password"]',
    "sign_in_submit":   '[data-automation-id="click_filter"]',

    # Navigation
    "next_btn":         '[data-automation-id="bottom-navigation-next-button"], [data-automation-id="pageFooterNextButton"]',

    # Resume upload
    "resume_page":      '[data-automation-id="resumeUpload"]',
    "resume_upload":    'input[data-automation-id="file-upload-input-ref"]',
    "resume_uploaded":  '[data-automation-id="file-upload-item"]',
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

    def __init__(self, config: dict, answers: dict):
        self._apply_cfg = config.get("apply", {})
        self._genai_cfg = config.get("genai", {})
        self._profiles = config.get("profiles", [])
        self._answers = answers
        self._personal = answers.get("personal", {})
        self._work_exp = answers.get("work_experience", [])
        self._education = answers.get("education", [])
        self._credentials = _load_credentials()
        self._workday_fields = _load_workday_answers()
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

    def apply(self, job: dict, pdf_path: Path) -> bool:
        """Apply to a single Workday job. Returns True on success.

        Args:
            job:      Dict from DB with keys: job_id, url, title, company,
                      job_description, resume_name.
            pdf_path: Path to the resume PDF to upload.

        Raises:
            WorkdayApplyError: On any application step failure.
        """
        job_url = job["url"]
        tenant = _get_tenant(job_url)

        logger.info("Applying to: %s at %s (%s)", job["title"], job["company"], job_url)

        # Load context with saved session for this tenant
        if self._current_tenant != tenant:
            if self._context:
                self._context.close()
            self._load_context(tenant)

        try:
            # Step 1: Navigate to job page and click Apply
            self._page.goto(job_url, wait_until="networkidle", timeout=_TIMEOUT)
            self._click_apply_button()

            # Step 2: Handle authentication (if session expired or first time)
            self._handle_auth()

            # Step 3: Fill the multi-step application form
            self._fill_application(job, pdf_path)

            logger.info("Successfully applied to: %s at %s", job["title"], job["company"])
            return True

        except PlaywrightTimeout as e:
            raise WorkdayApplyError(f"Timeout waiting for element: {e}") from e

    # ── Step 1: Click Apply ────────────────────────────────────

    def _click_apply_button(self):
        """Find and click the Apply button, then select 'Autofill with Resume'."""
        self._page.wait_for_selector(_SEL["apply_btn"], timeout=_TIMEOUT).click()

        # Handle "Start Your Application" modal
        autofill_btn = self._page.wait_for_selector(_SEL["autofill_resume"], timeout=_TIMEOUT)
        logger.info("Application modal detected — choosing 'Autofill with Resume'")
        autofill_btn.click()
        self._page.wait_for_load_state("domcontentloaded")

    # ── Step 2: Authentication ─────────────────────────────────

    def _handle_auth(self):
        """Handle authentication. Auto sign-in if credentials exist, else manual."""
        # Wait for either auth form or next button (indicates page is ready)
        self._page.wait_for_selector(
            f'{_SEL["auth_form"]}, {_SEL["next_btn"]}', timeout=_TIMEOUT
        )

        if not self._page.query_selector(_SEL["auth_form"]):
            logger.info("Session valid — skipping authentication")
            return

        # Check if credentials exist for this tenant
        creds = self._credentials.get(self._current_tenant)
        if creds:
            self._auto_sign_in(creds)
        else:
            logger.info("No credentials for tenant: %s — manual sign-in required", self._current_tenant)
            input("Press Enter after you have signed in manually...")

        # Save fresh session to disk
        self._save_session()

    def _auto_sign_in(self, creds: dict):
        """Automatically sign in using stored credentials."""
        logger.info("Auto sign-in for tenant: %s", self._current_tenant)

        # Click Sign In to get to the sign-in form
        self._page.click(_SEL["sign_in_btn"])
        self._page.wait_for_selector(_SEL["auth_email"], timeout=_TIMEOUT)

        # Fill email and password (locator API auto-retries until element is editable)
        self._page.locator(_SEL["auth_email"]).fill(creds["email"])
        self._page.locator(_SEL["auth_password"]).fill(creds["password"])

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
        self._page.wait_for_load_state("domcontentloaded")

    def _is_submit_page(self) -> bool:
        """Check if the current page has a Submit button."""
        btn = self._page.query_selector(_SEL["next_btn"])
        if btn:
            text = btn.inner_text().strip().lower()
            return "submit" in text
        return False

    def _fill_application(self, job: dict, pdf_path: Path):
        """Walk through the Workday application form dynamically."""

        # Step 1: Resume upload
        self._page.wait_for_selector(_SEL["resume_page"], timeout=_TIMEOUT)
        self._upload_resume(pdf_path)
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

            # Fill all fields on current page
            self._fill_page(job)
            self._click_next()

    def _upload_resume(self, pdf_path: Path):
        """Upload the resume PDF file."""
        logger.info("Uploading resume: %s", pdf_path.name)
        self._page.set_input_files(_SEL["resume_upload"], str(pdf_path))
        self._page.wait_for_selector(_SEL["resume_uploaded"], timeout=_TIMEOUT)

    # ── Generic Page Filler ────────────────────────────────────

    def _fill_page(self, job: dict):
        """Detect and fill all form fields on the current page."""
        fields = self._scan_page_fields()
        logger.info("Page has %d fillable field(s)", len(fields))

        for field in fields:
            name = field["field_name"]

            # Known field → fill directly from workday_fields
            if name in self._workday_fields:
                value = self._workday_fields[name]
                logger.info("  Filling '%s' → '%s'", name, value[:30])
                self._fill_field_by_type(field, value)
                continue

            # Skip optional fields we don't have data for
            if not field["required"]:
                logger.debug("  Skipping optional: '%s'", name)
                continue

            # Unknown required field → AI fallback
            logger.info("  AI fallback for required field: '%s' (label: '%s')",
                        name, field["label"][:60])
            answer = self._ask_llm(field["label"], field["input_type"],
                                   field.get("options", []), job)
            self._fill_field_by_type(field, answer)

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
        """Classify a formField container and return a field descriptor, or None if already filled."""

        # Check for radio buttons
        radios = container.query_selector_all("input[type='radio']")
        if radios:
            checked = container.query_selector("input[type='radio']:checked")
            if checked:
                return None  # already answered
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
        multiselect = container.query_selector('[data-automation-id="multiSelectContainer"]')
        if multiselect:
            # Check if already has selections
            selected = container.query_selector('[data-automation-id="selectedItem"]')
            if selected:
                return None  # already filled
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
        dropdown_btn = container.query_selector('button[aria-haspopup="listbox"]')
        if dropdown_btn:
            btn_value = dropdown_btn.get_attribute("value") or ""
            if btn_value:  # has a value → already filled
                return None
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
            if (textarea.input_value() or "").strip():
                return None
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
            if (text_input.input_value() or "").strip():
                return None
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
        legend = container.query_selector("legend label")
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
            fill_value = value[0] if isinstance(value, list) else value
            self._fill_custom_dropdown(field, fill_value)

        elif input_type == "multiselect":
            self._fill_multiselect(field, value)

        elif input_type == "radio":
            fill_value = value[0] if isinstance(value, list) else value
            self._fill_radio(field, fill_value)

    def _fill_custom_dropdown(self, field: dict, value: str):
        """Fill a Workday custom dropdown (button with listbox)."""
        btn = field["element"]
        btn.click()
        # Wait for listbox to appear
        listbox = self._page.wait_for_selector('[role="listbox"]', timeout=_TIMEOUT)
        # Find matching option
        options = listbox.query_selector_all('[role="option"]')
        for opt in options:
            opt_text = opt.inner_text().strip()
            if opt_text.lower() == value.lower():
                opt.click()
                return
        # Substring fallback
        for opt in options:
            opt_text = opt.inner_text().strip()
            if value.lower() in opt_text.lower():
                opt.click()
                return
        logger.warning("No matching option for dropdown '%s': '%s'", field["field_name"], value)

    def _fill_multiselect(self, field: dict, value):
        """Fill a Workday multiselect widget (search and select).

        Value can be a string or a list of strings (priority order, first match wins).
        Opens dropdown, collects all available options, then selects the first priority match.
        """
        container = field["element"]
        search_input = container.query_selector("input")
        if not search_input:
            logger.warning("No search input found in multiselect: '%s'", field["field_name"])
            return

        # Open dropdown to get all options
        search_input.click()
        dropdown = self._page.wait_for_selector('[data-automation-id="activeListContainer"]', timeout=_TIMEOUT)

        # Collect all available options from THIS dropdown only
        menu_items = dropdown.query_selector_all('[data-automation-id="menuItem"]')
        available = {}
        for item in menu_items:
            label_el = item.query_selector('[data-automation-id="promptOption"]')
            if label_el:
                label = label_el.get_attribute("data-automation-label") or label_el.inner_text().strip()
                available[label.lower()] = item

        logger.info("  Multiselect '%s' has %d options: %s",
                    field["field_name"], len(available), list(available.keys()))

        # Normalize value to a priority list
        candidates = value if isinstance(value, list) else [value]

        # Select the first candidate that matches an available option
        for candidate in candidates:
            for label, item in available.items():
                if candidate.lower() == label or candidate.lower() in label:
                    item.click()
                    logger.info("  Multiselect '%s': clicked '%s'", field["field_name"], label)

                    # Check if a sub-menu appeared (back button = category expanded)
                    try:
                        self._page.wait_for_selector(
                            '[data-automation-id="backButton"]', timeout=3000
                        )
                        # Sub-menu is open — click the matching leaf item
                        sub_dropdown = self._page.query_selector('[data-automation-id="activeListContainer"]')
                        if sub_dropdown:
                            sub_items = sub_dropdown.query_selector_all('[data-automation-id="menuItem"]')
                            for sub_item in sub_items:
                                sub_label_el = sub_item.query_selector('[data-automation-id="promptOption"]')
                                if sub_label_el:
                                    sub_label = sub_label_el.get_attribute("data-automation-label") or sub_label_el.inner_text().strip()
                                    if candidate.lower() in sub_label.lower():
                                        sub_item.click()
                                        logger.info("  Multiselect '%s': selected leaf '%s'", field["field_name"], sub_label)
                                        return
                            # No exact match in sub-menu, click first item
                            if sub_items:
                                sub_items[0].click()
                                logger.info("  Multiselect '%s': selected first sub-item", field["field_name"])
                    except PlaywrightTimeout:
                        pass  # No sub-menu — selection was made directly

                    return

        logger.warning("No matching option found for multiselect '%s'. Available: %s, Candidates: %s",
                       field["field_name"], list(available.keys()), candidates)

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

    def _ask_llm(self, question: str, input_type: str,
                 options: list[str], job: dict) -> str:
        """Ask the LLM to answer a screening question."""
        profile_text = self._build_profile_text()
        options_text = ""
        if options:
            options_text = f"\nAvailable options: {json.dumps(options)}"

        prompt = f"""You are filling out a job application for the following position:
  Title: {job.get('title', '')}
  Company: {job.get('company', '')}
  Description (excerpt): {(job.get('job_description', '') or '')[:2000]}

The applicant's profile:
{profile_text}

Answer the following application question concisely and truthfully.
Return a JSON object with exactly two keys:
  "answer": the text to type or option to select (string, 1-3 sentences max for text fields)
  "confidence": how confident you are this is a good answer (1-10 integer)

Question: "{question}"
Input type: {input_type}{options_text}

If the input type is "select" or "radio", the answer MUST be one of the available options exactly.

Return ONLY the JSON object, no markdown fences or extra text."""

        try:
            response = self._genai.chat(prompt, system_role=_SYSTEM_ROLE)
            parsed = _parse_llm_json(response)
            answer = parsed.get("answer", "")
            confidence = int(parsed.get("confidence", 0))

            logger.info("  LLM answer (confidence=%d): '%s'", confidence, answer[:60])

            if confidence < self._confidence_threshold:
                raise WorkdayApplyError(
                    f"LLM confidence too low ({confidence}) for question: {question[:80]}"
                )

            return str(answer)
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            raise WorkdayApplyError(f"Failed to parse LLM answer for '{question[:60]}': {e}") from e

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

