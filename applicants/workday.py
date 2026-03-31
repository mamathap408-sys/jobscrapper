"""
applicants/workday.py — Workday Auto-Apply Engine
===================================================
Automates job application submission on Workday career portals using Playwright.

Flow per job:
  1. Navigate to job URL → click "Apply"
  2. Handle sign-in or account creation
  3. Fill personal information (name, email, phone, address)
  4. Upload tailored resume PDF
  5. Fill work history and education
  6. Answer screening questions (Q&A lookup → LLM fallback)
  7. Handle voluntary self-identification (decline all)
  8. Review and submit

On any failure: raises WorkdayApplyError → caller skips to next job.
"""

import json
import logging
import re
from pathlib import Path
from urllib.parse import urlparse

import httpx
import yaml
from playwright.sync_api import sync_playwright, Page, TimeoutError as PlaywrightTimeout

from services.genai_client import GenAIClient

logger = logging.getLogger(__name__)


def _normalize_question(text: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace."""
    text = text.lower().strip()
    text = re.sub(r"[^\w\s]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _build_qa_index(qa: dict) -> dict:
    """Normalize Q&A keys for fuzzy matching."""
    return {_normalize_question(k): v for k, v in qa.items()}


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
    "submit_btn":       '[data-automation-id="bottom-navigation-next-button"], [data-automation-id="pageFooterNextButton"]',

    # My Information
    "first_name":       '[data-automation-id="legalNameSection_firstName"]',
    "last_name":        '[data-automation-id="legalNameSection_lastName"]',
    "country_dropdown": '[data-automation-id="countryDropdown"]',
    "address_line1":    '[data-automation-id="addressSection_addressLine1"]',
    "city":             '[data-automation-id="addressSection_city"]',
    "state":            '[data-automation-id="addressSection_countryRegion"]',
    "postal_code":      '[data-automation-id="addressSection_postalCode"]',
    "phone_device":     '[data-automation-id="phone-device-type"]',
    "phone_number":     '[data-automation-id="phone-number"]',
    "email_field":      '[data-automation-id="email"]',

    # My Experience
    "resume_page":      '[data-automation-id="resumeUpload"]',
    "resume_upload":    'input[data-automation-id="file-upload-input-ref"]',
    "resume_uploaded":  '[data-automation-id="file-upload-item"]',

    # Self-identification
    "gender_dropdown":  '[data-automation-id="gender"]',
    "decline_option":   'text="Decline To Self Identify"',
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


def _load_credentials() -> dict:
    """Load per-tenant credentials from career_sites_credentials.yaml."""
    if _CREDS_PATH.exists():
        with open(_CREDS_PATH) as f:
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
        self._qa = _build_qa_index(answers.get("qa", {}))
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
            resp = self._http.get(f"{api_base}{external_path}")
            if resp.status_code != 200:
                logger.info("Job no longer valid (HTTP %d): %s", resp.status_code, job_url)
                return False
            data = resp.json()
            return "jobPostingInfo" in data
        except (httpx.HTTPError, ValueError) as e:
            logger.warning("Job validity check failed for %s: %s", job_url, e)
            return False

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

        # Wait for auth form to disappear (sign-in succeeded)
        self._page.wait_for_selector(_SEL["auth_form"], state="hidden", timeout=_TIMEOUT)

        # Save fresh session to disk
        self._save_session()

    def _auto_sign_in(self, creds: dict):
        """Automatically sign in using stored credentials."""
        logger.info("Auto sign-in for tenant: %s", self._current_tenant)

        # Click Sign In to get to the sign-in form
        self._page.click(_SEL["sign_in_btn"])
        self._page.wait_for_selector(_SEL["auth_email"], timeout=_TIMEOUT)

        # Fill email and password
        email_input = self._page.query_selector(_SEL["auth_email"])
        email_input.fill(creds["email"])
        password_input = self._page.query_selector(_SEL["auth_password"])
        password_input.fill(creds["password"])

        # Submit
        self._page.click(_SEL["sign_in_submit"])

    # ── Step 3: Multi-step form filling ────────────────────────

    def _click_next(self):
        """Click the Next/Continue button and wait for next page to load."""
        btn = self._page.wait_for_selector(_SEL["next_btn"], timeout=_TIMEOUT)
        btn_text = btn.inner_text().strip().lower()
        btn.click()
        self._page.wait_for_load_state("domcontentloaded")
        return btn_text

    def _fill_application(self, job: dict, pdf_path: Path):
        """Walk through the Workday application form in known step order."""

        # Step 1: Resume upload
        self._page.wait_for_selector(_SEL["resume_page"], timeout=_TIMEOUT)
        self._upload_resume(pdf_path)
        self._click_next()

        # Step 2: My Information
        self._page.wait_for_selector(_SEL["first_name"], timeout=_TIMEOUT)
        self._fill_my_information()
        self._click_next()

        # Steps 3+: Remaining pages (My Experience, Questions, Disclosures, Review)
        max_pages = 10
        for page_num in range(max_pages):
            self._page.wait_for_selector(_SEL["next_btn"], timeout=_TIMEOUT)

            if self._detect_questions():
                self._answer_application_questions(job)

            if self._page.query_selector(_SEL["gender_dropdown"]):
                self._handle_self_identification()

            btn_text = self._click_next()

            if "submit" in btn_text:
                logger.info("Application submitted")
                self._page.wait_for_load_state("networkidle")
                return

        raise WorkdayApplyError(f"Exceeded max pages ({max_pages}) without submitting")

    def _fill_my_information(self):
        """Fill the My Information page (name, contact, address)."""
        logger.info("Filling My Information page")

        self._fill_if_empty(_SEL["first_name"], self._personal.get("first_name", ""))
        self._fill_if_empty(_SEL["last_name"], self._personal.get("last_name", ""))
        self._fill_if_empty(_SEL["address_line1"], self._personal.get("address_line1", ""))
        self._fill_if_empty(_SEL["city"], self._personal.get("city", ""))
        self._fill_if_empty(_SEL["postal_code"], self._personal.get("postal_code", ""))

        phone_field = self._page.query_selector(_SEL["phone_number"])
        if phone_field:
            current = phone_field.input_value()
            if not current.strip():
                phone_type = self._page.query_selector(_SEL["phone_device"])
                if phone_type:
                    phone_type.select_option(label="Mobile")
                self._fill_field(_SEL["phone_number"], self._personal.get("phone", ""))

    def _upload_resume(self, pdf_path: Path):
        """Upload the resume PDF file."""
        logger.info("Uploading resume: %s", pdf_path.name)
        self._page.set_input_files(_SEL["resume_upload"], str(pdf_path))
        # Wait for upload to complete (file name appears in the UI)
        self._page.wait_for_selector(_SEL["resume_uploaded"], timeout=_TIMEOUT)

    def _handle_self_identification(self):
        """Handle voluntary self-identification page (decline all)."""
        logger.info("Handling self-identification page (declining)")
        dropdowns = self._page.query_selector_all('select[data-automation-id]')
        for dropdown in dropdowns:
            options = dropdown.query_selector_all("option")
            for opt in options:
                text = opt.inner_text().lower()
                if "decline" in text or "prefer not" in text or "choose not" in text:
                    dropdown.select_option(label=opt.inner_text())
                    break

    # ── Screening Questions ────────────────────────────────────

    def _detect_questions(self) -> bool:
        """Check if the current page has application questions."""
        questions = self._page.query_selector_all(
            '[data-automation-id*="questionContainer"], '
            '[data-automation-id*="formField"], '
            'div[data-automation-id] label'
        )
        return len(questions) > 0 and not self._page.query_selector(_SEL["first_name"])

    def _answer_application_questions(self, job: dict):
        """Extract and answer all screening questions on the current page."""
        logger.info("Answering application questions")
        question_groups = self._extract_question_groups()
        for label_text, input_el, input_type, options in question_groups:
            answer = self._get_answer(label_text, input_type, options, job)
            self._fill_answer(input_el, input_type, answer, options)

    def _extract_question_groups(self) -> list[tuple]:
        """Scrape the page for question labels and their associated inputs.

        Returns:
            List of (label_text, input_element, input_type, options).
        """
        groups = []

        # Select/dropdown questions
        for select_el in self._page.query_selector_all("select[data-automation-id]"):
            aid = select_el.get_attribute("data-automation-id") or ""
            if any(skip in aid for skip in ["country", "phone-device", "gender", "state"]):
                continue
            label = self._find_label_for(select_el)
            if label:
                options = [opt.inner_text() for opt in select_el.query_selector_all("option")
                           if opt.get_attribute("value")]
                groups.append((label, select_el, "select", options))

        # Text input questions
        for input_el in self._page.query_selector_all("input[data-automation-id][type='text']"):
            aid = input_el.get_attribute("data-automation-id") or ""
            if any(skip in aid for skip in [
                "legalName", "address", "city", "postal", "phone", "email",
                "signIn", "createAccount", "file-upload"
            ]):
                continue
            label = self._find_label_for(input_el)
            if label:
                groups.append((label, input_el, "text", []))

        # Textarea questions
        for ta_el in self._page.query_selector_all("textarea[data-automation-id]"):
            label = self._find_label_for(ta_el)
            if label:
                groups.append((label, ta_el, "textarea", []))

        # Radio button groups
        seen_groups = set()
        for radio_el in self._page.query_selector_all("input[type='radio']"):
            name = radio_el.get_attribute("name") or ""
            if name in seen_groups:
                continue
            seen_groups.add(name)
            label = self._find_label_for(radio_el)
            if label:
                radio_options = []
                for r in self._page.query_selector_all(f"input[name='{name}']"):
                    r_label = r.evaluate("el => el.parentElement?.textContent?.trim()")
                    if r_label:
                        radio_options.append(r_label)
                groups.append((label, radio_el, "radio", radio_options))

        return groups

    def _find_label_for(self, element) -> str:
        """Find the label text associated with a form element."""
        aria = element.get_attribute("aria-label")
        if aria and len(aria) > 3:
            return aria.strip()

        el_id = element.get_attribute("id")
        if el_id:
            label = self._page.query_selector(f'label[for="{el_id}"]')
            if label:
                return label.inner_text().strip()

        label_text = element.evaluate(
            """el => {
                let parent = el.closest('[data-automation-id]');
                if (parent) {
                    let label = parent.querySelector('label');
                    if (label) return label.textContent.trim();
                }
                return '';
            }"""
        )
        return label_text

    def _get_answer(self, question: str, input_type: str,
                    options: list[str], job: dict) -> str:
        """Determine the answer: Q&A lookup first, then LLM fallback."""
        normalized = _normalize_question(question)

        if normalized in self._qa:
            answer = self._qa[normalized]
            logger.info("  Q&A match: '%s' → '%s'", question[:60], answer)
            return answer

        logger.info("  LLM fallback for: '%s'", question[:80])
        return self._ask_llm(question, input_type, options, job)

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

    def _fill_answer(self, element, input_type: str, answer: str, options: list[str]):
        """Fill an answer into the appropriate input element."""
        if input_type == "select":
            best_option = self._match_option(answer, options)
            element.select_option(label=best_option or answer)

        elif input_type == "radio":
            name = element.get_attribute("name") or ""
            for radio in self._page.query_selector_all(f"input[name='{name}']"):
                radio_text = radio.evaluate("el => el.parentElement?.textContent?.trim()") or ""
                if _normalize_question(radio_text) == _normalize_question(answer):
                    radio.click()
                    break
            else:
                for radio in self._page.query_selector_all(f"input[name='{name}']"):
                    radio_text = radio.evaluate("el => el.parentElement?.textContent?.trim()") or ""
                    if answer.lower() in radio_text.lower():
                        radio.click()
                        break

        else:  # text or textarea
            element.fill(answer)

    @staticmethod
    def _match_option(answer: str, options: list[str]) -> str | None:
        """Find the best matching option from a list (case-insensitive)."""
        answer_lower = answer.lower().strip()
        for opt in options:
            if opt.lower().strip() == answer_lower:
                return opt
        for opt in options:
            if answer_lower in opt.lower() or opt.lower() in answer_lower:
                return opt
        return None

    # ── Utility helpers ────────────────────────────────────────

    def _fill_field(self, selector: str, value: str):
        """Wait for a field, clear it, and type a value."""
        el = self._page.wait_for_selector(selector, timeout=_TIMEOUT)
        if el:
            el.fill(value)

    def _fill_if_empty(self, selector: str, value: str):
        """Fill a field only if it's currently empty."""
        el = self._page.query_selector(selector)
        if el:
            current = el.input_value()
            if not current.strip():
                el.fill(value)
