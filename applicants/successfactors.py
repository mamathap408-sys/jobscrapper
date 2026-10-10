"""
applicants/successfactors.py — SuccessFactors Auto-Apply Engine
================================================================
Automates job application submission on SAP SuccessFactors career portals
(HCLTech, Wipro, Capgemini, etc.) using Playwright.

Flow per job:
  1. Navigate to job apply URL → handle CAS SSO login
  2. Upload resume PDF
  3. Fill profile fields (parsed/verified from resume)
  4. Answer screening questions (Q&A lookup → LLM fallback)
  5. Review & submit

Progress tracking:
  - Each stage reports progress_pct (0–100) and stage name to DB
  - On failure, progress shows exactly how far we got
  - Retries can skip completed stages (future enhancement)

Supported portals (same SAP SuccessFactors platform):
  - careers.hcltech.com (HCLPRD, career55.sapsf.eu)
  - careers.wipro.com
  - careers.capgemini.com
"""

import json
import logging
import re
import time
from enum import IntEnum
from pathlib import Path
from urllib.parse import urlparse, urlencode

from playwright.sync_api import sync_playwright, Page, TimeoutError as PlaywrightTimeout

from services.genai_client import GenAIClient

logger = logging.getLogger(__name__)

# Default timeout for waiting on elements (ms)
_TIMEOUT = 30_000
_SHORT_TIMEOUT = 10_000


# ── Progress Stages ─────────────────────────────────────────────
class Stage(IntEnum):
    """Application progress stages with percentage values."""
    INIT = 0
    NAVIGATED = 10
    AUTHENTICATED = 20
    FORM_LOADED = 30
    RESUME_UPLOADED = 40
    PROFILE_FILLED = 50
    WORK_HISTORY = 60
    EDUCATION = 70
    SCREENING_QS = 80
    REVIEW = 90
    SUBMITTED = 100


STAGE_NAMES = {
    Stage.INIT: "initializing",
    Stage.NAVIGATED: "navigated_to_job",
    Stage.AUTHENTICATED: "authenticated",
    Stage.FORM_LOADED: "form_loaded",
    Stage.RESUME_UPLOADED: "resume_uploaded",
    Stage.PROFILE_FILLED: "profile_filled",
    Stage.WORK_HISTORY: "work_history_filled",
    Stage.EDUCATION: "education_filled",
    Stage.SCREENING_QS: "screening_questions_answered",
    Stage.REVIEW: "review_page",
    Stage.SUBMITTED: "submitted",
}


class SFApplyError(Exception):
    """Raised when a SuccessFactors application step fails."""


class SuccessFactorsApplicant:
    """Automates SuccessFactors job application form submission.

    Tracks progress through stages so failures are easily debuggable
    and retryable.
    """

    def __init__(self, config: dict, answers: dict, answers_raw: str = "", resume_text: str = ""):
        self._apply_cfg = config.get("apply", {})
        self._genai_cfg = config.get("genai", {})
        self._answers = answers
        self._answers_raw = answers_raw
        self._work_exp = answers.get("work_experience", [])
        self._education = answers.get("education", {})
        self._languages = answers.get("languages", [])
        self._confidence_threshold = self._apply_cfg.get("answer_confidence_threshold", 7)
        self._test_mode = self._apply_cfg.get("test_mode", False)

        # SF-specific credentials (loaded from answers or config)
        self._sf_credentials = answers.get("sf_account", {})

        # Runtime state
        self._pw = None
        self._browser = None
        self._context = None
        self._page: Page | None = None
        self._genai: GenAIClient | None = None
        self._current_progress = Stage.INIT
        self._progress_callback = None  # Set by caller to persist progress
        self._answer_reasoning: str = ""

    # ── Progress tracking ──────────────────────────────────────

    def _set_progress(self, stage: Stage):
        """Update current progress stage and notify callback."""
        self._current_progress = stage
        stage_name = STAGE_NAMES[stage]
        logger.info("╔══ PROGRESS: %d%% — %s ══╗", int(stage), stage_name)
        if self._progress_callback:
            self._progress_callback(int(stage), stage_name)

    # ── Lifecycle ──────────────────────────────────────────────

    def start(self):
        """Launch the Playwright browser."""
        headless = self._apply_cfg.get("headless", False)
        logger.info("Launching SuccessFactors browser (headless=%s)", headless)
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(headless=headless)
        self._context = self._browser.new_context()
        self._page = self._context.new_page()
        self._genai = GenAIClient(self._genai_cfg)
        logger.info("Browser launched successfully")

    def close(self):
        """Shut down browser and GenAI client."""
        if self._genai:
            self._genai.close()
        if self._browser:
            self._browser.close()
        if self._pw:
            self._pw.stop()
        logger.info("Browser closed")

    def is_job_valid(self, job_url: str) -> tuple[bool, int]:
        """Check if a SuccessFactors job is still active.

        Fetches the job page and checks for "not available" messaging.
        Returns (is_valid, status_code).
        """
        import httpx
        try:
            with httpx.Client(timeout=15, verify=False, follow_redirects=True) as client:
                resp = client.get(job_url)
                if resp.status_code == 404:
                    logger.info("Job expired (HTTP 404): %s", job_url)
                    return False, 404
                if resp.status_code != 200:
                    logger.warning("Job check inconclusive (HTTP %d): %s", resp.status_code, job_url)
                    return True, resp.status_code
                # Check if page says job is unavailable
                if "can't view this job" in resp.text.lower() or "not available" in resp.text.lower():
                    logger.info("Job expired (page says unavailable): %s", job_url)
                    return False, 200
                return True, 200
        except httpx.HTTPError as e:
            logger.warning("Job validity check failed for %s: %s", job_url, e)
            return True, 0

    # ── Main orchestration ─────────────────────────────────────

    def apply(self, job: dict, pdf_path: Path, resume_data: dict) -> bool:
        """Apply to a single SuccessFactors job.

        Args:
            job:         Dict from DB with keys: job_id, url, title, company,
                         job_description, resume_name.
            pdf_path:    Path to the resume PDF to upload.
            resume_data: Parsed resume data from resume_parser.

        Raises:
            SFApplyError: On any application step failure.
        """
        job_url = job["url"]
        job_id = job.get("job_id", "")
        logger.info("═" * 70)
        logger.info("APPLYING TO: %s at %s", job["title"], job["company"])
        logger.info("URL: %s", job_url)
        logger.info("Resume: %s", pdf_path.name)
        logger.info("═" * 70)

        self._set_progress(Stage.INIT)

        try:
            # Stage 1: Navigate to job apply page
            self._navigate_to_apply(job_url, job_id)
            self._set_progress(Stage.NAVIGATED)

            # Stage 2: Handle authentication
            self._handle_auth()
            self._set_progress(Stage.AUTHENTICATED)

            # Stage 3: Wait for application form to load
            self._wait_for_form()
            self._set_progress(Stage.FORM_LOADED)

            # Stage 4: Upload resume
            self._upload_resume(pdf_path)
            self._set_progress(Stage.RESUME_UPLOADED)

            # Stage 5: Fill profile/personal info
            self._fill_profile()
            self._set_progress(Stage.PROFILE_FILLED)

            # Stage 6: Fill work history
            self._fill_work_history(resume_data)
            self._set_progress(Stage.WORK_HISTORY)

            # Stage 7: Fill education
            self._fill_education()
            self._set_progress(Stage.EDUCATION)

            # Stage 8: Answer screening questions
            self._fill_screening_questions(job)
            self._set_progress(Stage.SCREENING_QS)

            # Stage 9: Review
            self._review_and_submit(job)
            self._set_progress(Stage.REVIEW)

            # Stage 10: Submit
            if self._test_mode:
                raise SFApplyError("TEST MODE — stopping before final submit")
            self._submit()
            self._set_progress(Stage.SUBMITTED)

            logger.info("✓ Successfully applied to: %s at %s", job["title"], job["company"])
            return True

        except PlaywrightTimeout as e:
            raise SFApplyError(f"Timeout waiting for element: {e}") from e

    # ── Stage 1: Navigate ──────────────────────────────────────

    def _navigate_to_apply(self, job_url: str, job_id: str):
        """Navigate to the job's apply page.

        SuccessFactors apply URL pattern: /talentcommunity/apply/{jobId}/?locale=en_US
        """
        # Extract the base URL and job ID from the job URL
        parsed = urlparse(job_url)
        base_url = f"{parsed.scheme}://{parsed.hostname}"

        # Extract numeric job ID from URL (e.g., /job/Title/124898-en_US → 124898)
        match = re.search(r"/(\d+)-en_US", job_url)
        if match:
            sf_job_id = match.group(1)
        else:
            # Fallback: use the job_id from DB
            sf_job_id = job_id

        apply_url = f"{base_url}/talentcommunity/apply/{sf_job_id}/?locale=en_US"
        logger.info("[navigate] Going to apply URL: %s", apply_url)
        logger.info("[navigate] Base URL: %s, SF Job ID: %s", base_url, sf_job_id)

        self._page.goto(apply_url, wait_until="networkidle", timeout=_TIMEOUT)
        logger.info("[navigate] Page loaded. Title: %s", self._page.title())
        logger.info("[navigate] Current URL: %s", self._page.url)

        # Check if we got redirected to homepage (means auth required)
        current = self._page.url
        if "/talentcommunity/apply" not in current and "/apply" not in current:
            logger.info("[navigate] Redirected away from apply page — likely needs auth")
            # Try navigating to the job page first, then click Apply
            logger.info("[navigate] Trying job page first: %s", job_url)
            self._page.goto(job_url, wait_until="networkidle", timeout=_TIMEOUT)
            logger.info("[navigate] Job page loaded. Title: %s", self._page.title())

            # Click Apply Now button
            apply_btn = self._page.query_selector(".unify-apply-now, .dialogApplyBtn, .applylink a")
            if apply_btn:
                logger.info("[navigate] Found Apply button — clicking")
                # Apply click often triggers navigation to SSO page
                # Use expect_navigation or just wait for URL change
                with self._page.expect_navigation(timeout=_TIMEOUT, wait_until="networkidle"):
                    apply_btn.click()
                time.sleep(2)
                logger.info("[navigate] After Apply click. URL: %s", self._page.url)
            else:
                logger.warning("[navigate] No Apply button found on job page")
                self._screenshot("no_apply_button")
                raise SFApplyError("Could not find Apply button on job page")

    # ── Stage 2: Authentication ────────────────────────────────

    def _handle_auth(self):
        """Handle CAS SSO authentication if required.

        SuccessFactors uses career55.sapsf.eu for external candidate auth.
        Flow: Sign In → if invalid → Register new account.
        """
        current_url = self._page.url
        logger.info("[auth] Current URL: %s", current_url)

        # Dismiss cookie consent banner if present
        self._dismiss_cookie_banner()

        # Check if we're on a login/SSO page
        is_sso_page = "sapsf.eu" in current_url or "login" in current_url.lower()
        has_login_form = self._page.query_selector(
            'input[type="email"], input[name="email"], input[name="username"], '
            '#loginForm, .login-form, [data-testid="email-input"]'
        )

        if not is_sso_page and not has_login_form:
            logger.info("[auth] No login required — already authenticated or no auth gate")
            return

        logger.info("[auth] Authentication required. SSO page detected: %s", is_sso_page)
        logger.info("[auth] Page title: %s", self._page.title())

        # Log all visible form elements for debugging
        inputs = self._page.query_selector_all("input:visible")
        for inp in inputs:
            inp_type = inp.get_attribute("type") or "text"
            inp_name = inp.get_attribute("name") or inp.get_attribute("id") or "unknown"
            logger.info("[auth] Found input: type=%s name=%s", inp_type, inp_name)

        # Try auto-login with credentials
        if self._sf_credentials:
            email = self._sf_credentials.get("email", "")
            password = self._sf_credentials.get("password", "")
            if email and password:
                logger.info("[auth] Attempting auto-login with stored credentials")
                login_success = self._auto_login(email, password)
                if login_success:
                    return
                # Login failed — try to register
                logger.info("[auth] Login failed — attempting account registration")
                self._register_account(email, password)
                return

        # No credentials — prompt for manual login
        logger.warning("[auth] No SF credentials configured — manual login required")
        logger.warning("[auth] Add sf_account.email and sf_account.password to workday_answers.yaml")
        input("[auth] Press Enter after you have logged in manually...")
        self._page.wait_for_load_state("networkidle", timeout=_TIMEOUT)

    def _dismiss_cookie_banner(self):
        """Dismiss cookie consent banner if present (OneTrust overlay)."""
        # OneTrust uses a specific button ID — try that first with force click
        btn = self._page.query_selector('#onetrust-accept-btn-handler')
        if btn:
            logger.info("[auth] Dismissing OneTrust cookie banner (force click)")
            btn.click(force=True)
            time.sleep(1)
            # Wait for the overlay to disappear
            try:
                self._page.wait_for_selector('#onetrust-consent-sdk', state="hidden", timeout=5000)
            except PlaywrightTimeout:
                # Try to hide it via JS as fallback
                self._page.evaluate("document.getElementById('onetrust-consent-sdk')?.remove()")
            logger.info("[auth] Cookie banner dismissed")
            return

        # Generic fallback
        cookie_selectors = [
            'button:has-text("Accept All Cookies")',
            'button:has-text("Accept All")',
            'button:has-text("Reject All Cookies")',
        ]
        for sel in cookie_selectors:
            btn = self._page.query_selector(sel)
            if btn and btn.is_visible():
                logger.info("[auth] Dismissing cookie banner: '%s'", btn.inner_text().strip()[:30])
                btn.click(force=True)
                time.sleep(1)
                return
        logger.info("[auth] No cookie banner found")

    def _auto_login(self, email: str, password: str) -> bool:
        """Attempt automated login on the SSO page.

        Returns True if login succeeded, False if credentials were rejected.
        """
        logger.info("[auth] Looking for email field...")

        # Try various email field selectors
        email_selectors = [
            'input[name="username"]',
            'input[type="email"]',
            'input[name="email"]',
            'input[id="email"]',
            'input[id="username"]',
            'input[placeholder*="email" i]',
        ]

        email_field = None
        for sel in email_selectors:
            email_field = self._page.query_selector(sel)
            if email_field:
                logger.info("[auth] Found email field with selector: %s", sel)
                break

        if not email_field:
            logger.warning("[auth] Could not find email field")
            self._screenshot("no_email_field")
            raise SFApplyError("Cannot find email input on login page")

        # Fill email
        email_field.click()
        email_field.fill(email)
        logger.info("[auth] Email filled: %s", email[:3] + "***")

        # Look for password field
        password_selectors = [
            'input[type="password"]',
            'input[name="password"]',
            'input[id="password"]',
        ]

        password_field = None
        for sel in password_selectors:
            password_field = self._page.query_selector(sel)
            if password_field:
                logger.info("[auth] Found password field with selector: %s", sel)
                break

        if not password_field:
            logger.warning("[auth] Could not find password field")
            self._screenshot("no_password_field")
            raise SFApplyError("Cannot find password input on login page")

        # Fill password
        password_field.click()
        password_field.fill(password)
        logger.info("[auth] Password filled")

        # Submit login
        submit_btn = self._page.query_selector(
            'button:has-text("Sign In"), input[value="Sign In"], '
            'button[type="submit"], input[type="submit"]'
        )
        if submit_btn:
            logger.info("[auth] Clicking Sign In button")
            submit_btn.click()
        else:
            logger.info("[auth] No submit button — pressing Enter")
            password_field.press("Enter")

        # Wait for response
        self._page.wait_for_load_state("networkidle", timeout=_TIMEOUT)
        time.sleep(3)
        logger.info("[auth] After login. URL: %s", self._page.url)
        logger.info("[auth] Page title: %s", self._page.title())

        # Check for error message (invalid credentials)
        # SF shows errors in a specific banner with class or role=alert
        error_el = self._page.query_selector(
            '.alert-danger, .error-message, .message-error, '
            '[role="alert"]:not(#onetrust-consent-sdk *), '
            '.loginError, .errorMsg'
        )
        if error_el and error_el.is_visible():
            error_text = error_el.inner_text().strip()
            # Filter out false positives (cookie banners, etc.)
            if "invalid" in error_text.lower() or "incorrect" in error_text.lower() or "re-enter" in error_text.lower():
                logger.warning("[auth] Login error detected: '%s'", error_text[:100])
                return False

        # Check if still on login page
        if "sapsf.eu" in self._page.url and "career" in self._page.url:
            # Could still be on login page — check for username field
            still_login = self._page.query_selector('input[name="username"]')
            if still_login and still_login.is_visible():
                logger.warning("[auth] Still on login page — credentials rejected")
                return False

        logger.info("[auth] Login appears successful")
        return True

    def _register_account(self, email: str, password: str):
        """Register a new candidate account on SuccessFactors."""
        logger.info("[auth] Looking for registration link...")

        # Look for "Not a registered user yet?" or "Create Account" link
        register_selectors = [
            'a:has-text("Register")',
            'a:has-text("Create")',
            'a:has-text("Sign Up")',
            'a:has-text("register")',
            'a[href*="register"]',
            'a[href*="Register"]',
        ]

        register_link = None
        for sel in register_selectors:
            register_link = self._page.query_selector(sel)
            if register_link and register_link.is_visible():
                logger.info("[auth] Found registration link: '%s'", register_link.inner_text().strip()[:30])
                break

        if not register_link:
            # Try to find it in the page text
            page_text = self._page.inner_text("body")
            if "not a registered user" in page_text.lower():
                # Find the link near that text
                register_link = self._page.query_selector(
                    'a[href*="registration"], a[href*="createAccount"]'
                )

        if not register_link:
            logger.error("[auth] No registration link found")
            self._screenshot("no_register_link")
            raise SFApplyError("Login failed and no registration option found")

        logger.info("[auth] Clicking registration link")
        register_link.click()
        self._page.wait_for_load_state("networkidle", timeout=_TIMEOUT)
        time.sleep(2)
        logger.info("[auth] Registration page. URL: %s", self._page.url)
        logger.info("[auth] Registration page title: %s", self._page.title())

        # Log all visible fields on registration page
        inputs = self._page.query_selector_all("input:visible")
        for inp in inputs:
            inp_type = inp.get_attribute("type") or "text"
            inp_name = inp.get_attribute("name") or inp.get_attribute("id") or "unknown"
            logger.info("[auth] Registration field: type=%s name=%s", inp_type, inp_name)

        # Fill registration form using SF-specific field names (fbclc_ prefix)
        first_name = self._sf_credentials.get("first_name", "")
        last_name = self._sf_credentials.get("last_name", "")

        # SF registration fields use fbclc_ prefix:
        # fbclc_userName (email), fbclc_emailConf, fbclc_pwd, fbclc_pwdConf, fbclc_fName, fbclc_lName
        sf_fields = {
            'input[name="fbclc_userName"]': email,
            'input[name="fbclc_emailConf"]': email,
            'input[name="fbclc_pwd"]': password,
            'input[name="fbclc_pwdConf"]': password,
            'input[name="fbclc_fName"]': first_name,
            'input[name="fbclc_lName"]': last_name,
        }

        for selector, value in sf_fields.items():
            field = self._page.query_selector(selector)
            if field:
                field.fill(value)
                field_name = selector.split('"')[1]
                display_val = value if "pwd" not in field_name.lower() else "***"
                logger.info("[auth] Filled %s → '%s'", field_name, display_val)
            else:
                logger.warning("[auth] Field not found: %s", selector)

        # Fill Country/Region dropdown (required)
        country_select = self._page.query_selector('select[name="fbclc_country"], select[name*="country"]')
        if country_select:
            try:
                country_select.select_option("India")
                logger.info("[auth] Filled Country → 'India'")
            except Exception:
                # Try label-based selection
                try:
                    country_select.select_option(label="India")
                    logger.info("[auth] Filled Country → 'India' (by label)")
                except Exception as e:
                    logger.warning("[auth] Could not select Country: %s", e)

        # Accept Disclaimer (required) — click the link/checkbox
        disclaimer_link = self._page.query_selector(
            'a:has-text("Please read and accept"), a:has-text("declaration"), '
            'a:has-text("disclaimer"), input[name*="disclaimer"], '
            'input[name*="Disclaimer"]'
        )
        if disclaimer_link:
            logger.info("[auth] Clicking disclaimer acceptance")
            disclaimer_link.click()
            time.sleep(2)
            # May open a popup/modal — look for Accept button
            accept_btn = self._page.query_selector(
                'button:has-text("Accept"), button:has-text("I Accept"), '
                'button:has-text("Agree"), button:has-text("OK"), button:has-text("Close")'
            )
            if accept_btn:
                logger.info("[auth] Clicking Accept in disclaimer popup")
                accept_btn.click(force=True)
                time.sleep(1)
        else:
            # Try checkbox variant
            disclaimer_cb = self._page.query_selector(
                'input[type="checkbox"][name*="disclaim"], '
                'input[type="checkbox"][name*="Disclaim"], '
                'input[type="checkbox"][name*="agree"]'
            )
            if disclaimer_cb and not disclaimer_cb.is_checked():
                disclaimer_cb.click()
                logger.info("[auth] Checked disclaimer checkbox")

        # Fallback for non-SF registration forms
        if not self._page.query_selector('input[name="fbclc_userName"]'):
            logger.info("[auth] Non-SF form — trying generic field names")
            fn_field = self._page.query_selector('input[name*="firstName"], input[name*="first_name"]')
            if fn_field:
                fn_field.fill(first_name)
            ln_field = self._page.query_selector('input[name*="lastName"], input[name*="last_name"]')
            if ln_field:
                ln_field.fill(last_name)
            email_field = self._page.query_selector('input[type="email"], input[name*="email"]')
            if email_field:
                email_field.fill(email)
            pw_fields = self._page.query_selector_all('input[type="password"]:visible')
            for pw in pw_fields:
                pw.fill(password)

        # Dismiss cookie banner again (may reappear on registration page)
        self._dismiss_cookie_banner()

        # Take screenshot before submitting registration
        self._screenshot("registration_filled")

        # Submit registration — click "Create Account"
        submit_btn = self._page.query_selector(
            'button:has-text("Create Account"), button:has-text("Register"), '
            'button:has-text("Create"), button:has-text("Sign Up"), '
            'button[type="submit"], input[type="submit"]'
        )
        if submit_btn:
            btn_text = submit_btn.inner_text().strip()
            logger.info("[auth] Submitting registration — clicking '%s'", btn_text)
            submit_btn.click(force=True)  # force in case overlay still partially blocking
            self._page.wait_for_load_state("networkidle", timeout=60_000)
            time.sleep(5)
            logger.info("[auth] After registration. URL: %s", self._page.url)
            logger.info("[auth] After registration. Title: %s", self._page.title())

            # Check for errors (email already exists, validation errors, etc.)
            page_text = self._page.inner_text("body").lower()
            if "already" in page_text and ("registered" in page_text or "exists" in page_text or "account" in page_text):
                logger.error("[auth] Account already exists for this email")
                self._screenshot("account_exists")
                raise SFApplyError("Account already exists — credentials in yaml may be wrong. Try password reset.")

            # Check if still on registration page
            if "create an account" in self._page.title().lower() or "register" in self._page.title().lower():
                logger.warning("[auth] Still on registration page — checking for validation errors")
                self._screenshot("registration_still_here")
                # Look for validation error text
                errors = self._page.query_selector_all('.error, .field-error, [class*="error"]')
                for err in errors:
                    if err.is_visible():
                        logger.error("[auth] Validation error: '%s'", err.inner_text().strip()[:100])
                raise SFApplyError("Registration failed — form validation errors (see screenshots/logs)")
        else:
            logger.error("[auth] No registration submit button found")
            self._screenshot("no_register_submit")
            raise SFApplyError("Could not find registration submit button")

    # ── Stage 3: Wait for form ─────────────────────────────────

    def _wait_for_form(self):
        """Wait for the application form to fully render."""
        logger.info("[form] Waiting for application form to load...")
        logger.info("[form] Current URL: %s", self._page.url)

        # Wait for page to stabilize
        self._page.wait_for_load_state("networkidle", timeout=_TIMEOUT)
        time.sleep(2)

        # Log what we see on the page
        logger.info("[form] Page title: %s", self._page.title())

        # Look for common form indicators
        form_indicators = [
            ("form", "form"),
            ("input[type='file']", "file_upload"),
            ("input[type='text']", "text_input"),
            ("textarea", "textarea"),
            ("select", "select_dropdown"),
            (".wizard-step, .step-indicator, .progress-bar", "wizard_steps"),
            ("button:has-text('Next'), button:has-text('Continue'), button:has-text('Submit')", "nav_buttons"),
        ]

        for selector, name in form_indicators:
            elements = self._page.query_selector_all(selector)
            if elements:
                logger.info("[form] Found %d %s element(s)", len(elements), name)

        # Log all visible buttons
        buttons = self._page.query_selector_all("button:visible")
        for btn in buttons[:10]:  # Limit to first 10
            btn_text = btn.inner_text().strip()[:50]
            if btn_text:
                logger.info("[form] Button: '%s'", btn_text)

    # ── Stage 4: Resume upload ─────────────────────────────────

    def _upload_resume(self, pdf_path: Path):
        """Upload the resume PDF to the application form."""
        logger.info("[resume] Looking for file upload field...")

        # Try various file input selectors
        file_selectors = [
            'input[type="file"]',
            'input[accept*="pdf"]',
            'input[accept*=".pdf"]',
            '[data-testid="file-upload"] input',
        ]

        file_input = None
        for sel in file_selectors:
            file_input = self._page.query_selector(sel)
            if file_input:
                logger.info("[resume] Found file input with selector: %s", sel)
                break

        if not file_input:
            # Maybe need to click an "Upload" button first to reveal the input
            upload_btns = self._page.query_selector_all(
                'button:has-text("Upload"), button:has-text("Attach"), '
                'a:has-text("Upload"), label:has-text("Upload")'
            )
            if upload_btns:
                logger.info("[resume] Clicking upload button to reveal file input")
                upload_btns[0].click()
                time.sleep(2)
                for sel in file_selectors:
                    file_input = self._page.query_selector(sel)
                    if file_input:
                        break

        if not file_input:
            logger.warning("[resume] No file upload field found — may not be on this page")
            self._screenshot("no_file_upload")
            # Not all portals require upload on a separate step
            return

        logger.info("[resume] Uploading: %s (%d bytes)", pdf_path.name, pdf_path.stat().st_size)
        file_input.set_input_files(str(pdf_path))
        time.sleep(3)  # Wait for upload to process

        # Verify upload succeeded
        logger.info("[resume] Upload initiated — waiting for confirmation")
        self._page.wait_for_load_state("networkidle", timeout=_TIMEOUT)
        logger.info("[resume] Resume upload complete")

    # ── Stage 5: Profile / Personal info ───────────────────────

    def _fill_profile(self):
        """Fill personal information fields (name, email, phone, etc.)."""
        logger.info("[profile] Scanning for profile/personal info fields...")

        # Scan all visible input fields
        fields = self._scan_visible_fields()
        logger.info("[profile] Found %d fillable field(s)", len(fields))

        # Map common field names to answers
        field_map = self._build_field_mapping()

        filled = 0
        for field in fields:
            name = field["name"].lower()
            label = field["label"].lower()

            # Try to match field to known answer
            value = self._match_field_to_answer(name, label, field_map)
            if value:
                logger.info("[profile] Filling '%s' (label: '%s') → '%s'",
                           field["name"], field["label"], str(value)[:30])
                self._fill_field(field, value)
                filled += 1
            else:
                if field.get("required"):
                    logger.warning("[profile] REQUIRED field not mapped: '%s' (label: '%s')",
                                  field["name"], field["label"])
                else:
                    logger.debug("[profile] Skipping optional: '%s'", field["name"])

        logger.info("[profile] Filled %d/%d fields", filled, len(fields))

        # Navigate to next step if there's a Next/Continue button
        self._click_next_if_available()

    # ── Stage 6: Work history ──────────────────────────────────

    def _fill_work_history(self, resume_data: dict):
        """Fill work history section from resume data."""
        logger.info("[work] Filling work history...")

        work_exp = resume_data.get("work_experience", [])
        if not work_exp:
            work_exp = self._work_exp
        logger.info("[work] Have %d work experience entries to fill", len(work_exp))

        # Look for work history section indicators
        work_section = self._page.query_selector(
            '[data-help-id*="work"], [aria-label*="Work"], '
            '[aria-label*="Experience"], h2:has-text("Work"), h3:has-text("Work")'
        )
        if work_section:
            logger.info("[work] Found work history section")
        else:
            logger.info("[work] No dedicated work history section found — may be combined form")

        # Scan fields and try to fill
        fields = self._scan_visible_fields()
        work_fields = [f for f in fields if self._is_work_field(f)]
        logger.info("[work] Found %d work-related fields", len(work_fields))

        # Fill from first work experience entry
        if work_exp and work_fields:
            exp = work_exp[0]
            for field in work_fields:
                value = self._match_work_field(field, exp)
                if value:
                    logger.info("[work] Filling '%s' → '%s'", field["name"], str(value)[:30])
                    self._fill_field(field, value)

        self._click_next_if_available()

    # ── Stage 7: Education ─────────────────────────────────────

    def _fill_education(self):
        """Fill education section from answers."""
        logger.info("[edu] Filling education...")
        logger.info("[edu] Education data: %s", json.dumps(self._education, default=str)[:200])

        fields = self._scan_visible_fields()
        edu_fields = [f for f in fields if self._is_edu_field(f)]
        logger.info("[edu] Found %d education-related fields", len(edu_fields))

        for field in edu_fields:
            value = self._match_edu_field(field)
            if value:
                logger.info("[edu] Filling '%s' → '%s'", field["name"], str(value)[:30])
                self._fill_field(field, value)

        self._click_next_if_available()

    # ── Stage 8: Screening questions ───────────────────────────

    def _fill_screening_questions(self, job: dict):
        """Answer screening/qualification questions using Q&A lookup + LLM fallback."""
        logger.info("[questions] Looking for screening questions...")

        # Look for question-like fields (textareas, radio groups, selects with question labels)
        questions = self._detect_questions()
        if not questions:
            logger.info("[questions] No screening questions found on this page")
            return

        logger.info("[questions] Found %d screening question(s)", len(questions))
        qa_lookup = self._answers.get("qa", {})

        for i, q in enumerate(questions):
            logger.info("[questions] Q%d: '%s' (type: %s, required: %s)",
                       i + 1, q["question_text"][:80], q["input_type"], q.get("required"))

            # Try Q&A lookup first
            answer = self._lookup_qa(q["question_text"], qa_lookup)
            if answer:
                logger.info("[questions] Q%d: matched in Q&A lookup → '%s'", i + 1, str(answer)[:50])
                self._fill_field(q, answer)
                continue

            # LLM fallback
            logger.info("[questions] Q%d: no Q&A match — asking LLM", i + 1)
            answer = self._ask_llm_question(q, job)
            if answer:
                logger.info("[questions] Q%d: LLM answered → '%s'", i + 1, str(answer)[:50])
                self._fill_field(q, answer)
            else:
                logger.warning("[questions] Q%d: LLM gave no answer", i + 1)

        self._click_next_if_available()

    # ── Stage 9: Review ────────────────────────────────────────

    def _review_and_submit(self, job: dict):
        """Review the application before final submission."""
        logger.info("[review] On review/summary page")
        logger.info("[review] Current URL: %s", self._page.url)

        # Log page content for debugging
        page_text = self._page.inner_text("body")[:2000]
        logger.info("[review] Page content preview:\n%s", page_text[:500])

        # Look for submit button
        submit_btn = self._page.query_selector(
            'button:has-text("Submit"), button:has-text("Apply"), '
            'input[type="submit"][value*="Submit"], '
            'button:has-text("Send Application")'
        )
        if submit_btn:
            logger.info("[review] Found Submit button: '%s'", submit_btn.inner_text().strip())
        else:
            logger.info("[review] No Submit button found yet — may need to scroll or continue")

    # ── Stage 10: Final submit ─────────────────────────────────

    def _submit(self):
        """Click the final Submit button."""
        logger.info("[submit] Looking for final submit button...")

        submit_selectors = [
            'button:has-text("Submit Application")',
            'button:has-text("Submit")',
            'button:has-text("Apply Now")',
            'button:has-text("Send Application")',
            'input[type="submit"]',
        ]

        for sel in submit_selectors:
            btn = self._page.query_selector(sel)
            if btn:
                btn_text = btn.inner_text().strip() if btn.inner_text else "submit"
                logger.info("[submit] Clicking: '%s'", btn_text)
                btn.click()
                self._page.wait_for_load_state("networkidle", timeout=_TIMEOUT)
                time.sleep(3)
                logger.info("[submit] After submit. URL: %s", self._page.url)
                logger.info("[submit] Page title: %s", self._page.title())
                return

        logger.error("[submit] No submit button found!")
        self._screenshot("no_submit_button")
        raise SFApplyError("Could not find submit button on final page")

    # ── Helper: Field scanning ─────────────────────────────────

    def _scan_visible_fields(self) -> list[dict]:
        """Scan all visible form fields on the current page.

        Returns list of field descriptors with keys:
          - name: field name/id
          - label: associated label text
          - input_type: text/email/tel/select/textarea/radio/checkbox/file
          - element: Playwright element handle
          - required: bool
          - container: parent element (for context)
        """
        fields = []

        # Text/email/tel/number inputs
        for inp in self._page.query_selector_all("input:visible"):
            inp_type = inp.get_attribute("type") or "text"
            if inp_type in ("hidden", "file", "submit", "button"):
                continue
            name = inp.get_attribute("name") or inp.get_attribute("id") or ""
            label = self._get_label_for_element(inp)
            required = inp.get_attribute("required") is not None or inp.get_attribute("aria-required") == "true"
            fields.append({
                "name": name,
                "label": label,
                "input_type": inp_type,
                "element": inp,
                "required": required,
                "container": inp,
            })

        # Select dropdowns
        for sel in self._page.query_selector_all("select:visible"):
            name = sel.get_attribute("name") or sel.get_attribute("id") or ""
            label = self._get_label_for_element(sel)
            required = sel.get_attribute("required") is not None
            options = [opt.inner_text().strip() for opt in sel.query_selector_all("option")]
            fields.append({
                "name": name,
                "label": label,
                "input_type": "select",
                "element": sel,
                "required": required,
                "container": sel,
                "options": options,
            })

        # Textareas
        for ta in self._page.query_selector_all("textarea:visible"):
            name = ta.get_attribute("name") or ta.get_attribute("id") or ""
            label = self._get_label_for_element(ta)
            required = ta.get_attribute("required") is not None
            fields.append({
                "name": name,
                "label": label,
                "input_type": "textarea",
                "element": ta,
                "required": required,
                "container": ta,
            })

        return fields

    def _get_label_for_element(self, element) -> str:
        """Find the label text associated with a form element."""
        # Try aria-label
        aria = element.get_attribute("aria-label")
        if aria:
            return aria

        # Try aria-labelledby
        labelledby = element.get_attribute("aria-labelledby")
        if labelledby:
            label_el = self._page.query_selector(f"#{labelledby}")
            if label_el:
                return label_el.inner_text().strip()

        # Try associated <label>
        el_id = element.get_attribute("id")
        if el_id:
            label_el = self._page.query_selector(f'label[for="{el_id}"]')
            if label_el:
                return label_el.inner_text().strip()

        # Try parent label
        parent_label = element.evaluate(
            "el => el.closest('label')?.textContent?.trim() || ''"
        )
        if parent_label:
            return parent_label

        # Try placeholder
        placeholder = element.get_attribute("placeholder")
        if placeholder:
            return placeholder

        return ""

    # ── Helper: Field mapping ──────────────────────────────────

    def _build_field_mapping(self) -> dict:
        """Build a mapping from common field name patterns to answer values."""
        acct = self._sf_credentials
        answers = self._answers

        return {
            "first_name": answers.get("legalName--firstName", acct.get("first_name", "")),
            "last_name": answers.get("legalName--lastName", acct.get("last_name", "")),
            "email": acct.get("email", answers.get("email", "")),
            "phone": answers.get("phone", answers.get("phoneNumber", "")),
            "city": answers.get("city", ""),
            "country": answers.get("country", "India"),
            "current_company": answers.get("currentCompany", ""),
            "current_title": answers.get("currentTitle", ""),
            "years_experience": answers.get("yearsExperience", ""),
            "linkedin": answers.get("linkedIn", answers.get("linkedin", "")),
        }

    def _match_field_to_answer(self, name: str, label: str, field_map: dict) -> str | None:
        """Match a field name/label to a known answer."""
        combined = f"{name} {label}".lower()

        patterns = {
            "first_name": ["first.?name", "given.?name", "fname"],
            "last_name": ["last.?name", "family.?name", "surname", "lname"],
            "email": ["email", "e-mail"],
            "phone": ["phone", "mobile", "cell", "telephone"],
            "city": ["city", "town"],
            "country": ["country", "nation"],
            "current_company": ["current.?company", "employer", "organization"],
            "current_title": ["current.?title", "job.?title", "position", "designation"],
            "years_experience": ["years?.?(?:of)?.?experience", "total.?exp"],
            "linkedin": ["linkedin", "linked.?in"],
        }

        for key, pats in patterns.items():
            for pat in pats:
                if re.search(pat, combined):
                    value = field_map.get(key, "")
                    if value:
                        return value
        return None

    def _is_work_field(self, field: dict) -> bool:
        """Check if a field is work-history related."""
        combined = f"{field['name']} {field['label']}".lower()
        work_keywords = ["company", "employer", "job.?title", "position", "role",
                        "start.?date", "end.?date", "currently", "description", "responsibilities"]
        return any(re.search(kw, combined) for kw in work_keywords)

    def _is_edu_field(self, field: dict) -> bool:
        """Check if a field is education-related."""
        combined = f"{field['name']} {field['label']}".lower()
        edu_keywords = ["school", "university", "college", "degree", "major",
                       "field.?of.?study", "graduation", "gpa", "education"]
        return any(re.search(kw, combined) for kw in edu_keywords)

    def _match_work_field(self, field: dict, exp: dict) -> str | None:
        """Match a work field to a work experience entry."""
        combined = f"{field['name']} {field['label']}".lower()
        if re.search(r"company|employer|organization", combined):
            return exp.get("companyName", exp.get("company", ""))
        if re.search(r"title|position|role|designation", combined):
            return exp.get("jobTitle", exp.get("title", ""))
        if re.search(r"description|responsibilities|duties", combined):
            return exp.get("description", "")
        return None

    def _match_edu_field(self, field: dict) -> str | None:
        """Match an education field to education answers."""
        combined = f"{field['name']} {field['label']}".lower()
        if re.search(r"school|university|college|institution", combined):
            return self._education.get("school", "")
        if re.search(r"degree|qualification", combined):
            return self._education.get("degree", "")
        if re.search(r"major|field.?of.?study|specialization", combined):
            return self._education.get("fieldOfStudy", self._education.get("major", ""))
        if re.search(r"gpa|grade|percentage|cgpa", combined):
            return self._education.get("gpa", "")
        return None

    # ── Helper: Fill field ─────────────────────────────────────

    def _fill_field(self, field: dict, value):
        """Fill a field based on its type."""
        element = field["element"]
        input_type = field.get("input_type", "text")

        try:
            if input_type == "select":
                # Select dropdown
                options = field.get("options", [])
                if isinstance(value, list):
                    # Try each candidate in priority order
                    for v in value:
                        if v in options:
                            element.select_option(v)
                            return
                    # Substring match
                    for v in value:
                        for opt in options:
                            if v.lower() in opt.lower():
                                element.select_option(opt)
                                return
                    logger.warning("[fill] No matching option for '%s': %s. Available: %s",
                                  field["name"], value, options[:5])
                else:
                    element.select_option(str(value))
            elif input_type == "textarea":
                element.fill(str(value))
            elif input_type in ("checkbox", "radio"):
                if value and not element.is_checked():
                    element.click()
                elif not value and element.is_checked():
                    element.click()
            else:
                # text, email, tel, number, etc.
                element.fill(str(value))
        except Exception as e:
            logger.warning("[fill] Failed to fill '%s': %s", field.get("name", "?"), e)

    # ── Helper: Questions ──────────────────────────────────────

    def _detect_questions(self) -> list[dict]:
        """Detect screening questions on the current page.

        Returns field descriptors enriched with question_text.
        """
        questions = []
        fields = self._scan_visible_fields()

        for field in fields:
            # Questions typically have longer labels or are textareas/radio groups
            label = field["label"]
            if len(label) > 20 or field["input_type"] in ("textarea", "radio"):
                field["question_text"] = label
                questions.append(field)

        return questions

    def _lookup_qa(self, question_text: str, qa_lookup: dict) -> str | None:
        """Look up a question in the pre-written Q&A answers."""
        # Normalize question text
        normalized = re.sub(r"[^a-z0-9\s]", "", question_text.lower()).strip()

        for key, answer in qa_lookup.items():
            key_norm = re.sub(r"[^a-z0-9\s]", "", key.lower()).strip()
            if key_norm in normalized or normalized in key_norm:
                return answer

        return None

    def _ask_llm_question(self, question: dict, job: dict) -> str | None:
        """Ask LLM to answer a screening question."""
        if not self._genai:
            return None

        prompt = (
            f"You are filling out a job application for '{job.get('title', '')}' "
            f"at '{job.get('company', '')}'. Answer the following screening question "
            f"truthfully and concisely based on the candidate profile below.\n\n"
            f"Question: {question['question_text']}\n"
            f"Input type: {question['input_type']}\n"
        )

        if question.get("options"):
            prompt += f"Available options: {question['options']}\n"

        prompt += f"\nCandidate profile:\n{self._answers_raw[:3000]}\n"
        prompt += "\nRespond with ONLY the answer text (or option value). No explanation."

        try:
            response = self._genai.chat(prompt)
            answer = response.strip().strip('"').strip("'")
            return answer if answer else None
        except Exception as e:
            logger.warning("[questions] LLM failed: %s", e)
            return None

    # ── Helper: Navigation ─────────────────────────────────────

    def _click_next_if_available(self):
        """Click Next/Continue/Save button if present."""
        next_selectors = [
            'button:has-text("Next")',
            'button:has-text("Continue")',
            'button:has-text("Save and Continue")',
            'button:has-text("Save & Continue")',
            'button:has-text("Proceed")',
            'a:has-text("Next")',
        ]

        for sel in next_selectors:
            btn = self._page.query_selector(sel)
            if btn and btn.is_visible():
                btn_text = btn.inner_text().strip()
                # Don't click Submit — that's handled separately
                if "submit" in btn_text.lower():
                    logger.info("[nav] Found Submit button — not clicking yet")
                    return
                logger.info("[nav] Clicking: '%s'", btn_text)
                btn.click()
                self._page.wait_for_load_state("networkidle", timeout=_TIMEOUT)
                time.sleep(2)
                logger.info("[nav] After navigation. URL: %s", self._page.url)
                return

        logger.info("[nav] No Next/Continue button found — may be single-page form")

    # ── Helper: Debug ──────────────────────────────────────────

    def _screenshot(self, name: str):
        """Take a screenshot for debugging."""
        screenshots_dir = Path("data/screenshots")
        screenshots_dir.mkdir(parents=True, exist_ok=True)
        path = screenshots_dir / f"sf_{name}_{int(time.time())}.png"
        try:
            self._page.screenshot(path=str(path))
            logger.info("[debug] Screenshot saved: %s", path)
        except Exception as e:
            logger.warning("[debug] Failed to take screenshot: %s", e)
