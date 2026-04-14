"""
eligibility_prompt.py — LLM Eligibility Check Before Submission
================================================================
Defines the criteria and prompt for reviewing a filled application
before clicking Submit. The LLM reviews the review page HTML and
returns either "eligible" or an explanation of why it's not eligible.
"""

ELIGIBILITY_CRITERIA = """
ELIGIBILITY CRITERIA — The application MUST satisfy ALL of the following to be eligible:

1. PERSONAL INFORMATION
   - Full legal name is filled (first + last name present)
   - Valid email address is present
   - Phone number with country code is present
   - Address is complete (street, city, postal code, country)

2. WORK EXPERIENCE
   - At least one work experience entry is present
   - Job title, company name, and dates are filled
   - Role description is not empty

3. EDUCATION
   - At least one education entry is present
   - School/university name, degree, and field of study are filled

4. RESUME
   - A resume/CV file has been uploaded (file name visible)

5. APPLICATION QUESTIONS
   - All required questions (marked with *) have responses
   - No response is blank or "No Response" for required fields
   - Salary/compensation answer is a reasonable numeric value (not 0, not absurdly low)
   - Yes/No questions have definitive answers (not left unanswered)

6. VOLUNTARY DISCLOSURES
   - Terms and conditions checkbox is marked "Yes"
   - Gender field is filled (if required)
   - Citizenship/nationality fields are filled (if required)

7. NO DUPLICATES
   - No duplicate work experience entries (same company + same job title appearing more than once)
   - No duplicate education entries (same school + same degree appearing more than once)
   - No duplicate language entries
   - No duplicate skills listed
   - In general, no section should have repeated/duplicate entries

8. CONSISTENCY CHECKS
   - Work experience dates don't overlap impossibly
   - Education dates are reasonable (not in the future)
   - The candidate's current work status answer is consistent with work history
     (e.g., if "I currently work here" is "No" for all entries, should not claim current employment elsewhere)

9. MISSING ANSWERS (compare application vs candidate answers)
   - If the candidate's answers/profile has data for a field but the application shows "No Response" or is blank, flag it
   - Specifically check: if answers.yaml has a value for a field that appears in the application but wasn't filled, that's a failure
   - Examples:
     * If candidate has phone number in their profile but Phone section shows "No Response", flag it
     * If candidate has gender in their profile but Gender field shows "No Response", flag it
     * If candidate has citizenship/nationality in answers but those fields are blank, flag it

10. RED FLAGS (auto-reject if any found)
   - Any field contains placeholder text like "TODO", "FILL THIS", "N/A" for required fields
   - Salary is unreasonably low (below 100000 for India roles) or absurdly high
   - Visa sponsorship answer contradicts citizenship (e.g., Indian citizen applying in India should not need sponsorship)
   - Any answer contradicts the candidate's profile (wrong name, wrong company, etc.)
"""

ELIGIBILITY_PROMPT_TEMPLATE = """You are reviewing a job application before final submission. Your job is to determine if this application is ELIGIBLE to be submitted.

{criteria}

CANDIDATE PROFILE & ANSWERS (this is the source of truth for who the candidate is):
{answers_content}

WORKDAY FORM FIELD MAPPING (used to fill form fields — arrays mean ANY value in the list is acceptable):
{workday_answers_content}

IMPORTANT RULES FOR MATCHING:
- When a field in the workday mapping has an ARRAY of values (e.g. degree: ["Bachelor of Technology", "B.Tech", "B.S."]),
  the application may use ANY ONE of those values. All are equally valid — do NOT flag as mismatch.
- Education school/degree/field values in the application should be matched against the workday mapping arrays, NOT the profile.
- GPA/gradeAverage in the workday mapping is the authoritative value for education GPA.
- Optional fields showing "No Response" are fine if the candidate has no answer for them.
  Only flag "No Response" if the candidate explicitly has a value for that field in their answers.
- The armed forces/veteran/military section is OPTIONAL — "No Response" is acceptable.

JOB BEING APPLIED TO:
  Title: {job_title}
  Company: {job_company}
  Description: {job_description}

APPLICATION REVIEW PAGE CONTENT (what was actually filled in the form):
{review_content}

INSTRUCTIONS:
- Compare the filled application against the candidate's actual profile/answers AND workday mapping above.
- Verify all fields match the candidate's real information (name, email, phone, address, work history, education).
- Check that application answers are appropriate for this specific job and company.
- If ALL criteria are satisfied, respond with exactly: {{"verdict": "eligible"}}
- If ANY criteria is NOT satisfied, respond with: {{"verdict": "not_eligible", "reasons": ["reason 1", "reason 2", ...]}}
- Be strict on dates, names, and required fields. Be lenient on values that match any entry in an array.
- Only return the JSON object, no other text."""
