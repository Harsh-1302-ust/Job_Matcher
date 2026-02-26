import uuid
import json
import asyncio
import re
from datetime import datetime
from openai import AsyncAzureOpenAI
from database.mongo import resume_collection
from parsers.pdf_extractor import extract_text_from_pdf
from config.settings import (
    AZURE_OPENAI_API_KEY,
    AZURE_OPENAI_ENDPOINT,
    AZURE_OPENAI_API_VERSION,
    AZURE_OPENAI_DEPLOYMENT,
    AZURE_CONCURRENCY
)

from config.tech_mapping import TECH_CATEGORIES_MAP as technologies_and_categories

client = AsyncAzureOpenAI(
    api_key=AZURE_OPENAI_API_KEY,
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_version=AZURE_OPENAI_API_VERSION
)

# Controls concurrency for N resumes
semaphore = asyncio.Semaphore(AZURE_CONCURRENCY)

def clean_json_response(text: str) -> str:
    text = text.strip()

    # Remove markdown if model adds accidentally
    if text.startswith("```"):
        text = re.sub(r"^```json|```$", "", text, flags=re.MULTILINE).strip()

    return text
current_date = datetime.utcnow().strftime("%d/%m/%Y")

def build_prompt(text: str) -> str:
    return f"""
            You are an enterprise-grade Resume Parser designed for an Applicant Tracking System (ATS).
Your task is to extract accurate, structured, and normalized information from the provided resume content.

------------------------------------------------------------
RESUME CONTENT:
{text}
------------------------------------------------------------

Generate a STRICT JSON response with the exact structure described below.
Do NOT add explanations or extra text outside JSON.

------------------------------------------------------------
RETURN JSON STRUCTURE
------------------------------------------------------------

{{
    "Employee_Name": "",
    "Email": "",
    "Education": [],
    "Experience": [],
    "Experience_Mentioned_In_Resume": 0.0,
    "Primary_Skills": [],
    "Secondary_Skills": [],
    "Location": [],
    "Technology": "",
    "Category": "",
    "Justification": "",
    "Profile_Summary": "",
    "Certifications": []
}}

------------------------------------------------------------
FIELD EXTRACTION RULES
------------------------------------------------------------

EMPLOYEE NAME:
- Extract full name.
- If unclear, return "Not Specified".

EMAIL:
- Extract professional email.
- If not present, return "Not Specified".

------------------------------------------------------------
EDUCATION:
------------------------------------------------------------

Return a list of dictionaries:
Each dictionary must contain:
- Period
- Degree
- University

Rules:
1. Period must contain start and end years (e.g., "2018 - 2022").
2. If any field is missing, return "Not Specified".
3. If education section is missing entirely, return [].

------------------------------------------------------------
EXPERIENCE:
------------------------------------------------------------

Return experience entries ordered:
Most recent → Oldest

Each entry must contain:

- "Period":
    Format strictly as:
    dd/mm/yyyy - dd/mm/yyyy

    Rules:
    • If end date is "Present", replace it with {current_date}
    • If day is missing → use "XX"
    • If month is missing → use "XX/XX/yyyy"
    • If completely missing → "Not Specified"

- "Experience_In_Months":
    Calculate total months for that role.
    If period missing → return 0.

- "Company":
    Extract official company name.
    If missing → "Not Specified"

- "Job_Role":
    Extract official role/title.
    If missing → "Not Specified"

- "Summary":
    Write concise summary of responsibilities.
    Must be gender-neutral.
    Do NOT assume pronouns.
    If unavailable → "Not Available"

- "Skills":
    Extract only standardized technical skills used in that role.
    Examples:
    Python, Java, SQL, Docker, Kubernetes, Git, Agile, Scrum
    For abbreviations like UI, SQL → uppercase.
    If no skills → return []

If no experience section exists → return [].

------------------------------------------------------------
EXPERIENCE_MENTIONED_IN_RESUME:
------------------------------------------------------------

Extract explicitly stated years of experience.
Examples:
- "9.5+ years experience" → 9.5
- "4 years of experience" → 4.0

Rules:
- Return float with one decimal place.
- If nothing mentioned → return 0.0.

------------------------------------------------------------
PRIMARY SKILLS:
------------------------------------------------------------

Extract top skills based on:
- Most recent role
- Depth of experience
- Frequency of mention

Rules:
1. Only standardized professional skills.
2. No soft skills.
3. Proper casing:
   - Node.js
   - React.js
   - JavaScript
   - TypeScript
   - PostgreSQL
4. Abbreviations like SQL, UI → uppercase.
5. If none → [].

------------------------------------------------------------
SECONDARY SKILLS:
------------------------------------------------------------

All remaining valid technical skills not in Primary_Skills.

Rules:
- Same normalization rules as primary skills.
- No duplicates.
- If none → [].

------------------------------------------------------------
LOCATION:
------------------------------------------------------------

Extract candidate current location.

Priority Order:

1. Contact/Header section location.
2. Most recent job location (if available).
3. If no experience AND no header location,
   use most recent education location.

Rules:
- Return city name only.
- Return as list.
- Do NOT guess.
- If absolutely not mentioned → return [].

------------------------------------------------------------
TECHNOLOGY:
------------------------------------------------------------

Determine dominant technology using mapping:
{technologies_and_categories}

Rules:
1. Most recent role takes priority.
2. If recent role is Manager or Business Analyst,
   classify accordingly even if earlier developer experience exists.
3. If no alignment → "Others".

------------------------------------------------------------
CATEGORY:
------------------------------------------------------------

Determine specific category within selected Technology.

Rules:
1. Reflect most recent role.
2. Must align with mapping.
3. If no alignment → "Others".

------------------------------------------------------------
JUSTIFICATION:
------------------------------------------------------------

Provide brief reasoning explaining:
- Why selected technology
- Why selected category
- How recent role influenced classification

Keep concise and logical.

------------------------------------------------------------
PROFILE SUMMARY:
------------------------------------------------------------

Write a concise profile summary under 55 words.
Must:
- Be gender-neutral.
- Mention years of experience.
- Mention specialization.
- Mention key technologies.
- Avoid pronouns.
- Be professional and compact.

------------------------------------------------------------
CERTIFICATIONS:
------------------------------------------------------------

Extract professional certifications only.
Examples:
- AWS Certified Solutions Architect
- Scrum Master Certified
- PMP
- Azure Fundamentals

If none mentioned → [].

------------------------------------------------------------
LANGUAGE RULE:
------------------------------------------------------------

If resume is in Spanish:
- Keep JSON keys in English.
- Return values in Spanish.

If resume is in English:
- Return everything in English.

------------------------------------------------------------
STRICT RULES:
------------------------------------------------------------

1. Return ONLY valid JSON.
2. No trailing commas.
3. No extra explanations.
4. No hallucinated data.
5. If data unavailable, follow fallback rules strictly.

------------------------------------------------------------
NOW PROCESS THE RESUME ABOVE. 
                """


# -----------------------------
# Normalize skill text
# -----------------------------
def normalize_skill(skill: str) -> str:
    skill = (skill or "").lower()
    skill = re.sub(r"[^a-z0-9\s]", " ", skill)
    skill = re.sub(r"\s+", " ", skill)
    return skill.strip()
import re

def normalize_location(location: str) -> str:
    location = (location or "").lower()
    location = re.sub(r"[^a-z0-9\s]", " ", location)
    location = re.sub(r"\s+", " ", location)
    return location.strip()

def extract_email(text: str) -> str:
    # Fix common pypdf formatting issues
    text = text.replace("\n", " ")
    text = re.sub(r"\s*@\s*", "@", text)
    text = re.sub(r"\s*\.\s*", ".", text)

    match = re.search(
        r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
        text
    )

    return match.group(0).lower() if match else ""

# -----------------------------
# Main Async Parser
# -----------------------------
async def parse_resume(pdf_path: str):

    async with semaphore:

        try:
            text = extract_text_from_pdf(pdf_path)
            extracted_email = extract_email(text)

            response = await client.chat.completions.create(
                model=AZURE_OPENAI_DEPLOYMENT,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a senior ATS resume parser. Return ONLY valid JSON."
                    },
                    {"role": "user", "content": build_prompt(text)}
                ],
                temperature=0,
                response_format={"type": "json_object"}
            )

            raw_content = response.choices[0].message.content
            cleaned = clean_json_response(raw_content)
            parsed = json.loads(cleaned)

        except Exception as e:
            print(f"❌ Parsing failed for {pdf_path}: {e}")
            return


        # 1️⃣ Mentioned years → months
        mentioned_years = float(
            parsed.get("Experience_Mentioned_In_Resume", 0) or 0
        )
        mentioned_months = int(mentioned_years * 12)

        # 2️⃣ Calculated months from each role
        experience_details = parsed.get("Experience", [])

        calculated_months = 0
        for exp in experience_details:
            if isinstance(exp, dict):
                months = exp.get("Experience_In_Months", 0)
                try:
                    calculated_months += int(months)
                except:
                    pass

        # 3️⃣ Enterprise logic → take maximum
        total_experience_months = max(mentioned_months, calculated_months)
        total_experience_years = round(total_experience_months / 12, 1)

        # ---------------------------------
        # Resume Document
        # ---------------------------------
        resume_data = {
            "candidate_id": str(uuid.uuid4()),
            "name": (parsed.get("Employee_Name") or "").strip(),
            "email": extracted_email,

            "primary_skills": [
                normalize_skill(s)
                for s in parsed.get("Primary_Skills", [])
                if isinstance(s, str)
            ],

            "secondary_skills": [
                normalize_skill(s)
                for s in parsed.get("Secondary_Skills", [])
                if isinstance(s, str)
            ],

            "location": [
                normalize_location(loc)
                for loc in parsed.get("Location", [])
                if isinstance(loc, str)
            ],

            "total_experience_months": total_experience_months,
            "total_experience_years": total_experience_years,

            "education": parsed.get("Education", []),
            "experience_details": experience_details,

            "technology": parsed.get("Technology", "Others"),
            "category": parsed.get("Category", "Others"),
            "justification": parsed.get("Justification", ""),

            "profile_summary": parsed.get("Profile_Summary", ""),
            "certifications": parsed.get("Certifications", []),

            "created_at": datetime.utcnow()
        }

        if not resume_data["email"]:
            print(f"⚠ Skipped (no email): {pdf_path}")
            return

        if resume_collection.find_one({"email": resume_data["email"]}):
            print(f"⚠ Duplicate skipped: {resume_data['email']}")
            return

        resume_collection.insert_one(resume_data)

        print(f"✅ Stored: {resume_data['email']}")