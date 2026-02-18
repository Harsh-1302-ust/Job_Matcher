import uuid
import json
import asyncio
import re
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

# -----------------------------
# Async Azure Client
# -----------------------------
client = AsyncAzureOpenAI(
    api_key=AZURE_OPENAI_API_KEY,
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_version=AZURE_OPENAI_API_VERSION
)

# Controls concurrency for N resumes
semaphore = asyncio.Semaphore(AZURE_CONCURRENCY)


# -----------------------------
# Utility: Clean JSON safely
# -----------------------------
def clean_json_response(text: str) -> str:
    text = text.strip()

    # Remove markdown if model adds accidentally
    if text.startswith("```"):
        text = re.sub(r"^```json|```$", "", text, flags=re.MULTILINE).strip()

    return text


# -----------------------------
# Enterprise Resume Prompt
# -----------------------------
def build_prompt(text: str) -> str:
    return f"""
You are an enterprise-level AI resume intelligence engine.

Your task is to extract structured hiring data and classify skills 
based on real usage depth in projects and professional work.

Follow internal reasoning carefully:

STEP 1 – Extract identity:
- Full candidate name
- Primary email

STEP 2 – Extract ALL technical skills mentioned.

STEP 3 – Analyze work experience & projects:
For each skill determine:
- Used professionally?
- Used in multiple projects?
- Used recently?
- Depth of involvement (architecture/design/optimization)?
- Or only exposure / coursework?

STEP 4 – Classify:

PRIMARY SKILLS:
- Core technologies used professionally
- Used in multiple projects
- Demonstrated depth

SECONDARY SKILLS:
- Limited exposure
- Academic only
- Supporting tools

STEP 5 – Calculate total professional experience:
- Convert to integer years only
- If range (3–5), take lower bound
- Ignore internships < 3 months

STEP 6 – Extract:
- Current location
- Highest completed degree

CRITICAL RULES:
- Think step-by-step internally.
- DO NOT reveal reasoning.
- Return STRICT JSON only.
- No explanation.
- No markdown.
- Valid JSON only.

OUTPUT FORMAT EXACTLY:

{{
  "name": "string",
  "email": "string",
  "primary_skills": ["skill1", "skill2"],
  "secondary_skills": ["skill1", "skill2"],
  "experience_years": integer,
  "location": "string",
  "education": "string"
}}

Resume Text:
{text}
"""


# -----------------------------
# Normalize skill text
# -----------------------------
def normalize_skill(skill: str) -> str:
    skill = (skill or "").lower()
    skill = re.sub(r"[^a-z0-9\s]", " ", skill)
    skill = re.sub(r"\s+", " ", skill)
    return skill.strip()


# -----------------------------
# Main Async Parser
# -----------------------------
async def parse_resume(pdf_path: str):
    async with semaphore:

        try:
            text = extract_text_from_pdf(pdf_path)

            response = await client.chat.completions.create(
                model=AZURE_OPENAI_DEPLOYMENT,
                messages=[
                    {"role": "system", "content": "You are a senior ATS resume parser."},
                    {"role": "user", "content": build_prompt(text)}
                ],
                temperature=0
            )

            raw_content = response.choices[0].message.content
            cleaned = clean_json_response(raw_content)
            parsed = json.loads(cleaned)

        except Exception as e:
            print(f"❌ Parsing failed for {pdf_path}: {e}")
            return

        # -----------------------------
        # Structured Resume Document
        # -----------------------------
        resume_data = {
            "candidate_id": str(uuid.uuid4()),
            "name": (parsed.get("name") or "").strip(),
            "email": (parsed.get("email") or "").strip().lower(),
            "primary_skills": [
                normalize_skill(s) for s in parsed.get("primary_skills", [])
            ],
            "secondary_skills": [
                normalize_skill(s) for s in parsed.get("secondary_skills", [])
            ],
            "experience_years": int(parsed.get("experience_years", 0)),
            "location": (parsed.get("location") or "").lower().strip(),
            "education": (parsed.get("education") or "").lower().strip()
        }

        # -----------------------------
        # Validation Checks
        # -----------------------------
        if not resume_data["email"]:
            print(f"⚠ Skipped (no email): {pdf_path}")
            return

        if resume_collection.find_one({"email": resume_data["email"]}):
            print(f"⚠ Duplicate skipped: {resume_data['email']}")
            return

        # -----------------------------
        # Store in MongoDB
        # -----------------------------
        resume_collection.insert_one(resume_data)
        print(f"✅ Stored: {resume_data['email']}")
