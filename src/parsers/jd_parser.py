import os
import json
import asyncio
from openai import AsyncAzureOpenAI
from database.mongo import job_collection
from parsers.pdf_extractor import extract_text_from_pdf
from config.settings import (
    AZURE_OPENAI_API_KEY,
    AZURE_OPENAI_ENDPOINT,
    AZURE_OPENAI_API_VERSION,
    AZURE_OPENAI_DEPLOYMENT,
    AZURE_CONCURRENCY
)

client = AsyncAzureOpenAI(
    api_key=AZURE_OPENAI_API_KEY,
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_version=AZURE_OPENAI_API_VERSION
)

semaphore = asyncio.Semaphore(AZURE_CONCURRENCY)


# -----------------------------
# 🔥 ENTERPRISE JD PROMPT
# -----------------------------
def build_prompt(text: str) -> str:
    return f"""
You are an enterprise-grade ATS Job Description parser.

Your task is to extract structured hiring intelligence.

Follow internal reasoning carefully:

STEP 1:
Extract job title if mentioned.

STEP 2:
Extract ALL technical skills mentioned.

STEP 3:
Classify skills:

PRIMARY SKILLS:
- Core mandatory technologies
- Mentioned as required
- Core stack of the role
- Frequently referenced

SECONDARY SKILLS:
- Nice-to-have
- Supporting tools
- Optional technologies

STEP 4:
Extract experience:
- Identify minimum experience required
- Identify maximum experience if mentioned
- If range like 3-5 years → min=3, max=5
- If only “3+ years” → min=3, max=null
- Return integers only

STEP 5:
Extract location.

STEP 6:
Extract required education.

CRITICAL:
- Think step-by-step internally
- Do NOT show reasoning
- Return STRICT JSON only
- No explanation
- No markdown
- Valid JSON only

OUTPUT FORMAT EXACTLY:

{{
  "job_title": "string",
  "primary_skills": ["skill"],
  "secondary_skills": ["skill"],
  "min_experience": integer,
  "max_experience": integer or null,
  "location": "string",
  "education": "string"
}}

JD TEXT:
{text}
"""


# -----------------------------
# 🔥 SAFE JSON PARSER
# -----------------------------
def safe_json_load(content: str):
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        # Attempt small fix if model adds extra text
        content = content.strip()
        start = content.find("{")
        end = content.rfind("}") + 1
        return json.loads(content[start:end])


# -----------------------------
# 🚀 MAIN PARSER
# -----------------------------
async def parse_jd(pdf_path: str):
    async with semaphore:
        text = extract_text_from_pdf(pdf_path)
        job_id = os.path.splitext(os.path.basename(pdf_path))[0]

        response = await client.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT,
            messages=[
                {"role": "system", "content": "You are a precise enterprise ATS job parser."},
                {"role": "user", "content": build_prompt(text)}
            ],
            temperature=0
        )

        parsed = safe_json_load(response.choices[0].message.content)

        job_data = {
            "job_id": job_id,
            "job_title": parsed.get("job_title", "").lower(),
            "primary_skills": list(set(s.lower() for s in parsed.get("primary_skills", []))),
            "secondary_skills": list(set(s.lower() for s in parsed.get("secondary_skills", []))),
            "min_experience": int(parsed.get("min_experience", 0)),
            "max_experience": (
                int(parsed["max_experience"])
                if parsed.get("max_experience") is not None
                else None
            ),
            "location": parsed.get("location", "").lower(),
            "education": parsed.get("education", "").lower()
        }

        # Remove overlap between primary & secondary
        job_data["secondary_skills"] = [
            s for s in job_data["secondary_skills"]
            if s not in job_data["primary_skills"]
        ]

        if not job_collection.find_one({"job_id": job_id}):
            job_collection.insert_one(job_data)
            print(f"Stored JD: {job_id}")
