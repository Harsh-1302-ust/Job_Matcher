import uuid
import json
from openai import AzureOpenAI
from asyncio_throttle import Throttler
from database.mongo import resume_collection
from parsers.pdf_extractor import extract_text_from_pdf
from config.settings import (
    AZURE_OPENAI_API_KEY,
    AZURE_OPENAI_ENDPOINT,
    AZURE_OPENAI_API_VERSION,
    AZURE_OPENAI_DEPLOYMENT,
    AZURE_API_RPM,
    AZURE_API_CONCURRENCY
)

client = AzureOpenAI(
    api_key=AZURE_OPENAI_API_KEY,
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_version=AZURE_OPENAI_API_VERSION
)

throttler = Throttler(rate_limit=AZURE_API_RPM, period=60)

async def parse_resume(pdf_path: str):
    text = extract_text_from_pdf(pdf_path)

    prompt = f"""
Extract resume details in STRICT JSON with these fields ONLY:
name
email
skills
experience_years
location
education

Rules:
- name/email → empty string if not found
- skills → list of strings
- experience_years → integer only

Resume:
{text}
"""

    async with throttler:
        response = client.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT,
            messages=[
                {"role": "system", "content": "You are an ATS resume parser"},
                {"role": "user", "content": prompt}
            ],
            temperature=0
        )

    try:
        parsed = json.loads(response.choices[0].message.content)
    except Exception as e:
        print(f"JSON parse failed for {pdf_path}: {e}")
        return

    resume_data = {
        "candidate_id": str(uuid.uuid4()),
        "name": parsed.get("name", "").strip(),
        "email": parsed.get("email", "").strip().lower(),
        "skills": parsed.get("skills", []),
        "experience_years": int(parsed.get("experience_years", 0)),
        "location": parsed.get("location", "").strip(),
        "education": parsed.get("education", "").strip()
    }

    if not resume_data["email"]:
        print(f"Resume skipped (no email): {pdf_path}")
        return

    if resume_collection.find_one({"email": resume_data["email"]}):
        print(f"Duplicate resume skipped: {resume_data['email']}")
        return

    resume_collection.insert_one(resume_data)
    print(f"Stored Resume: {resume_data['email']}")
