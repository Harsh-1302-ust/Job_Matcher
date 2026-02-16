import os
import json
import re
import asyncio
from openai import AzureOpenAI
from asyncio_throttle import Throttler
from parsers.pdf_extractor import extract_text_from_pdf
from database.mongo import job_collection
from config.settings import (
    AZURE_OPENAI_API_KEY,
    AZURE_OPENAI_ENDPOINT,
    AZURE_OPENAI_API_VERSION,
    AZURE_OPENAI_DEPLOYMENT,
    AZURE_API_RPM
)

client = AzureOpenAI(
    api_key=AZURE_OPENAI_API_KEY,
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_version=AZURE_OPENAI_API_VERSION
)

throttler = Throttler(rate_limit=AZURE_API_RPM, period=60)

def normalize_experience(exp_text: str) -> int:
    if not exp_text:
        return 0
    numbers = re.findall(r"\d+", str(exp_text))
    return int(numbers[0]) if numbers else 0

def clean_llm_json(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```json|```$", "", text, flags=re.MULTILINE).strip()
    return text

async def parse_jd(pdf_path: str):
    text = extract_text_from_pdf(pdf_path)
    job_id = os.path.splitext(os.path.basename(pdf_path))[0]

    prompt = f"""
Extract job description in JSON only with fields:
primary_skills, secondary_skills, min_experience, location, education

Rules:
- min_experience must be a number (years only)
- If location is not mentioned, return null

JD:
{text}
"""

    async with throttler:
        response = client.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT,
            messages=[
                {"role": "system", "content": "You are a job description parser"},
                {"role": "user", "content": prompt}
            ],
            temperature=0
        )

    raw_content = response.choices[0].message.content
    parsed = json.loads(clean_llm_json(raw_content))

    parsed["min_experience"] = normalize_experience(parsed.get("min_experience"))
    parsed["location"] = parsed.get("location") or "Not specified"

    education = parsed.get("education")
    if isinstance(education, list):
        parsed["education"] = " / ".join(str(e) for e in education)

    parsed["job_id"] = job_id

    if job_collection.find_one({"job_id": job_id}):
        print(f" Duplicate JD skipped: {job_id}")
        return

    job_collection.insert_one(parsed)
    print(f"Parsed JD: {pdf_path}")
