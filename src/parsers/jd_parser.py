import os
import json
import re
from openai import AzureOpenAI
from parsers.pdf_extractor import extract_text_from_pdf
from config.settings import *

# Initialize Azure OpenAI client
client = AzureOpenAI(
    api_key=AZURE_OPENAI_API_KEY,
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_version=AZURE_OPENAI_API_VERSION
)


def normalize_experience(exp_text: str) -> int:
    """
    Converts '1–3 years', '2+ years', '3 years' → int (minimum number of years)
    """
    if not exp_text:
        return 0
    numbers = re.findall(r"\d+", str(exp_text))
    return int(numbers[0]) if numbers else 0


def clean_llm_json(text: str) -> str:
    """
    Removes ```json ``` wrappers if LLM adds them
    """
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```json|```$", "", text, flags=re.MULTILINE).strip()
    return text


def parse_jd(pdf_path: str):
    """
    Parse a JD PDF and append it to jobs.json.
    Avoid duplicates based on job_id (filename).
    """
    # Extract text from PDF
    text = extract_text_from_pdf(pdf_path)
    job_id = os.path.splitext(os.path.basename(pdf_path))[0]

    # Prompt for LLM to parse JD
    prompt = f"""
Extract job description in JSON only with fields:
primary_skills, secondary_skills, min_experience, location, education

Rules:
- min_experience must be a number (years only)
- If location is not mentioned, return null

JD:
{text}
"""

    # Call Azure OpenAI
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

    # 🔧 Normalize fields
    parsed["min_experience"] = normalize_experience(parsed.get("min_experience"))
    parsed["location"] = parsed.get("location") or "Not specified"

    # Convert education to string if it's a list
    education = parsed.get("education")
    if isinstance(education, list):
        parsed["education"] = " / ".join(str(e) for e in education)

    # Add job_id
    parsed["job_id"] = job_id

    # Ensure PARSED_DIR exists
    os.makedirs(PARSED_DIR, exist_ok=True)

    # Read existing jobs
    try:
        with open(JOB_JSON_PATH, "r") as f:
            jobs = json.load(f)
    except FileNotFoundError:
        jobs = []

    # 🔹 Avoid duplicate Job IDs
    existing_ids = {job["job_id"] for job in jobs}
    if job_id not in existing_ids:
        jobs.append(parsed)
        with open(JOB_JSON_PATH, "w") as f:
            json.dump(jobs, f, indent=2)
        print(f" Parsed JD: {os.path.basename(pdf_path)} | Job ID: {job_id} | Exp: {parsed['min_experience']} yrs")
    else:
        print(f" Duplicate JD skipped: {job_id}")
