import os
import json
import uuid
from openai import AzureOpenAI
from parsers.pdf_extractor import extract_text_from_pdf
from config.settings import *

client = AzureOpenAI(
    api_key=AZURE_OPENAI_API_KEY,
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_version=AZURE_OPENAI_API_VERSION
)


def parse_resume(pdf_path: str):
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
- If name or email not found, return empty string
- skills must be a list of strings
- experience_years must be an integer

Resume:
{text}
"""

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
        print(f"❌ Failed to parse resume JSON for {pdf_path}: {e}")
        return

    # Ensure fields exist
    resume_data = {
        "candidate_id": str(uuid.uuid4()),
        "name": parsed.get("name", "").strip(),
        "email": parsed.get("email", "").strip(),
        "skills": parsed.get("skills", []),
        "experience_years": parsed.get("experience_years", 0),
        "location": parsed.get("location", ""),
        "education": parsed.get("education", "")
    }

    os.makedirs(PARSED_DIR, exist_ok=True)

    # Load existing resumes
    if os.path.exists(RESUME_JSON_PATH):
        with open(RESUME_JSON_PATH, "r") as f:
            data = json.load(f)
    else:
        data = []

    # Prevent duplicates by email
    existing_emails = {r["email"] for r in data if r.get("email")}
    if resume_data["email"] in existing_emails:
        print(f"⚠ Duplicate found. Skipping resume: {resume_data['name']} ({resume_data['email']})")
        return

    data.append(resume_data)

    with open(RESUME_JSON_PATH, "w") as f:
        json.dump(data, f, indent=2)

    print(f"✔ Parsed resume: {os.path.basename(pdf_path)} | Name: {resume_data['name']} | Email: {resume_data['email']}")
