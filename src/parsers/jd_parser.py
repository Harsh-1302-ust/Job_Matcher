import os
import json
import asyncio
from datetime import datetime
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

from config.tech_mapping import TECH_CATEGORIES_JSON_STR as technologies_and_categories




def build_prompt(job_description: str) -> str:
    return f"""
 You are an expert enterprise-grade Job Description (JD) parser designed for an Applicant Tracking System (ATS). 
Your task is to extract structured, standardized, and normalized information from the provided job description (JD).

Your responsibilities include:

1. Extracting only standardized, professionally recognized technical and domain skills.
2. Assigning a relevance score between 1 and 10 to each required skill based on importance and emphasis in the JD.
3. Deriving the minimum years of experience required.
4. Classifying the job under the most appropriate technology and category using the provided mapping.
5. Generating a concise but meaningful job summary.
6. Identifying 3 to 5 key responsibilities.
7. Extracting "Good to Have Skills" that are beneficial but not mandatory.
8. Inferring relevant skills if only broad clusters are mentioned (e.g., Backend, Frontend, DevOps, Data Engineering).
9. Extracting and normalizing location information carefully and consistently.
10. If the job description is empty, unclear, or non-inferable, return empty strings or empty lists for all fields.

------------------------------------------------------------
RETURN STRICT JSON FORMAT (DO NOT ADD EXTRA TEXT)
------------------------------------------------------------

Return the results strictly in the following JSON structure:

{{
    "job_summary": "Brief description of the role and key responsibilities.",  
    "key_responsibilities": [
        "Responsibility 1",
        "Responsibility 2",
        "Responsibility 3",
        "Responsibility 4",
        "Responsibility 5"
    ],
    "required_skills_with_scores": [
        {{"skill_name": "Skill1", "score": 10}},
        {{"skill_name": "Skill2", "score": 8}},
        {{"skill_name": "Skill3", "score": 5}}
    ],
    "good_to_have_skills": [
        "Skill A",
        "Skill B",
        "Skill C"
    ],
    "minimum_experience_in_years": X,
    "technology": "Matching Technology from the provided mapping or 'Others' if no mapping is found",
    "category": "Matching Category from the provided mapping or 'Others' if no mapping is found",
    "location": "Extracted and normalized location",
    "justification": "Description of the logic behind the selection of the corresponding category and technology."
}}

------------------------------------------------------------
DETAILED EXTRACTION GUIDELINES
------------------------------------------------------------

JOB SUMMARY GUIDELINES:
1. Summarize the primary purpose of the role in 1-2 concise sentences.
2. Mention core technologies and responsibilities.
3. Avoid generic filler text.
4. Always generate a summary, even if minimal information is available.

KEY RESPONSIBILITIES GUIDELINES:
1. Extract the main duties from the JD.
2. Provide between 3 and 5 responsibilities.
3. Keep them action-oriented and precise.
4. Avoid repetition or vague statements.

SKILL EXTRACTION GUIDELINES:
1. Extract only real, standardized technical skills:
   - Programming languages (Python, Java, Go, C++)
   - Frameworks (Spring Boot, React.js, Django)
   - Databases (PostgreSQL, MySQL, MongoDB)
   - Tools (Docker, Kubernetes, Git)
   - Cloud platforms (AWS, Azure, GCP)
   - Methodologies (Agile, Scrum, CI/CD)
2. Exclude soft skills (e.g., communication, teamwork).
3. Exclude generic phrases (e.g., “problem solving”, “dynamic environment”).
4. Normalize skill names strictly:
   - Use "Node.js" not nodejs or Node.JS
   - Use "React.js"
   - Use "JavaScript"
   - Use "TypeScript"
   - Use "PostgreSQL"
5. Ensure consistent naming across outputs.
6. Assign scores:
   - 9-10 → Core mandatory skills
   - 7-8 → Important but not dominant
   - 4-6 → Supporting skills
   - 1-3 → Minor mentions

GOOD TO HAVE SKILLS GUIDELINES:
1. Extract beneficial but optional skills.
2. Include complementary tools, frameworks, or cloud services.
3. Keep concise and relevant.

EXPERIENCE EXTRACTION GUIDELINES:
1. Detect phrases like:
   - "X years of experience"
   - "Minimum X years"
   - "At least X years"
2. If multiple values are mentioned, select the minimum clearly required.
3. If no number is mentioned, return 0.

------------------------------------------------------------
ADVANCED LOCATION EXTRACTION RULES
------------------------------------------------------------

1. Search the entire JD including:
   - Title
   - Header
   - Footer
   - Body
2. Extract city, state, and country if available.
3. Normalize format:
   - "Bangalore, Karnataka, India"
   - "Pune, India"
4. If multiple locations exist:
   Return as a list:
   ["Bangalore, India", "Hyderabad, India"]
5. If remote:
   Return exactly "Remote"
6. If hybrid:
   Return "Hybrid - <City>"
   Example: "Hybrid - Chennai"
7. If no location information is found:
   Return "N/A"

------------------------------------------------------------
TECHNOLOGY AND CATEGORY CLASSIFICATION RULES
------------------------------------------------------------

1. Use the provided mapping strictly.
2. Identify dominant skills.
3. Select the most relevant technology.
4. Then choose the best matching category under that technology.
5. If no match exists:
   Return "Others" for both technology and category.
6. Provide a strong justification explaining:
   - Dominant skills detected
   - Why the chosen technology/category best matches
   - Why alternatives were not selected

------------------------------------------------------------
TECHNOLOGY AND CATEGORY MAPPING:
{technologies_and_categories}
------------------------------------------------------------

Now process the following Job Description:

{job_description}
"""


def safe_json_load(content: str):
    try:
        return json.loads(content)
    except Exception:
        content = content.strip()
        start = content.find("{")
        end = content.rfind("}") + 1
        return json.loads(content[start:end])


async def parse_jd(pdf_path: str):

    async with semaphore:

        print(f" Processing: {pdf_path}")

        text = extract_text_from_pdf(pdf_path)

        if not text or not text.strip():
            print("⚠ Empty JD detected.")
            return

        job_id = os.path.splitext(os.path.basename(pdf_path))[0]

        response = await client.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT,
            messages=[
                {
                    "role": "system",
                    "content": "You are a precise enterprise ATS job parser."
                },
                {
                    "role": "user",
                    "content": build_prompt(text)
                }
            ],
            temperature=0
        )

        raw_output = response.choices[0].message.content
        parsed = safe_json_load(raw_output)



        required_skills_list = parsed.get("required_skills_with_scores", [])
        good_to_have = parsed.get("good_to_have_skills", [])

        # Convert list → dict safely
        required_skills_dict = {
            item["skill_name"]: item["score"]
            for item in required_skills_list
            if isinstance(item, dict)
            and "skill_name" in item
            and "score" in item
        }

        # Primary = score >= 8
        primary_skills = [
            skill.lower()
            for skill, score in required_skills_dict.items()
            if score >= 8
        ]

        # Secondary = score < 8 + good_to_have
        secondary_skills = [
            skill.lower()
            for skill, score in required_skills_dict.items()
            if score < 8
        ] + [s.lower() for s in good_to_have]

        secondary_skills = list(set(secondary_skills) - set(primary_skills))

        # Safe experience conversion
        try:
            min_exp = int(parsed.get("minimum_experience_in_years", 0))
        except Exception:
            min_exp = 0


        job_data = {
            "job_id": job_id,
            "job_summary": parsed.get("job_summary", ""),
            "key_responsibilities": parsed.get("key_responsibilities", []),

            "required_skills_with_scores": {
                k.lower(): v for k, v in required_skills_dict.items()
            },

            "primary_skills": list(set(primary_skills)),
            "secondary_skills": list(set(secondary_skills)),

            "minimum_experience_in_years": min_exp,
            "technology": parsed.get("technology", "Others"),
            "category": parsed.get("category", "Others"),
            "location": parsed.get("location", "N/A"),
            "justification": parsed.get("justification", ""),
            "created_at": datetime.utcnow()
        }

        # -----------------------------------
        # 🚀 UPSERT TO MONGODB
        # -----------------------------------

        job_collection.update_one(
            {"job_id": job_id},
            {"$set": job_data},
            upsert=True
        )

        print(f"✅ Stored/Updated JD: {job_id}")