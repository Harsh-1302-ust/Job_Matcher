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

# -----------------------------------
# 🔹 Azure OpenAI Client
# -----------------------------------

client = AsyncAzureOpenAI(
    api_key=AZURE_OPENAI_API_KEY,
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_version=AZURE_OPENAI_API_VERSION
)

semaphore = asyncio.Semaphore(AZURE_CONCURRENCY)


# -----------------------------------
# 🔹 Technology Mapping
# -----------------------------------
from config.tech_mapping import TECH_CATEGORIES_JSON_STR as technologies_and_categories


# -----------------------------------
# 🔥 YOUR EXACT PROMPT
# -----------------------------------

def build_prompt(job_description: str) -> str:
    return f"""
 You are an expert text parser. Your task is to extract relevant information from the provided job description (JD). Your responsibilities include:
               
                       1. Extracting only standardized skills, which are widely recognized and professionally relevant.
                       2. Assigning a score between 1 and 10 to each skill based on its relevance to the job as described in the JD.
                       3. Deriving the minimum years of experience required based on the job description.
                       4. Classifying the job under an appropriate category and technology based on the provided mapping.
                       5. Generating a concise job summary that highlights the key aspects of the role. (A job summary must be generated for every job description.)
                       6. Identifying key responsibilities of the role as a list of 3 to 5 points, outlining the main duties.
                       7. Extracting a set of "Good to Have Skills" that align with the job role but are not mandatory.
                       8. If the job description mentions a broad skill cluster (e.g., "Backend Development", "Frontend Development", "DevOps", etc.) without listing specific skills, infer a set of relevant skills for that cluster based on industry standards.
                       9. If the job description is empty or is not inferable, return the fields of json as empty string or emplty list.
               
                   Return the results in the following JSON format:
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
                       "technology": "Matching Technology from the provided mapping or 'others' if no mapping is found",
                       "category": "Matching Category from the provided mapping or 'others' if no mapping is found",
                       "location": "Extract city, state, and country if mentioned anywhere in the JD (title, header, footer, or body). if there are multiple locations then show in a list . If remote, return 'Remote'. If hybrid, return 'Hybrid - <City>'. Else return 'N/A'.",
                       "justification": "Description of the logic behind the selection of the corresponding category and technology."
                   }}
               
                   Guidelines for Job Summary Extraction:
                       1. Summarize the key responsibilities and expectations of the role in 1-2 sentences.
                       2. Capture essential aspects such as the primary technologies, main tasks, and team collaboration aspects.
                       3. Keep the summary concise and informative without unnecessary details.
                       4. Ensure that a job summary is generated for every job description.
               
                   Guidelines for Key Responsibilities Extraction:
                       1. Identify the main duties mentioned in the JD and summarize them in a list.
                       2. Ensure at least 3 and at most 5 key responsibilities are included.
                       3. Responsibilities should be precise and actionable, focusing on primary tasks.
                       4. Example:
                          - "Develop and maintain backend services using Java and Spring Boot."
                          - "Collaborate with cross-functional teams to design scalable solutions."
                          - "Ensure code quality through unit testing and peer reviews."
               
                   Guidelines for Skills Extraction:
                       1. Focus on technical and professional skills such as programming languages (e.g., Python, Java), tools (e.g., Docker, Git), and methodologies (e.g., Agile, Scrum).
                       2. Exclude generic phrases, non-skill-related text, or ambiguous terms.
                       3. Ensure that all skills are in title case and correctly spelled. For abbreviations like UI, SQL, ensure they are in upper case.
                       4. Assign scores based on how frequently the skill is mentioned or implied as critical in the job description.
                       5. If the job description only references a broad skill cluster (like "Backend", "Frontend", "DevOps", etc.) without specific skills, generate a list of commonly associated skills for that cluster.
               
                   Guidelines for Good to Have Skills Extraction:
                       1. Identify additional skills that are beneficial but not explicitly required.
                       2. These may include complementary technologies, tools, frameworks, or methodologies relevant to the role.
                       3. Keep the list concise and aligned with the job responsibilities.
                       4. Example:
                          - If the JD is for a Java backend developer, good to have skills might include "Kafka," "GraphQL," or "Cloud Platforms."
                          - If the JD is for a Data Engineer, good to have skills might include "Snowflake," "Airflow," or "Terraform."
               
                   Guidelines for Experience Extraction:
                       1. Look for phrases like "X years of experience," "minimum X years," or similar variations.
                       2. If multiple values are mentioned, choose the minimum explicitly stated or implied.
                       3. If no clear number is provided, default to 0.
               
                   Guidelines for Technology and Category Classification:
                       1. Identify the most relevant technology from the given mapping based on the mentioned skills and requirements.
                       2. Choose the best matching category within that technology.
                       3. If multiple technologies/categories are possible, select the most dominant based on the frequency of mentions in the JD.
                       4. If no relevant technology is mapped based on the provided mapping, default both 'technology' and 'category' to 'Others'.
                       5. Additionally, provide a justification for the selection, describing the logic behind choosing the specific category and technology, including reference to the relevance and frequency of key skills.
               
                   Normalize all skill names to a consistent format. Use proper casing and standard naming conventions as widely accepted in the tech industry.
                   For example, always use "Node.js" instead of variations like "node.js", "Node.JS", or "Node.Js". 
                   Similarly, standardize names like "React.js", "JavaScript", "TypeScript", "PostgreSQL", etc.
                   Make sure the same skill is always represented in the same format across all outputs.
                   If there is ambiguity in formatting, refer to the official or most commonly accepted spelling on developer documentation or trusted sources (e.g., MDN, official language websites, or GitHub).

                   Technology and Category Mapping:
                   {technologies_and_categories}
               
                   Example Input JD:
                   'We are looking for a Java backend developer experienced in backend development. A minimum of 3 years of experience is required.'
               
                   Example Output JSON:
                   {{
                       "job_summary": "Hiring a Java backend developer skilled in building robust server-side applications using Java and related backend technologies.",  
                       "key_responsibilities": [
                           "Develop and maintain scalable backend applications using Java and Spring Boot.",
                           "Design and implement RESTful APIs and microservices.",
                           "Optimize application performance and ensure security best practices."
                       ],
                       "required_skills_with_scores": [
                           {{"skill_name": "Java", "score": 10}},
                           {{"skill_name": "Spring Boot", "score": 9}},
                           {{"skill_name": "Microservices", "score": 8}},
                           {{"skill_name": "REST API", "score": 7}},
                           {{"skill_name": "SQL", "score": 6}},
                           {{"skill_name": "Docker", "score": 5}},
                           {{"skill_name": "Kubernetes", "score": 5}},
                           {{"skill_name": "CI/CD", "score": 5}}
                       ],
                       "good_to_have_skills": [
                           "Kafka",
                           "GraphQL",
                           "AWS",
                           "Terraform"
                       ],
                       "minimum_experience_in_years": 3,
                       "technology": "Java",
                       "category": "Core Java",
                       "location": "Pune, India",
                       "justification": "Selected Java and Core Java because the JD emphasizes Java-based backend development through required skills like Spring Boot, Microservices, and REST API, indicating a strong focus on server-side applications."
                   }}
               
                   Now, process the following job description:
                   {job_description}
"""


# -----------------------------------
# 🔹 SAFE JSON PARSER
# -----------------------------------

def safe_json_load(content: str):
    try:
        return json.loads(content)
    except Exception:
        content = content.strip()
        start = content.find("{")
        end = content.rfind("}") + 1
        return json.loads(content[start:end])


# -----------------------------------
# 🚀 MAIN JD PARSER
# -----------------------------------

async def parse_jd(pdf_path: str):

    async with semaphore:

        print(f"📄 Processing: {pdf_path}")

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

        # -----------------------------------
        # 🔥 FIXED SKILL PROCESSING
        # -----------------------------------

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

        # -----------------------------------
        # 📦 FINAL STRUCTURE
        # -----------------------------------

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