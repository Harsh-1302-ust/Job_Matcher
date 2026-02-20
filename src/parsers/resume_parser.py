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
current_date = datetime.utcnow().strftime("%d/%m/%Y")

# -----------------------------
# Enterprise Resume Prompt
# -----------------------------
def build_prompt(text: str) -> str:
    return f"""
            Below is the data about an employee in a firm. The employee details are as follows:
 
                Resume Content: {text}
                Based on the resume content, generate a JSON containing the following information:
 
                Employee Name:
                    The full name of the employee.
                Email:
                    The email address of the employee. If the email is not mentioned, write "Not Specified".
                Education:
                    The educational background of the employee, including their degree and major.
                    This field should be a list of dictionaries where each dictionary contains:
                        - Period: The start and end years of the education.
                        - Degree: The degree obtained.
                        - University: The name of the university or institution.
                        - If any of these fields are not specified, write "Not Specified".
                    If education is not mentioned, return an empty list [].
                Experience:
                    The work experience of the employee, including roles and responsibilities. From the most recent one to the least recent one. This field should contain these fields inside it:
                        Period: start and end dates of the current experience. Both start and end dates should be in the format dd/mm/yyyy. If the dates are not specified, write "Not Specified". If the end date is specified as "Present" change it to current date which is {current_date}. If the day is not specified, use "XX" in place of the day, e.g., "XX/03/2022". If month is not specified, use "XX/XX/yyyy" format.
                        Experience_In_Months: Total months of experience in this given role returned as a integer. If the period of experience is not specified, return 0.
                        Company: the name of the company. If the company is not specified, write "Not Specified".
                        Job_Role: the role of the employee in the company. If the role is not specified, write "Not Specified".
                        Summary:  A brief summary of the responsibilities and achievements in the role, written using gender-neutral language without assuming any pronouns. If a summary is not available, write "Not Available".
                        Skills: the skills used in the role. For abbreviations like UI, SQL, ensure they are in upper case. If skills are not mentioned, return an empty list [].
                    If experience is not mentioned, return an empty list [].
                Experience_Mentioned_In_Resume:
                    Explicitly metioned years of experience in the resume. This should be a number representing the total years of experience mentioned in the resume. If no experience is mentioned, return 0.
                    This should be a float value with one decimal place. For example, if the employee has 9 years and 6 months of experience, return 9.5.
                    Usually employee may say in their resume that they have 9.5+ year of experience then return 9.5
                Primary Skills:
                    The skills in which the employee excels the most.
                    These skills should align with their most recent job experience and areas where they are most experienced.
                    Extracting only standardized skills, which are widely recognized and professionally relevant.
                    Focus on technical and professional skills such as programming languages (e.g., Python, Java), tools (e.g., Docker, Git), and methodologies (e.g., Agile, Scrum).
                    Exclude generic phrases, non-skill-related text, or ambiguous terms.
                    Ensure that all skills are in title case and correctly spelled.
                    For abbreviations like UI, SQL, ensure they are in upper case.
                    If primary skills are not available return an empty list [].
                Secondary Skills:
                    The remaining skills that are not considered primary but are still part of the employee's skill set.
                    Extracting only standardized skills, which are widely recognized and professionally relevant.
                    Focus on technical and professional skills such as programming languages (e.g., Python, Java), tools (e.g., Docker, Git), and methodologies (e.g., Agile, Scrum).
                    Exclude generic phrases, non-skill-related text, or ambiguous terms.
                    Ensure that all skills are in title case and correctly spelled.
                    For abbreviations like UI, SQL, ensure they are in upper case.
                    If secondary skills are not avavilable return an empty list [].
                Technology:
                    The main technology the employee specializes in, based on their experience and recent job role.
                    This should be determined using the mapping provided: {technologies_and_categories}.
                    If their most recent role is as a manager or business analyst, the technology should reflect that, even if they previously worked in software development roles.
                Category:
                    The specific category of expertise the employee falls under.
                    This should be determined using the mapping provided: {technologies_and_categories}.
                    The category should reflect their most recent experience and role. For example:
                    If their most recent role is as a manager or business analyst, the category should reflect that, even if they previously worked in software development roles.
                If the employees most recent experience and skills dont align with any of the given technology and category, set both the value as Others.
                Justification:
                    A brief explanation supporting the categorization.
                    The justification should consider the recent job roles, skills, main technology, and the selected category.
                Profile Summary:
                    Write a concise profile summary of the employee based on the information provided in their resume. This summary should include details about the employee's skills, overall experience, years of experience, and expertise in their field. Ensure that the language is completely gender neutral and avoids any pronouns that assume gender identity. Try to make it in less than 55 words.
                Certifications:
                    A list of professional certifications obtained by the employee.
                    If no certifications are mentioned, return an empty list [].

                Important:
                    - If the resume content is in Spanish, return the **values** in Spanish but keep the JSON **keys** in English.
                    - If the resume content is in English, return everything in English as usual.
               
                Example of the JSON structure:
                    "Employee_Name": "John Doe",
                    "Email": "john.doe@example.com",
                    "Education": [
                        {{
                            Period: "2010 - 2013",
                            Degree: "Master of Computer Applications (MCA)",
                            University: "A P J Abdul Kalam Technological University"
                        }}
                    ]
                    "Experience": [
                        {{
                            "Period": "01/01/2025 - {current_date}",
                            "Experience_In_Months": 5,
                            "Company": "UST",
                            "Job_Role": "Senior Software Engineer",
                            "Summary": "Developed and enhanced Dell's commerce platform by designing and coding .Net Core web APIs using C#. Collaborated with product owners and stakeholders, managed requirements via JIRA and Confluence, resolved application bugs, and implemented unit tests and BDDs within an agile framework.",
                            "Skills": ["C#", ".Net Core Web API", "Microservices", "Git", "BDD", "Unit Testing", "Dynatrace", "CICD Pipeline"]
                        }},
                        {{
                            "Period": "21/08/2021 - 21/12/2024",
                            "Experience_In_Months": 40,
                            "Company": "UST-Experience Incubator",
                            "Job_Role": "Developer",
                            "Summary": "Development REST API using .NET Core, dependency injection, repository pattern, dapper and database connection in MySQL. Developing a MVC web application, using .NET Core, consuming a REST API, dependency injection, merge of modules, design of modules with HTML and CSS. Saving and updating changes in a git repository, working on branches. Scrum agile participates in daily standup meetings, planning meetings, grooming, demo, closure and three sprints.",
                            "Skills": ["Visual Studio Code", "MySQL Workbench", "GitLab", "Ubuntu server"]
                        }},
                        {{
                            "Period": "Not Specified",
                            "Experience_In_Months": 0,
                            "Company": "UST",
                            "Job_Role": "QA Engineer - Integration Testing",
                            "Summary": "Executed custom SQL queries to ensure data accuracy and integrity while performing regression, functionality, and integration testing on healthcare insurance processes. Collaborated in agile sprints, managed defect reporting using JIRA, and maintained test cases in TestRail.",
                            "Skills": "Skills": ["SQL", "Jira", "TestRail", "Comparator Tool"]
                        }}
                    ],
                    "Experience_Mentioned_In_Resume": 4.0,
                    "Primary_Skills": ["Python", "SQL"],
                    "Secondary_Skills": ["Java", "C++"],
                    "Technology": "CyberSecurity",
                    "Category": "CyberSecurity",
                    "Justification": "The employee has extensive experience in Python and SQL, which are the primary skills. The secondary skills include Java and C++. The employee's main technology is CyberSecurity, and the category of expertise is CyberSecurity.",
                    "Profile_Summary": "A highly skilled developer with a strong foundation in Python and SQL, with experience in backend development, API integration, and agile methodologies. Known for delivering high-quality work and actively contributing to team success." ,
                    "Certifications": ["Scrum Master Certified (SCM)", "AWS Certified Solutions Architect - Associate"]  
                """


# -----------------------------
# Normalize skill text
# -----------------------------
def normalize_skill(skill: str) -> str:
    skill = (skill or "").lower()
    skill = re.sub(r"[^a-z0-9\s]", " ", skill)
    skill = re.sub(r"\s+", " ", skill)
    return skill.strip()

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

            try:
                parsed = json.loads(cleaned)
            except json.JSONDecodeError as err:
                print("⚠ JSON decode error")
                print("Raw response:\n", raw_content)
                print("Cleaned response:\n", cleaned)
                raise err

        except Exception as e:
            print(f"❌ Parsing failed for {pdf_path}: {e}")
            return

        resume_data = {
            "candidate_id": str(uuid.uuid4()),
            "name": (parsed.get("Employee_Name") or "").strip(),
            "email": extracted_email,
            "primary_skills": [
                normalize_skill(s) for s in parsed.get("Primary_Skills", [])
            ],
            "secondary_skills": [
                normalize_skill(s) for s in parsed.get("Secondary_Skills", [])
            ],
            "experience_years": float(parsed.get("Experience_Mentioned_In_Resume", 0) or 0),
            "location": "",
            "education": ""
        }

        if not resume_data["email"]:
            print(f"⚠ Skipped (no email): {pdf_path}")
            return

        if resume_collection.find_one({"email": resume_data["email"]}):
            print(f"⚠ Duplicate skipped: {resume_data['email']}")
            return

        resume_collection.insert_one(resume_data)
        print(f"✅ Stored: {resume_data['email']}")