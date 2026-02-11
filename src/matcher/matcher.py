from database.mongo import resume_collection, job_collection
import re


# --- normalizers
def normalize_skill(skill: str) -> str:
    skill = (skill or "").lower()
    skill = re.sub(r"[^a-z0-9\s]", " ", skill)
    skill = re.sub(r"\s+", " ", skill)
    return skill.strip()

def normalize_education(edu: str) -> dict:
    s = (edu or "").lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    if any(k in s for k in ["phd", "doctorate", "doctor"]):
        level = 4
    elif any(k in s for k in ["master", "ms", "msc", "mba"]):
        level = 3
    elif any(k in s for k in ["bachelor", "ba", "bs", "bsc"]):
        level = 2
    elif any(k in s for k in ["associate", "diploma"]):
        level = 1
    else:
        level = 0

    return {"level": level, "text": s}

PRIMARY_WEIGHT = 50
SECONDARY_WEIGHT = 20
EXPERIENCE_WEIGHT = 15
LOCATION_WEIGHT = 5
EDUCATION_WEIGHT = 10

def match_resumes(job_id: str, top_n: int = 5):
    job = job_collection.find_one({"job_id": job_id})
    if not job:
        print("Job not found")
        return

    resumes = list(resume_collection.find({}, {"_id":0, "candidate_id":1,"name":1,"email":1,"skills":1,"experience_years":1,"location":1,"education":1}))
    if not resumes:
        print(" No resumes found")
        return

    results = []

    job_primary = set(normalize_skill(s) for s in job.get("primary_skills", []))
    job_secondary = set(normalize_skill(s) for s in job.get("secondary_skills", []))
    job_location = (job.get("location") or "").lower().strip()
    job_exp = job.get("min_experience", 0)
    job_edu = normalize_education(job.get("education", ""))

    for resume in resumes:
        score_data = score_resume(resume, job_primary, job_secondary, job_location, job_exp, job_edu)
        results.append(score_data)

    results.sort(key=lambda x: x["score"], reverse=True)

    print(f"\nScores for Job ID: {job_id}\n{'='*70}")
    for r in results[:top_n]:
        print(f"""
            Name: {r['name']}
            Email: {r['email']}
            Primary Skill Score:   {r['primary_score']}
            Secondary Skill Score: {r['secondary_score']}
            Experience Score:      {r['experience_score']}
            Location Score:        {r['location_score']}
            Education Score:       {r['education_score']}
            -----------------------------------
            TOTAL SCORE: {r['score']}%
            """)

def score_resume(resume, job_primary, job_secondary, job_location, job_exp, job_edu):
    resume_skills = set(normalize_skill(s) for s in resume.get("skills", []))
    resume_exp = resume.get("experience_years", 0)
    resume_loc = (resume.get("location") or "").lower().strip()
    resume_edu = normalize_education(resume.get("education", ""))

    primary_matched = resume_skills & job_primary
    secondary_matched = resume_skills & job_secondary

    primary_score = (len(primary_matched) / max(len(job_primary),1)) * PRIMARY_WEIGHT
    secondary_score = (len(secondary_matched) / max(len(job_secondary),1)) * SECONDARY_WEIGHT
    experience_score = EXPERIENCE_WEIGHT if resume_exp>=job_exp else (resume_exp/max(job_exp,1))*EXPERIENCE_WEIGHT
    location_score = LOCATION_WEIGHT if (not job_location or job_location=="not specified" or resume_loc in job_location or job_location in resume_loc) else 0
    education_score = EDUCATION_WEIGHT if not job_edu["text"] or resume_edu["level"]>=job_edu["level"] else 0

    total_score = round(primary_score+secondary_score+experience_score+location_score+education_score,2)

    return {
        "candidate_id": resume.get("candidate_id"),
        "name": resume.get("name","Unknown"),
        "email": resume.get("email","N/A"),
        "score": total_score,
        "primary_score": round(primary_score,2),
        "secondary_score": round(secondary_score,2),
        "experience_score": round(experience_score,2),
        "location_score": round(location_score,2),
        "education_score": round(education_score,2),
    }
