import re
from database.mongo import resume_collection, job_collection, approved_collection

# ================== NORMALIZERS ==================

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


# ================== WEIGHTS ==================

PRIMARY_WEIGHT = 50
SECONDARY_WEIGHT = 20
EXPERIENCE_WEIGHT = 15
LOCATION_WEIGHT = 5
EDUCATION_WEIGHT = 10


# ================== CORE MATCHER ==================

def match_resumes(job_id: str, top_n: int = 5):

    # 🔁 ALWAYS read fresh job from DB
    job = job_collection.find_one({"job_id": job_id})
    if not job:
        print("❌ Job not found")
        return

    resumes = list(resume_collection.find())
    if not resumes:
        print("❌ No resumes found")
        return

    results = []

    # ---- Normalize job fields ONCE (performance boost) ----
    job_primary = set(normalize_skill(s) for s in job.get("primary_skills", []))
    job_secondary = set(normalize_skill(s) for s in job.get("secondary_skills", []))
    job_location = (job.get("location") or "").lower().strip()
    job_exp = job.get("min_experience", 0)
    job_edu = normalize_education(job.get("education", ""))

    #  Clear old approvals for this job (VERY IMPORTANT)
    approved_collection.delete_many({"job_id": job_id})

    for resume in resumes:
        score_data = score_resume(
            resume,
            job_primary,
            job_secondary,
            job_location,
            job_exp,
            job_edu,
        )

        results.append(score_data)

      
        if score_data["score"] >= 50:
            approved_collection.update_one(
                {
                    "candidate_id": resume["candidate_id"],
                    "job_id": job_id,
                },
                {
                    "$set": {
                        **score_data,
                        "job_id": job_id,
                    }
                },
                upsert=True,
            )

    # Sort by score DESC
    results.sort(key=lambda x: x["score"], reverse=True)

    # ================== OUTPUT ==================

    print(f"\n Scores for Job ID: {job_id}")
    print("=" * 70)

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
STATUS: {r['status']}
""")

    approved_count = approved_collection.count_documents({"job_id": job_id})
    print(f"✅ Approved Candidates (>=60%): {approved_count}")


# ================== SCORING ENGINE ==================

def score_resume(
    resume: dict,
    job_primary: set,
    job_secondary: set,
    job_location: str,
    job_exp: int,
    job_edu: dict,
):

    # ---- Resume normalization ----
    resume_skills = set(normalize_skill(s) for s in resume.get("skills", []))
    resume_exp = resume.get("experience_years", 0)
    resume_loc = (resume.get("location") or "").lower().strip()
    resume_edu = normalize_education(resume.get("education", ""))

    # ---- Skill matching (FAST) ----
    primary_matched = resume_skills & job_primary
    secondary_matched = resume_skills & job_secondary

    primary_score = (len(primary_matched) / max(len(job_primary), 1)) * PRIMARY_WEIGHT
    secondary_score = (len(secondary_matched) / max(len(job_secondary), 1)) * SECONDARY_WEIGHT

    # ---- Experience ----
    experience_score = (
        EXPERIENCE_WEIGHT
        if resume_exp >= job_exp
        else (resume_exp / max(job_exp, 1)) * EXPERIENCE_WEIGHT
    )

    # ---- Location (FIXED & DYNAMIC) ----
    if not job_location or job_location == "not specified":
        location_score = LOCATION_WEIGHT
    elif resume_loc and (resume_loc in job_location or job_location in resume_loc):
        location_score = LOCATION_WEIGHT
    else:
        location_score = 0

    # ---- Education ----
    if not job_edu["text"]:
        education_score = EDUCATION_WEIGHT
    elif resume_edu["level"] >= job_edu["level"]:
        education_score = EDUCATION_WEIGHT
    else:
        education_score = 0

    # ---- Final score ----
    total_score = round(
        primary_score
        + secondary_score
        + experience_score
        + location_score
        + education_score,
        2,
    )

    return {
        "candidate_id": resume.get("candidate_id"),
        "name": resume.get("name"),
        "email": resume.get("email"),
        "score": total_score,
        "primary_score": round(primary_score, 2),
        "secondary_score": round(secondary_score, 2),
        "experience_score": round(experience_score, 2),
        "location_score": round(location_score, 2),
        "education_score": round(education_score, 2),
        "status": "APPROVED" if total_score >= 50 else "REJECTED",
    }
