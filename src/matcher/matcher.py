import os
import json
import re
from config.settings import RESUME_JSON_PATH, JOB_JSON_PATH, APPROVED_JSON_PATH


# Lowercase, remove punctuation, normalize for matching.
def normalize_skill(skill: str) -> str:
    skill = (skill or "").lower()
    skill = re.sub(r'[^a-z0-9\s]', ' ', skill)
    skill = re.sub(r'\s+', ' ', skill)
    return skill.strip()


def normalize_education(edu: str) -> dict:
    s = (edu or "").lower()
    s = re.sub(r'[^a-z0-9\s]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()

    if any(k in s for k in ['phd', 'doctorate', 'doctor']):
        lvl = 4
    elif any(k in s for k in ['master', "ms", "m.sc", "msc", 'mba']):
        lvl = 3
    elif any(k in s for k in ['bachelor', "ba", "bs", "b.sc", "bsc"]):
        lvl = 2
    elif any(k in s for k in ['associate', 'diploma']):
        lvl = 1
    elif s:
        lvl = 1
    else:
        lvl = 0

    return {"level": lvl, "text": s}

PRIMARY_WEIGHT = 50
SECONDARY_WEIGHT = 20
EXPERIENCE_WEIGHT = 15
LOCATION_WEIGHT = 5
EDUCATION_WEIGHT = 10

# Return items from target_list that appear in any item of source_list.
def matched(target_list, source_list):
    
    matched_items = []
    for t in target_list:
        if not t:
            continue
        for s in source_list:
            if not s:
                continue
            if t in s or s in t:
                matched_items.append(t)
                break
    return matched_items


# Match resumes against a specific job ID, print top N approved candidates, and save them.
def match_resumes(job_id: str, top_n: int = 5, show_all: bool = False):

    if not os.path.exists(RESUME_JSON_PATH):
        print(" No resumes found.")
        return
    with open(RESUME_JSON_PATH) as f:
        resumes = json.load(f)

    if not os.path.exists(JOB_JSON_PATH):
        print(" No jobs found.")
        return
    with open(JOB_JSON_PATH) as f:
        jobs = json.load(f)

    job = next((j for j in jobs if j["job_id"].upper() == job_id.upper()), None)
    if not job:
        print(f" Job ID '{job_id}' not found in jobs.json")
        return

    all_scored = []

    for resume in resumes:
        scored = score_resume(resume, job)
        scored["job_id"] = job_id
        all_scored.append(scored)

    approved_only = [s for s in all_scored if s["status"] == "APPROVED"]

    if not approved_only and not show_all:
        print(f"\n No approved candidates for Job {job_id} (score < 60%)")
        return

    all_scored.sort(key=lambda x: x["percentage"], reverse=True)
    approved_only.sort(key=lambda x: x["percentage"], reverse=True)

    if show_all:
        print(f"\n All Candidates for Job {job_id} (showing {len(all_scored)})\n")
        for i, r in enumerate(all_scored, 1):
            print(f"{i}. {r['name']} ({r['email']})")
            print(f"   Score: {r['percentage']}% → {r['status']}")
            print(f"   Experience: {r['experience_years']} yrs")
            print(f"   Location Match: {r['location_match']}")
            print(f"   Education Match: {r.get('education_match', 'N/A')}")
            print(f"   Primary Skills Matched: {', '.join(r['primary_matched'])}")
            print(f"   Secondary Skills Matched: {', '.join(r['secondary_matched'])}")
            print("-" * 50)
    else:
        approved_top = approved_only[:top_n]
        print(f"\n Top {len(approved_top)} Candidates for Job {job_id}\n")
        for i, r in enumerate(approved_top, 1):
            print(f"{i}. {r['name']} ({r['email']})")
            print(f"   Score: {r['percentage']}% → {r['status']}")
            print(f"   Experience: {r['experience_years']} yrs")
            print(f"   Location Match: {r['location_match']}")
            print(f"   Education Match: {r.get('education_match', 'N/A')} ")
            print(f"   Primary Skills Matched: {', '.join(r['primary_matched'])}")
            print(f"   Secondary Skills Matched: {', '.join(r['secondary_matched'])}")
            print("-" * 50)

    if approved_only:
        save_approved(approved_only[:top_n])


def score_resume(resume: dict, job: dict) -> dict:
    # Primary skills -50 points
    # Secondary skills - 20 points
    # Experience - 20 points
    # Location - 10 points
    # Total = 100 points
    # Threshold for approval = 60%
    
    resume_skills = [normalize_skill(s) for s in resume.get("skills", [])]
    primary = [normalize_skill(s) for s in job.get("primary_skills", [])]
    secondary = [normalize_skill(s) for s in job.get("secondary_skills", [])]

    primary_matched = matched(primary, resume_skills)
    secondary_matched = matched(secondary, resume_skills)

    primary_score = (len(primary_matched) / max(len(primary), 1)) * PRIMARY_WEIGHT
    secondary_score = (len(secondary_matched) / max(len(secondary), 1)) * SECONDARY_WEIGHT

    # Experience score 
    resume_exp = resume.get("experience_years", 0)
    job_exp = job.get("min_experience", 0)
    experience_score = EXPERIENCE_WEIGHT if resume_exp >= job_exp else (resume_exp / max(job_exp, 1)) * EXPERIENCE_WEIGHT

    # Location score 
    resume_loc = resume.get("location", "").strip().lower()
    job_locs = [l.strip().lower() for l in job.get("location", "").split(",") if l.strip()]
    location_score = LOCATION_WEIGHT if resume_loc and resume_loc in job_locs else 0

    # Education score 
    job_edu = job.get("education", "")
    resume_edu = resume.get("education", "")
    job_edu_n = normalize_education(job_edu)
    resume_edu_n = normalize_education(resume_edu)

    if not job_edu_n["text"]:
        education_score = EDUCATION_WEIGHT
        education_match = "Not specified"
    else:
        if resume_edu_n["level"] >= job_edu_n["level"] and resume_edu_n["level"] > 0:
            education_score = EDUCATION_WEIGHT
            education_match = "Yes"
        else:
            # partial match if major/subject words appear in resume education text
            degree_words = ['phd','doctorate','doctor','master','ms','m.sc','msc','mba','bachelor','ba','bs','b.sc','bsc','associate','diploma','highschool']
            job_words = [w for w in job_edu_n['text'].split() if w not in degree_words]
            partial = any(w in resume_edu_n['text'] for w in job_words) if job_words else False
            if partial:
                education_score = round(EDUCATION_WEIGHT * 0.5, 2)
                education_match = "Partial"
            else:
                education_score = 0
                education_match = "No"

    total = round(primary_score + secondary_score + experience_score + location_score + education_score, 2)
    status = "APPROVED" if total >= 60 else "REJECTED"

    return {
        "candidate_id": resume.get("candidate_id"),
        "name": resume.get("name", "Unknown"),
        "email": resume.get("email", "N/A"),
        "experience_years": resume_exp,
        "percentage": total,
        "status": status,
        "location_match": "Yes" if location_score else "No",
        "primary_matched": primary_matched,
        "secondary_matched": secondary_matched,
        "education": resume_edu,
        "education_match": education_match,
        "education_score": education_score,
        "education_level": resume_edu_n.get("level", 0),
    }

# Save approved candidates to JSON without duplicates
def save_approved(approved_candidates: list):
    if not approved_candidates:
        return

    os.makedirs(os.path.dirname(APPROVED_JSON_PATH), exist_ok=True)

    if os.path.exists(APPROVED_JSON_PATH):
        with open(APPROVED_JSON_PATH, "r") as f:
            existing = json.load(f)
    else:
        existing = []

    existing_keys = {(c["candidate_id"], c["job_id"]) for c in existing}
    for cand in approved_candidates:
        key = (cand["candidate_id"], cand["job_id"])
        if key not in existing_keys:
            existing.append(cand)

    with open(APPROVED_JSON_PATH, "w") as f:
        json.dump(existing, f, indent=2)
