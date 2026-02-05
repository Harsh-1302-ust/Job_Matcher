import os
import json
import re
from config.settings import RESUME_JSON_PATH, JOB_JSON_PATH, APPROVED_JSON_PATH

def normalize_skill(skill: str) -> str:
    """Lowercase, remove punctuation, normalize for matching.

    This version removes non-alphanumeric characters (keeps spaces) and
    collapses whitespace so that variants like 'React.js' -> 'reactjs' and
    'RESTful APIs' -> 'restful apis' become comparable. Matching later
    uses substring checks so 'react' will match 'reactjs'.
    """
    skill = (skill or "").lower()
    # replace any non-alphanumeric character with a space (keeps words separate)
    skill = re.sub(r'[^a-z0-9\s]', ' ', skill)
    # collapse multiple spaces
    skill = re.sub(r'\s+', ' ', skill)
    return skill.strip()


def match_resumes(job_id: str, top_n: int = 5, show_all: bool = False):
    """
    Match resumes against a specific job ID, print top N approved candidates, and save them.
    """
    # Load resumes
    if not os.path.exists(RESUME_JSON_PATH):
        print("❌ No resumes found. Parse resumes first.")
        return
    with open(RESUME_JSON_PATH) as f:
        resumes = json.load(f)

    # Load jobs
    if not os.path.exists(JOB_JSON_PATH):
        print("❌ No jobs found. Parse JDs first.")
        return
    with open(JOB_JSON_PATH) as f:
        jobs = json.load(f)

    # Find the specific job
    job = next((j for j in jobs if j["job_id"].upper() == job_id.upper()), None)
    if not job:
        print(f"❌ Job ID '{job_id}' not found in jobs.json")
        return

    all_scored = []

    # Score each resume
    for resume in resumes:
        scored = score_resume(resume, job)
        scored["job_id"] = job_id
        all_scored.append(scored)

    # Approved-only list (used for saving or normal output)
    approved_only = [s for s in all_scored if s["status"] == "APPROVED"]

    if not approved_only and not show_all:
        print(f"\n❌ No approved candidates for Job {job_id} (score < 60%)")
        return

    # Sort descending by percentage
    all_scored.sort(key=lambda x: x["percentage"], reverse=True)
    approved_only.sort(key=lambda x: x["percentage"], reverse=True)

    if show_all:
        # Print all candidates (debug mode)
        print(f"\n🔎 All Candidates for Job {job_id} (showing {len(all_scored)})\n")
        for i, r in enumerate(all_scored, 1):
            print(f"{i}. {r['name']} ({r['email']})")
            print(f"   Score: {r['percentage']}% → {r['status']}")
            print(f"   Experience: {r['experience_years']} yrs")
            print(f"   Location Match: {r['location_match']}")
            print(f"   Primary Skills Matched: {', '.join(r['primary_matched'])}")
            print(f"   Secondary Skills Matched: {', '.join(r['secondary_matched'])}")
            print("-" * 50)
    else:
        # Print top approved candidates
        approved_top = approved_only[:top_n]
        print(f"\n🏆 Top {len(approved_top)} Candidates for Job {job_id}\n")
        for i, r in enumerate(approved_top, 1):
            print(f"{i}. {r['name']} ({r['email']})")
            print(f"   Score: {r['percentage']}% → {r['status']}")
            print(f"   Experience: {r['experience_years']} yrs")
            print(f"   Location Match: {r['location_match']}")
            print(f"   Primary Skills Matched: {', '.join(r['primary_matched'])}")
            print(f"   Secondary Skills Matched: {', '.join(r['secondary_matched'])}")
            print("-" * 50)

    # Save approved candidates (always save approved ones only)
    if approved_only:
        save_approved(approved_only[:top_n])


def score_resume(resume: dict, job: dict) -> dict:
    """
    Primary skills -50 points
    Secondary skills - 20 points
    Experience - 20 points
    Location - 10 points
    Total = 100 points
    Threshold for approval = 60%
    """
    # Normalize lists
    resume_skills = [normalize_skill(s) for s in resume.get("skills", [])]
    primary = [normalize_skill(s) for s in job.get("primary_skills", [])]
    secondary = [normalize_skill(s) for s in job.get("secondary_skills", [])]

   
    def matched(target_list, source_list):
        matched_items = []
        for t in target_list:
            for s in source_list:
                if not t or not s:
                    continue
                if t in s or s in t:
                    matched_items.append(t)
                    break
        return matched_items

   
    primary_matched = matched(primary, resume_skills)
    secondary_matched = matched(secondary, resume_skills)

    primary_score = (len(primary_matched) / max(len(primary), 1)) * 50
    secondary_score = (len(secondary_matched) / max(len(secondary), 1)) * 20

    # Experience score (max 20 points)
    resume_exp = resume.get("experience_years", 0)
    job_exp = job.get("min_experience", 0)
    experience_score = 20 if resume_exp >= job_exp else (resume_exp / max(job_exp, 1)) * 20

    # Location score (max 10 points)
    resume_loc = resume.get("location", "").strip().lower()
    job_locs = [l.strip().lower() for l in job.get("location", "").split(",")]
    location_score = 10 if resume_loc in job_locs else 0

    total = round(primary_score + secondary_score + experience_score + location_score, 2)
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
        "secondary_matched": secondary_matched
    }


def save_approved(approved_candidates: list):
    """Save approved candidates to JSON without duplicates"""
    if not approved_candidates:
        return

    os.makedirs(os.path.dirname(APPROVED_JSON_PATH), exist_ok=True)

    # Load existing approved candidates
    if os.path.exists(APPROVED_JSON_PATH):
        with open(APPROVED_JSON_PATH, "r") as f:
            existing = json.load(f)
    else:
        existing = []

    # Avoid duplicates (candidate_id + job_id)
    existing_keys = {(c["candidate_id"], c["job_id"]) for c in existing}
    for cand in approved_candidates:
        key = (cand["candidate_id"], cand["job_id"])
        if key not in existing_keys:
            existing.append(cand)

    with open(APPROVED_JSON_PATH, "w") as f:
        json.dump(existing, f, indent=2)
