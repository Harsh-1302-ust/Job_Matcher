import json
from config.settings import RESUME_JSON_PATH, JOB_JSON_PATH
from matcher.matcher import score_resume


def print_scores_for_job(job_id: str):
    with open(RESUME_JSON_PATH) as f:
        resumes = json.load(f)
    with open(JOB_JSON_PATH) as f:
        jobs = json.load(f)
    job = next((j for j in jobs if j.get('job_id','').upper() == job_id.upper()), None)
    if not job:
        print('Job not found')
        return
    for r in resumes:
        scored = score_resume(r, job)
        print(f"{scored['name']}: {scored['percentage']}% -> {scored['status']}")
        print(f"  primary_matched: {scored['primary_matched']}")
        print(f"  secondary_matched: {scored['secondary_matched']}")
        print('-----')


if __name__ == '__main__':
    print_scores_for_job('jd1')
