from matcher.matcher import match_resumes

if __name__ == '__main__':
    # Run matching for jd1 and show top 5 candidates
    match_resumes(job_id='Application_Support_Automation_Engineer_JD', top_n=5)
