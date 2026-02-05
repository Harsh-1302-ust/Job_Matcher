from matcher.matcher import match_resumes

if __name__ == '__main__':
    # non-interactive debug runner for job 'jd1' (show all candidates)
    match_resumes('Application_Support_Automation_Engineer_JD', top_n=5, show_all=True)
