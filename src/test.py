from matcher.matcher import match_resumes

if __name__ == '__main__':
    # non-interactive debug runner for job 'jd1' (show all candidates)
    match_resumes('JD_Data_Analyst', top_n=5, show_all=True)
