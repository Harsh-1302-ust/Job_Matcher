import json
import os
from parsers.resume_parser import parse_resume
from parsers.jd_parser import parse_jd
from matcher.matcher import match_resumes
from config.settings import JOB_JSON_PATH, RESUME_INPUT_DIR, JD_INPUT_DIR


def menu():
    print("\n=== Resume Matching System ===")
    print("1. Parse resumes")
    print("2. Parse job descriptions")
    print("3. Match resumes with job")
    print("4. Exit")

def list_job_ids():
    # List all available Job IDs from jobs.json
    if not os.path.exists(JOB_JSON_PATH):
        return []
    with open(JOB_JSON_PATH) as f:
        jobs = json.load(f)
    return [job["job_id"] for job in jobs]


if __name__ == "__main__":
    while True:
        menu()
        choice = input("Enter choice: ").strip()

        if choice == "1":
            print("\nParsing resumes...")
            for file in os.listdir(RESUME_INPUT_DIR):
                if file.endswith(".pdf"):
                    parse_resume(os.path.join(RESUME_INPUT_DIR, file))

        elif choice == "2":
            print("\nParsing job descriptions...")
            for file in os.listdir(JD_INPUT_DIR):
                if file.endswith(".pdf"):
                    parse_jd(os.path.join(JD_INPUT_DIR, file))

        elif choice == "3":
            job_ids = list_job_ids()
            if not job_ids:
                print("No Job Descriptions found.")
                continue

            print("\nAvailable Job IDs:")
            for idx, jid in enumerate(job_ids, 1):
                print(f"{idx}. {jid}")

            selected = input("Select Job ID : ").strip()

            if not selected:
                job_id = job_ids[0]
            elif selected.isdigit() and 1 <= int(selected) <= len(job_ids):
                job_id = job_ids[int(selected) - 1]
            else:
                job_id = selected.upper()
                if job_id not in job_ids:
                    print("Invalid Job ID")
                    continue

            try:
                top_n_input = input("How many top candidates to display: ").strip()
                top_n = int(top_n_input) if top_n_input else 5
            except ValueError:
                top_n = 5

            match_resumes(job_id, top_n)

        elif choice == "4":
            print(" Exiting...")
            break

        else:
            print(" Invalid option")






