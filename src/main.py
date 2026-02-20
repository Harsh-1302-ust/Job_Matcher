import os
import asyncio
from parsers.resume_parser import parse_resume
from parsers.jd_parser import parse_jd
from matcher.matcher import match_resume_to_jobs
from database.mongo import resume_collection

RESUME_DIR = "data/input/resumes"
JD_DIR = "data/input/jd"


# -----------------------------------
# Parse all resumes
# -----------------------------------
async def parse_all_resumes():
    files = [
        os.path.join(RESUME_DIR, f)
        for f in os.listdir(RESUME_DIR)
        if f.lower().endswith(".pdf")
    ]

    if not files:
        print("No resume files found.")
        return

    await asyncio.gather(*(parse_resume(f) for f in files),return_exceptions=True)
    print("All resumes parsed successfully.\n")


# -----------------------------------
# Parse all JDs
# -----------------------------------
async def parse_all_jds():
    files = [
        os.path.join(JD_DIR, f)
        for f in os.listdir(JD_DIR)
        if f.lower().endswith(".pdf")
    ]

    if not files:
        print("No JD files found.")
        return

    await asyncio.gather(*(parse_jd(f) for f in files),return_exceptions=True)
    print("All JDs parsed successfully.\n")


# -----------------------------------
# Main Continuous Menu
# -----------------------------------
def main_menu():
    while True:
        print("\n========== ATS SYSTEM ==========")
        print("1. Parse Resumes")
        print("2. Parse JDs")
        print("3. Match Resume → Top JDs")
        print("4. Exit")

        choice = input("Enter choice: ").strip()

        if choice == "1":
            asyncio.run(parse_all_resumes())

        elif choice == "2":
            asyncio.run(parse_all_jds())

        elif choice == "3":

            resumes = list(resume_collection.find({}, {"candidate_id": 1, "name": 1, "email": 1}))

            if not resumes:
                print("No resumes found in database.\n")
                continue

            print("\nAvailable Candidates:")
            for idx, r in enumerate(resumes, 1):
                print(f"{idx}. {r.get('name', 'Unknown')} | {r.get('email')}")

            selected = input("\nEnter candidate number or candidate_id: ").strip()

            # Handle numeric selection
            if selected.isdigit():
                selected_index = int(selected) - 1
                if 0 <= selected_index < len(resumes):
                    candidate_id = resumes[selected_index]["candidate_id"]
                else:
                    print("Invalid selection.\n")
                    continue
            else:
                candidate_id = selected

            results = match_resume_to_jobs(candidate_id, 10)

            if not results:
                print("No matching jobs found.\n")
            else:
                print("\nTop Matching Jobs:\n")
                for idx, job in enumerate(results, 1):
                    print(f"{idx}. Job ID: {job.get('job_id')}")
                    print(f"   category: {job.get('category', 'N/A')}")
                    print(f"   Technology: {(job.get('technology'))}")
                    print(f"   Score: {job.get('total_score', 0)}")
                    print()


        elif choice == "4":
            print("Exiting system. Goodbye!")
            break

        else:
            print("Invalid choice. Please select 1-4.\n")


if __name__ == "__main__":
    main_menu()
