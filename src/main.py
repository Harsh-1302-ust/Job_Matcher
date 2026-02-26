import os
import asyncio
from parsers.resume_parser import parse_resume
from parsers.jd_parser import parse_jd
from matcher.matcher import match_job_to_resumes, match_resume_to_jobs
from database.mongo import resume_collection,job_collection

RESUME_DIR = "data/input/resumes"
JD_DIR = "data/input/jd"


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


def main_menu():
    while True:
        try:
            print("\n========== ATS SYSTEM ==========")
            print("1. Parse Resumes")
            print("2. Parse JDs")
            print("3. Match Resume → Top JDs")
            print("4. Match Job → Top Resumes")
            print("5. Exit")

            choice = input("Enter choice: ").strip()

            if choice == "1":
                try:
                    asyncio.run(parse_all_resumes())
                except Exception as e:
                    print(f"Error while parsing resumes: {e}")


            elif choice == "2":
                try:
                    asyncio.run(parse_all_jds())
                except Exception as e:
                    print(f"Error while parsing JDs: {e}")


            elif choice == "3":
                try:
                    resumes = list(
                        resume_collection.find({}, {"candidate_id": 1, "name": 1, "email": 1})
                    )

                    if not resumes:
                        print("No resumes found in database.\n")
                        continue

                    print("\nAvailable Candidates:")
                    for idx, r in enumerate(resumes, 1):
                        print(f"{idx}. {r.get('name', 'Unknown')} | {r.get('email')}")

                    selected = input("\nEnter candidate number or candidate_id: ").strip()
                    number = input("Enter number of top matches to display (default 5): ").strip()

                    if number.isdigit() and int(number) > 0:
                        top_n = int(number)
                    else:
                        top_n = 5

                    if selected.isdigit():
                        selected_index = int(selected) - 1
                        if 0 <= selected_index < len(resumes):
                            candidate_id = resumes[selected_index]["candidate_id"]
                        else:
                            print("Invalid selection.\n")
                            continue
                    else:
                        candidate_id = selected

                    results = match_resume_to_jobs(candidate_id, top_n)

                    if not results:
                        print("No matching jobs found.\n")
                    else:
                        print(f"\nTop {top_n} Matching Jobs:\n")
                        for idx, job in enumerate(results, 1):
                            print(f"{idx}. Job ID: {job.get('job_id')}")
                            print(f"   Category: {job.get('category', 'N/A')}")
                            print(f"   Technology: {job.get('technology')}")
                            print(f"   Score: {job.get('total_score', 0)}")
                            print()

                except Exception as e:
                    print(f"Error during Resume → Job matching: {e}")

            elif choice == "4":
                try:
                    jobs = list(
                        job_collection.find({}, {"job_id": 1, "technology": 1})
                    )

                    if not jobs:
                        print("No jobs found.\n")
                        continue

                    print("\nAvailable Jobs:")
                    for idx, j in enumerate(jobs, 1):
                        print(f"{idx}. {j.get('job_id')} | {j.get('technology')}")

                    selected = input("\nEnter job number or job_id: ").strip()
                    number = input("Enter number of top matches to display (default 5): ").strip()

                    if number.isdigit() and int(number) > 0:
                        top_n = int(number)
                    else:
                        top_n = 5

                    if selected.isdigit():
                        selected_index = int(selected) - 1
                        if 0 <= selected_index < len(jobs):
                            job_id = jobs[selected_index]["job_id"]
                        else:
                            print("Invalid selection.\n")
                            continue
                    else:
                        job_id = selected

                    results = match_job_to_resumes(job_id, top_n)

                    if not results:
                        print("No matching resumes found.\n")
                    else:
                        print(f"\nTop {top_n} Matching Resumes:\n")
                        for idx, r in enumerate(results, 1):
                            print(f"{idx}. {r.get('name')} | {r.get('email')}")
                            print(f"   Score: {r.get('total_score')}")
                            print()

                except Exception as e:
                    print(f"Error during Job → Resume matching: {e}")

            elif choice == "5":
                print("Exiting system. Goodbye!")
                break

            else:
                print("Invalid choice. Please select 1-5.\n")

        except KeyboardInterrupt:
            print("\nProgram interrupted by user. Exiting safely.")
            break

        except Exception as e:
            print(f"Unexpected system error: {e}")


if __name__ == "__main__":
    main_menu()
