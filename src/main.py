import os
from parsers.resume_parser import parse_resume
from parsers.jd_parser import parse_jd
from matcher.matcher import match_resumes
from config.settings import RESUME_INPUT_DIR, JD_INPUT_DIR


def menu():
    print("\n=== Resume Matching System ===")
    print("1. Parse resumes")
    print("2. Parse job descriptions")
    print("3. Match resumes with job")
    print("4. Exit")


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
            job_id = input("Enter Job ID: ").strip()
            top_n = int(input("How many top candidates? ").strip())
            match_resumes(job_id, top_n)

        elif choice == "4":
            print("👋 Exiting...")
            break

        else:
            print("❌ Invalid option")
