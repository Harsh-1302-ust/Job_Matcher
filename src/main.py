import os
import asyncio
from parsers.resume_parser import parse_resume
from parsers.jd_parser import parse_jd
from matcher.matcher import match_resumes
from config.settings import RESUME_INPUT_DIR, JD_INPUT_DIR
from database.mongo import job_collection

def menu():
    print("\n=== Resume Matching System ===")
    print("1. Parse resumes")
    print("2. Parse job descriptions")
    print("3. Match resumes with job")
    print("4. Exit")

def list_job_ids():
    jobs = job_collection.find({}, {"job_id":1,"_id":0})
    return [job["job_id"] for job in jobs]

async def parse_all_resumes():
    pdf_files = [os.path.join(RESUME_INPUT_DIR,f) for f in os.listdir(RESUME_INPUT_DIR) if f.endswith(".pdf")]
    await asyncio.gather(*(parse_resume(f) for f in pdf_files))

async def parse_all_jds():
    pdf_files = [os.path.join(JD_INPUT_DIR,f) for f in os.listdir(JD_INPUT_DIR) if f.endswith(".pdf")]
    await asyncio.gather(*(parse_jd(f) for f in pdf_files))

if __name__ == "__main__":
    while True:
        menu()
        choice = input("Enter choice: ").strip()

        if choice=="1":
            print("\nParsing resumes...")
            asyncio.run(parse_all_resumes())
        elif choice=="2":
            print("\nParsing JDs...")
            asyncio.run(parse_all_jds())
        elif choice=="3":
            job_ids = list_job_ids()
            if not job_ids:
                print("No Job Descriptions found.")
                continue
            for idx,jid in enumerate(job_ids,1):
                print(f"{idx}. {jid}")
            selected = input("Select Job ID: ").strip()
            if not selected:
                job_id = job_ids[0]
            elif selected.isdigit() and 1<=int(selected)<=len(job_ids):
                job_id = job_ids[int(selected)-1]
            else:
                job_id = selected
                if job_id not in job_ids:
                    print("Invalid Job ID")
                    continue
            try:
                top_n_input = input("Top N candidates to display: ").strip()
                top_n = int(top_n_input) if top_n_input else 5
            except:
                top_n=5
            match_resumes(job_id, top_n)
        elif choice=="4":
            print("Exiting...")
            break
        else:
            print("Invalid option")
