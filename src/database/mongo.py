from pymongo import MongoClient
from config.settings import MONGO_URI

client = MongoClient(MONGO_URI)

db = client["job_matcher_db"]

resume_collection = db["resumes"]
job_collection = db["jobs"]
approved_collection = db["approved_candidates"]


# -------- Create Indexes (safe if already exists) --------
def create_indexes():
    resume_collection.create_index("email", unique=True)

    job_collection.create_index("job_id", unique=True)

    approved_collection.create_index(
        [("candidate_id", 1), ("job_id", 1)],
        unique=True
    )


# Call once when app starts
create_indexes()
