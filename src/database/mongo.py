from pymongo import MongoClient
from config.settings import MONGO_URI

if not MONGO_URI:
    raise ValueError("MONGO_URI not set in environment variables")

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
client.server_info()

db = client["job_matcher_db"]

resume_collection = db["resumes"]
job_collection = db["jobs"]
approved_collection = db["approved_candidates"]

def create_indexes():
    resume_collection.create_index("email", unique=True)
    job_collection.create_index("job_id", unique=True)
    approved_collection.create_index(
        [("candidate_id", 1), ("job_id", 1)],
        unique=True
    )

create_indexes()
