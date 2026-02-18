from pymongo import MongoClient
from config.settings import MONGO_URI

client = MongoClient(MONGO_URI)
db = client["resume_job_matcher"]

resume_collection = db["resumes"]
job_collection = db["jobs"]

resume_collection.create_index("email", unique=True)
job_collection.create_index("job_id", unique=True)
