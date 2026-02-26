from pymongo import MongoClient, ASCENDING
from config.settings import MONGO_URI

client = MongoClient(MONGO_URI)
db = client["Job_Matcher"]

resume_collection = db["resumes"]
job_collection = db["jobs"]

resume_collection.create_index("email", unique=True)
resume_collection.create_index("candidate_id", unique=True)
job_collection.create_index("job_id", unique=True)

resume_collection.create_index("primary_skills")
resume_collection.create_index("secondary_skills")

job_collection.create_index("primary_skills")
job_collection.create_index("secondary_skills")

print("✅ MongoDB indexes created successfully!")