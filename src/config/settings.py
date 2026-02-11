import os
from dotenv import load_dotenv

load_dotenv()

def get_env(key):
    value = os.getenv(key)
    if not value:
        raise ValueError(f"{key} not set in environment variables")
    return value

AZURE_OPENAI_API_KEY = get_env("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_ENDPOINT = get_env("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_DEPLOYMENT = get_env("AZURE_OPENAI_DEPLOYMENT_NAME")
AZURE_OPENAI_API_VERSION = get_env("AZURE_OPENAI_API_VERSION")
MONGO_URI = get_env("MONGO_URI")

RESUME_INPUT_DIR = "data/input/resumes"
JD_INPUT_DIR = "data/input/jd"

AZURE_API_RPM = 100            # requests per minute
AZURE_API_CONCURRENCY = 20     # max parallel calls

