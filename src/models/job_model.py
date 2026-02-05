from pydantic import BaseModel
from typing import List


class JobModel(BaseModel):
    job_id: str
    primary_skills: List[str]
    secondary_skills: List[str]
    min_experience: int
    location: str
    education: str
