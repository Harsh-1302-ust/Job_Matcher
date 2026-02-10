from pydantic import BaseModel
from typing import List, Optional

class JobModel(BaseModel):
    job_id: str
    job_title: Optional[str] = None
    primary_skills: List[str]
    secondary_skills: List[str] = []
    min_experience: int
    max_experience: Optional[int] = None
    location: str
    education: str
