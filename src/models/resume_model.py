from pydantic import BaseModel
from typing import List

class ResumeModel(BaseModel):
    candidate_id: str
    skills: List[str]
    experience_years: int
    location: str
    education: str
