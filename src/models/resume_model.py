from pydantic import BaseModel
from typing import List, Optional

class ResumeModel(BaseModel):
    candidate_id: str
    email: Optional[str] = None
    name: Optional[str] = None
    skills: List[str]
    experience_years: int
    location: str
    education: str
