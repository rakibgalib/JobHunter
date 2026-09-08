from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class JobPosting:
    source: str
    source_job_id: str
    title: str
    company: str
    description: str
    url: str
    location: str
    employment_type: str  # 'full_time' | 'part_time' | 'contract' | 'unknown'
    salary_raw: Optional[str] = None
    posted_at: Optional[datetime] = None
