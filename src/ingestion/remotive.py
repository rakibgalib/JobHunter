from datetime import datetime
import requests
from src.ingestion.base import SourceAdapter
from src.ingestion.models import JobPosting

API_URL = "https://remotive.com/api/remote-jobs"

EMPLOYMENT_TYPE_MAP = {
    "full_time": "full_time",
    "part_time": "part_time",
    "contract": "contract",
    "freelance": "contract",
}


class RemotiveAdapter(SourceAdapter):
    name = "remotive"

    def __init__(self, api_url: str = API_URL):
        self.api_url = api_url

    def fetch(self) -> list[JobPosting]:
        response = requests.get(self.api_url, timeout=10)
        response.raise_for_status()
        postings = []
        for item in response.json().get("jobs", []):
            posted_at = None
            if item.get("publication_date"):
                posted_at = datetime.fromisoformat(item["publication_date"].replace("Z", "+00:00"))
            postings.append(
                JobPosting(
                    source=self.name,
                    source_job_id=str(item["id"]),
                    title=item.get("title", ""),
                    company=item.get("company_name", "Unknown"),
                    description=item.get("description", ""),
                    url=item.get("url", ""),
                    location=item.get("candidate_required_location") or "Remote",
                    employment_type=EMPLOYMENT_TYPE_MAP.get(item.get("job_type"), "unknown"),
                    salary_raw=item.get("salary") or None,
                    posted_at=posted_at,
                )
            )
        return postings
