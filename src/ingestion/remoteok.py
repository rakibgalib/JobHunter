from datetime import datetime
import requests
from src.ingestion.base import SourceAdapter
from src.ingestion.models import JobPosting

API_URL = "https://remoteok.com/api"


class RemoteOKAdapter(SourceAdapter):
    name = "remoteok"

    def __init__(self, api_url: str = API_URL):
        self.api_url = api_url

    def fetch(self) -> list[JobPosting]:
        response = requests.get(self.api_url, headers={"User-Agent": "job-agent/1.0"}, timeout=10)
        response.raise_for_status()
        postings = []
        for item in response.json():
            if "id" not in item:
                continue  # legal-notice entry, not a job
            posted_at = None
            if item.get("date"):
                posted_at = datetime.fromisoformat(item["date"])
            postings.append(
                JobPosting(
                    source=self.name,
                    source_job_id=str(item["id"]),
                    title=item.get("position", ""),
                    company=item.get("company", "Unknown"),
                    description=item.get("description", ""),
                    url=item.get("url", ""),
                    location=item.get("location") or "Remote",
                    employment_type="unknown",
                    salary_raw=item.get("salary") or None,
                    posted_at=posted_at,
                )
            )
        return postings
