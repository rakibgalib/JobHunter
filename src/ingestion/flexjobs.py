import requests
from bs4 import BeautifulSoup
from src.ingestion.base import SourceAdapter
from src.ingestion.models import JobPosting

LISTING_URL = "https://www.flexjobs.com/remote-jobs"
BASE_URL = "https://www.flexjobs.com"


class FlexJobsAdapter(SourceAdapter):
    name = "flexjobs"

    def __init__(self, listing_url: str = LISTING_URL):
        self.listing_url = listing_url

    def fetch(self) -> list[JobPosting]:
        response = requests.get(self.listing_url, headers={"User-Agent": "job-agent/1.0"}, timeout=10)
        response.raise_for_status()
        return self.parse_html(response.text)

    def parse_html(self, html: str) -> list[JobPosting]:
        soup = BeautifulSoup(html, "html.parser")
        postings = []
        for teaser in soup.select("li.job-teaser"):
            link = teaser.select_one(".job-teaser__link")
            company = teaser.select_one(".job-teaser__company")
            location = teaser.select_one(".job-teaser__location")
            snippet = teaser.select_one(".job-teaser__snippet")
            href = link.get("href", "") if link else ""
            job_id = teaser.get("data-job-id", "") or href
            if not job_id:
                continue
            url = href if href.startswith("http") else f"{BASE_URL}{href}"
            postings.append(
                JobPosting(
                    source=self.name,
                    source_job_id=job_id,
                    title=link.get_text(strip=True) if link else "",
                    company=company.get_text(strip=True) if company else "Unknown",
                    description=snippet.get_text(strip=True) if snippet else "",
                    url=url,
                    location=location.get_text(strip=True) if location else "Remote",
                    employment_type="unknown",
                    salary_raw=None,
                    posted_at=None,
                )
            )
        return postings
