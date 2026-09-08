import requests
from bs4 import BeautifulSoup
from src.ingestion.base import SourceAdapter
from src.ingestion.models import JobPosting

LISTING_URL = "https://nodesk.co/remote-jobs/"


class NoDeskAdapter(SourceAdapter):
    name = "nodesk"

    def __init__(self, listing_url: str = LISTING_URL):
        self.listing_url = listing_url

    def fetch(self) -> list[JobPosting]:
        response = requests.get(self.listing_url, headers={"User-Agent": "job-agent/1.0"}, timeout=10)
        response.raise_for_status()
        return self.parse_html(response.text)

    def parse_html(self, html: str) -> list[JobPosting]:
        soup = BeautifulSoup(html, "html.parser")
        postings = []
        for card in soup.select("a.job-card"):
            href = card.get("href", "")
            url = href if href.startswith("http") else f"https://nodesk.co{href}"
            source_job_id = url.rstrip("/").rsplit("/", 1)[-1]
            title = card.select_one(".job-card__title")
            company = card.select_one(".job-card__company")
            location = card.select_one(".job-card__location")
            postings.append(
                JobPosting(
                    source=self.name,
                    source_job_id=source_job_id,
                    title=title.get_text(strip=True) if title else "",
                    company=company.get_text(strip=True) if company else "Unknown",
                    description="",
                    url=url,
                    location=location.get_text(strip=True) if location else "Remote",
                    employment_type="unknown",
                    salary_raw=None,
                    posted_at=None,
                )
            )
        return postings
