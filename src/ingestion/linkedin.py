from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from src.ingestion.base import SourceAdapter
from src.ingestion.models import JobPosting

SEARCH_URL = "https://www.linkedin.com/jobs/search/?keywords=remote"
STORAGE_STATE_PATH = "data/linkedin_storage_state.json"


class LinkedInAdapter(SourceAdapter):
    name = "linkedin"

    def __init__(self, storage_state_path: str = STORAGE_STATE_PATH, search_url: str = SEARCH_URL):
        self.storage_state_path = storage_state_path
        self.search_url = search_url

    def fetch(self) -> list[JobPosting]:  # pragma: no cover — requires a live authenticated session
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(storage_state=self.storage_state_path)
            page = context.new_page()
            page.goto(self.search_url)
            page.wait_for_selector(".jobs-search__results-list")
            html = page.content()
            browser.close()
        return self.parse_html(html)

    def parse_html(self, html: str) -> list[JobPosting]:
        soup = BeautifulSoup(html, "html.parser")
        postings = []
        for card in soup.select(".base-card"):
            urn = card.get("data-entity-urn", "")
            job_id = urn.rsplit(":", 1)[-1] if urn else ""
            title = card.select_one(".base-search-card__title")
            company = card.select_one(".base-search-card__subtitle")
            location = card.select_one(".job-search-card__location")
            link = card.select_one(".base-card__full-link")
            postings.append(
                JobPosting(
                    source=self.name,
                    source_job_id=job_id,
                    title=title.get_text(strip=True) if title else "",
                    company=company.get_text(strip=True) if company else "Unknown",
                    description="",
                    url=link.get("href", "") if link else "",
                    location=location.get_text(strip=True) if location else "Remote",
                    employment_type="unknown",
                    salary_raw=None,
                    posted_at=None,
                )
            )
        return postings
