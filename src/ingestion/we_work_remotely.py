from datetime import datetime
import feedparser
from src.ingestion.base import SourceAdapter
from src.ingestion.models import JobPosting

FEED_URL = "https://weworkremotely.com/categories/remote-programming-jobs.rss"


class WeWorkRemotelyAdapter(SourceAdapter):
    name = "we_work_remotely"

    def __init__(self, feed_url: str = FEED_URL):
        self.feed_url = feed_url

    def fetch(self) -> list[JobPosting]:
        parsed = feedparser.parse(self.feed_url)
        postings = []
        for entry in parsed.entries:
            company, sep, title = entry.title.partition(": ")
            if not sep:
                company, title = "Unknown", entry.title
            posted_at = None
            if entry.get("published_parsed"):
                posted_at = datetime(*entry.published_parsed[:6])
            postings.append(
                JobPosting(
                    source=self.name,
                    source_job_id=entry.get("id", entry.link),
                    title=title,
                    company=company,
                    description=entry.get("summary", ""),
                    url=entry.link,
                    location="Remote",
                    employment_type="unknown",
                    salary_raw=None,
                    posted_at=posted_at,
                )
            )
        return postings
