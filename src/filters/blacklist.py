from src.ingestion.models import JobPosting


def is_blacklisted(job: JobPosting, blacklist: dict) -> bool:
    blacklisted_companies = {c.strip().lower() for c in blacklist.get("companies", [])}
    if job.company.strip().lower() in blacklisted_companies:
        return True
    text = f"{job.title} {job.description}".lower()
    return any(keyword.strip().lower() in text for keyword in blacklist.get("keywords", []))
