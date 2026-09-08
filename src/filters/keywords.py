from src.ingestion.models import JobPosting


def matches_tech_stack(job: JobPosting, keywords: list[str]) -> bool:
    if not keywords:
        return True
    text = f"{job.title} {job.description}".lower()
    return any(keyword.strip().lower() in text for keyword in keywords)
