from src.ingestion.models import JobPosting


def passes_employment_filter(job: JobPosting, preferences: dict) -> bool:
    if job.employment_type == "full_time":
        return preferences.get("accepting_full_time", True)
    if job.employment_type == "part_time":
        return preferences.get("accepting_part_time", True)
    return True
