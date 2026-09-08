from src.ingestion.models import JobPosting
from src.filters.keywords import matches_tech_stack


def make_job(title, description=""):
    return JobPosting(
        source="test", source_job_id="1", title=title, company="Acme",
        description=description, url="http://x", location="Remote",
        employment_type="unknown", salary_raw=None, posted_at=None,
    )


def test_matches_keyword_in_title():
    job = make_job("Senior Python Engineer")
    assert matches_tech_stack(job, ["python", "django"]) is True


def test_matches_keyword_in_description_case_insensitive():
    job = make_job("Engineer", description="Must know REACT well")
    assert matches_tech_stack(job, ["react"]) is True


def test_no_match_returns_false():
    job = make_job("Sales Representative")
    assert matches_tech_stack(job, ["python", "react"]) is False


def test_empty_keyword_list_passes_everything():
    job = make_job("Sales Representative")
    assert matches_tech_stack(job, []) is True
