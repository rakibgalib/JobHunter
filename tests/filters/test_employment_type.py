from src.ingestion.models import JobPosting
from src.filters.employment_type import passes_employment_filter


def make_job(employment_type):
    return JobPosting(
        source="test", source_job_id="1", title="Engineer", company="Acme",
        description="", url="http://x", location="Remote",
        employment_type=employment_type, salary_raw=None, posted_at=None,
    )


def test_full_time_blocked_when_not_accepting_full_time():
    prefs = {"accepting_full_time": False, "accepting_part_time": True}
    assert passes_employment_filter(make_job("full_time"), prefs) is False


def test_part_time_allowed_when_accepting_part_time():
    prefs = {"accepting_full_time": False, "accepting_part_time": True}
    assert passes_employment_filter(make_job("part_time"), prefs) is True


def test_contract_and_unknown_always_pass():
    prefs = {"accepting_full_time": False, "accepting_part_time": False}
    assert passes_employment_filter(make_job("contract"), prefs) is True
    assert passes_employment_filter(make_job("unknown"), prefs) is True
