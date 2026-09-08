from src.ingestion.models import JobPosting
from src.filters.blacklist import is_blacklisted

BLACKLIST = {"companies": ["Shady Recruiting Inc"], "keywords": ["unpaid", "commission-only"]}


def make_job(**overrides):
    defaults = dict(
        source="test", source_job_id="1", title="Engineer", company="Acme",
        description="Great role", url="http://x", location="Remote",
        employment_type="full_time", salary_raw=None, posted_at=None,
    )
    defaults.update(overrides)
    return JobPosting(**defaults)


def test_blocks_blacklisted_company_case_insensitive():
    job = make_job(company="shady recruiting inc")
    assert is_blacklisted(job, BLACKLIST) is True


def test_blocks_blacklisted_keyword_in_description():
    job = make_job(description="This is an unpaid internship")
    assert is_blacklisted(job, BLACKLIST) is True


def test_passes_job_matching_neither():
    job = make_job()
    assert is_blacklisted(job, BLACKLIST) is False
