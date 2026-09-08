import pytest
from src.ingestion.base import SourceAdapter
from src.ingestion.models import JobPosting


def test_job_posting_holds_expected_fields():
    job = JobPosting(
        source="test", source_job_id="1", title="Engineer", company="Acme",
        description="desc", url="http://x", location="Remote",
        employment_type="full_time", salary_raw=None, posted_at=None,
    )
    assert job.title == "Engineer"
    assert job.employment_type == "full_time"


def test_source_adapter_cannot_be_instantiated_directly():
    with pytest.raises(TypeError):
        SourceAdapter()


def test_source_adapter_subclass_must_implement_fetch():
    class Incomplete(SourceAdapter):
        name = "incomplete"

    with pytest.raises(TypeError):
        Incomplete()

    class Complete(SourceAdapter):
        name = "complete"

        def fetch(self):
            return []

    assert Complete().fetch() == []
