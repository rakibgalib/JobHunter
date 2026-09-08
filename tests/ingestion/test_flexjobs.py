from pathlib import Path
from src.ingestion.flexjobs import FlexJobsAdapter

FIXTURE_HTML = (Path(__file__).parent.parent / "fixtures" / "flexjobs.html").read_text()


def test_parse_html_extracts_teaser_fields():
    adapter = FlexJobsAdapter()
    postings = adapter.parse_html(FIXTURE_HTML)
    assert len(postings) == 1
    job = postings[0]
    assert job.source == "flexjobs"
    assert job.source_job_id == "4001"
    assert job.title == "Backend Engineer"
    assert job.company == "Acme Corp"
    assert job.description == "Part-time Python role..."
    assert job.url == "https://www.flexjobs.com/remote-jobs/backend-engineer-acme"
    assert job.employment_type == "unknown"
