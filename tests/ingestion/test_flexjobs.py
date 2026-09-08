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


def test_parse_html_skips_teaser_with_missing_id_and_href():
    adapter = FlexJobsAdapter()
    postings = adapter.parse_html(FIXTURE_HTML)
    # Fixture contains a second <li class="job-teaser"> with neither a
    # data-job-id nor an href; it must be excluded rather than emitted with
    # a degenerate shared id (the bare BASE_URL).
    assert len(postings) == 1
    assert all(p.title != "Missing Id Job" for p in postings)
