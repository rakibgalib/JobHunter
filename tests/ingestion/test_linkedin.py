from pathlib import Path
from src.ingestion.linkedin import LinkedInAdapter

FIXTURE_HTML = (Path(__file__).parent.parent / "fixtures" / "linkedin.html").read_text()


def test_parse_html_extracts_job_id_from_entity_urn():
    adapter = LinkedInAdapter()
    postings = adapter.parse_html(FIXTURE_HTML)
    assert len(postings) == 1
    job = postings[0]
    assert job.source == "linkedin"
    assert job.source_job_id == "5001"
    assert job.title == "Backend Engineer"
    assert job.company == "Acme Corp"
    assert job.location == "Remote"
    assert job.url == "https://www.linkedin.com/jobs/view/5001"
    assert job.employment_type == "unknown"


def test_parse_html_skips_card_with_missing_entity_urn():
    adapter = LinkedInAdapter()
    postings = adapter.parse_html(FIXTURE_HTML)
    # Fixture contains a second .base-card with no data-entity-urn; it must
    # be excluded rather than emitted with a degenerate shared (empty) id.
    assert len(postings) == 1
    assert all(p.title != "Missing Urn Job" for p in postings)
