from pathlib import Path
from src.ingestion.nodesk import NoDeskAdapter

FIXTURE_HTML = (Path(__file__).parent.parent / "fixtures" / "nodesk.html").read_text()


def test_parse_html_extracts_jobs_and_normalizes_relative_url():
    adapter = NoDeskAdapter()
    postings = adapter.parse_html(FIXTURE_HTML)
    assert len(postings) == 2
    first = postings[0]
    assert first.title == "Backend Engineer"
    assert first.company == "Acme Corp"
    assert first.source == "nodesk"
    assert first.source_job_id == "acme-backend-engineer"
    assert first.url == "https://nodesk.co/remote-jobs/acme-backend-engineer"
    assert first.employment_type == "unknown"


def test_parse_html_keeps_absolute_url_unchanged():
    adapter = NoDeskAdapter()
    postings = adapter.parse_html(FIXTURE_HTML)
    assert postings[1].url == "https://nodesk.co/remote-jobs/globex-designer"
