from pathlib import Path
from src.ingestion.we_work_remotely import WeWorkRemotelyAdapter

FIXTURE = str(Path(__file__).parent.parent / "fixtures" / "we_work_remotely.xml")


def test_fetch_parses_all_entries():
    adapter = WeWorkRemotelyAdapter(feed_url=FIXTURE)
    postings = adapter.fetch()
    assert len(postings) == 2


def test_fetch_splits_company_and_title():
    adapter = WeWorkRemotelyAdapter(feed_url=FIXTURE)
    postings = adapter.fetch()
    first = postings[0]
    assert first.company == "Acme Corp"
    assert first.title == "Senior Backend Engineer"
    assert first.source == "we_work_remotely"
    assert first.source_job_id == "https://weworkremotely.com/jobs/1001"
    assert first.employment_type == "unknown"
    assert first.location == "Remote"
