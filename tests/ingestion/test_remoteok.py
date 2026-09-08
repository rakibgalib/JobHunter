import json
from pathlib import Path
import responses
from src.ingestion.remoteok import RemoteOKAdapter

FIXTURE = json.loads((Path(__file__).parent.parent / "fixtures" / "remoteok.json").read_text())
API_URL = "https://remoteok.com/api"


@responses.activate
def test_fetch_skips_legal_notice_and_parses_jobs():
    responses.add(responses.GET, API_URL, json=FIXTURE, status=200)
    adapter = RemoteOKAdapter(api_url=API_URL)
    postings = adapter.fetch()
    assert len(postings) == 1
    job = postings[0]
    assert job.source == "remoteok"
    assert job.source_job_id == "2001"
    assert job.title == "Backend Engineer"
    assert job.company == "Acme Corp"
    assert job.employment_type == "unknown"
