import json
from pathlib import Path
import responses
from src.ingestion.remotive import RemotiveAdapter

FIXTURE = json.loads((Path(__file__).parent.parent / "fixtures" / "remotive.json").read_text())
API_URL = "https://remotive.com/api/remote-jobs"


@responses.activate
def test_fetch_maps_job_type_to_employment_type():
    responses.add(responses.GET, API_URL, json=FIXTURE, status=200)
    adapter = RemotiveAdapter(api_url=API_URL)
    postings = adapter.fetch()
    assert len(postings) == 2
    assert postings[0].employment_type == "full_time"
    assert postings[1].employment_type == "part_time"
    assert postings[0].company == "Acme Corp"
    assert postings[0].source_job_id == "3001"


@responses.activate
def test_fetch_defaults_unknown_job_type_to_unknown():
    fixture = {"jobs": [dict(FIXTURE["jobs"][0], job_type="internship")]}
    responses.add(responses.GET, API_URL, json=fixture, status=200)
    adapter = RemotiveAdapter(api_url=API_URL)
    postings = adapter.fetch()
    assert postings[0].employment_type == "unknown"


@responses.activate
def test_fetch_freelance_maps_to_contract():
    fixture = {"jobs": [dict(FIXTURE["jobs"][0], job_type="freelance")]}
    responses.add(responses.GET, API_URL, json=fixture, status=200)
    adapter = RemotiveAdapter(api_url=API_URL)
    postings = adapter.fetch()
    assert postings[0].employment_type == "contract"


@responses.activate
def test_fetch_contract_maps_to_contract():
    fixture = {"jobs": [dict(FIXTURE["jobs"][0], job_type="contract")]}
    responses.add(responses.GET, API_URL, json=fixture, status=200)
    adapter = RemotiveAdapter(api_url=API_URL)
    postings = adapter.fetch()
    assert postings[0].employment_type == "contract"
