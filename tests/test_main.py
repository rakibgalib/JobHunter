from main import run_cycle


class FakeAdapter:
    def __init__(self, postings=None, error=None):
        self._postings = postings or []
        self._error = error

    def fetch(self):
        if self._error:
            raise self._error
        return self._postings


class FakeNotifier:
    def __init__(self):
        self.alerts = []
        self.health_alerts = []

    def send_job_alert(self, job_id, title, company, location, employment_type, source, url):
        self.alerts.append(title)

    def send_health_alert(self, source, last_error):
        self.health_alerts.append(source)

    def get_callback_updates(self, offset=None):
        return []


SOURCES_CONFIG = {
    "fake_source": {"enabled": True, "poll_interval_minutes": 30, "delay_range_seconds": [0, 0]}
}
BLACKLIST = {"companies": [], "keywords": []}
PROFILE = {"tech_stack_keywords": [], "employment_preferences": {"accepting_full_time": True, "accepting_part_time": True}}


def make_job(source_job_id):
    from src.ingestion.models import JobPosting
    return JobPosting(
        source="fake_source", source_job_id=source_job_id, title="Engineer", company="Acme",
        description="", url="http://x", location="Remote",
        employment_type="unknown", salary_raw=None, posted_at=None,
    )


def test_run_cycle_processes_enabled_source_and_alerts(db_conn):
    notifier = FakeNotifier()
    adapter_classes = {"fake_source": lambda: FakeAdapter(postings=[make_job("1")])}
    results = run_cycle(db_conn, SOURCES_CONFIG, BLACKLIST, PROFILE, notifier, adapter_classes)
    assert results["fake_source"] == ["alerted"]
    assert notifier.alerts == ["Engineer"]


def test_run_cycle_skips_disabled_source(db_conn):
    from src.ingestion.health import record_failure
    for _ in range(5):
        record_failure(db_conn, "fake_source")
    notifier = FakeNotifier()
    adapter_classes = {"fake_source": lambda: FakeAdapter(postings=[make_job("1")])}
    results = run_cycle(db_conn, SOURCES_CONFIG, BLACKLIST, PROFILE, notifier, adapter_classes)
    assert "fake_source" not in results


SOURCES_CONFIG_NO_COOLDOWN = {
    "fake_source": {"enabled": True, "poll_interval_minutes": 0, "delay_range_seconds": [0, 0]}
}


def test_run_cycle_records_failure_and_sends_health_alert_at_threshold(db_conn):
    # poll_interval_minutes=0 so should_poll never blocks the next attempt in this loop
    notifier = FakeNotifier()
    adapter_classes = {"fake_source": lambda: FakeAdapter(error=RuntimeError("boom"))}
    for _ in range(4):
        run_cycle(db_conn, SOURCES_CONFIG_NO_COOLDOWN, BLACKLIST, PROFILE, notifier, adapter_classes)
    assert notifier.health_alerts == []
    run_cycle(db_conn, SOURCES_CONFIG_NO_COOLDOWN, BLACKLIST, PROFILE, notifier, adapter_classes)
    assert notifier.health_alerts == ["fake_source"]
