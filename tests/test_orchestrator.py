from datetime import datetime, timedelta
from src.ingestion.models import JobPosting
from src.orchestrator import process_job, resurface_snoozed

BLACKLIST = {"companies": ["Bad Co"], "keywords": ["unpaid"]}
PROFILE = {
    "tech_stack_keywords": ["python"],
    "employment_preferences": {"accepting_full_time": True, "accepting_part_time": False},
}


class FakeNotifier:
    def __init__(self):
        self.alerts = []

    def send_job_alert(self, job_id, title, company, location, employment_type, source, url):
        self.alerts.append((job_id, title, company))

    def send_health_alert(self, source, last_error):
        pass


def make_job(**overrides):
    defaults = dict(
        source="wwr", source_job_id="1", title="Python Engineer", company="Acme",
        description="", url="http://x", location="Remote",
        employment_type="full_time", salary_raw=None, posted_at=None,
    )
    defaults.update(overrides)
    return JobPosting(**defaults)


def test_alerts_and_inserts_a_clean_job(db_conn):
    notifier = FakeNotifier()
    outcome = process_job(db_conn, make_job(), BLACKLIST, PROFILE, notifier)
    assert outcome == "alerted"
    assert len(notifier.alerts) == 1
    row = db_conn.execute("SELECT status FROM jobs WHERE source_job_id = '1'").fetchone()
    assert row["status"] == "alerted"


def test_drops_already_seen_job_without_reprocessing(db_conn):
    notifier = FakeNotifier()
    process_job(db_conn, make_job(), BLACKLIST, PROFILE, notifier)
    outcome = process_job(db_conn, make_job(), BLACKLIST, PROFILE, notifier)
    assert outcome == "duplicate_seen"
    assert len(notifier.alerts) == 1


def test_drops_blacklisted_job(db_conn):
    notifier = FakeNotifier()
    outcome = process_job(db_conn, make_job(company="Bad Co", source_job_id="2"), BLACKLIST, PROFILE, notifier)
    assert outcome == "blacklisted"
    assert notifier.alerts == []


def test_drops_job_failing_employment_filter(db_conn):
    notifier = FakeNotifier()
    outcome = process_job(
        db_conn, make_job(employment_type="part_time", source_job_id="3"), BLACKLIST, PROFILE, notifier
    )
    assert outcome == "employment_filtered"
    assert notifier.alerts == []


def test_drops_job_failing_keyword_filter(db_conn):
    notifier = FakeNotifier()
    outcome = process_job(
        db_conn, make_job(title="Sales Rep", source_job_id="4"), BLACKLIST, PROFILE, notifier
    )
    assert outcome == "keyword_filtered"
    assert notifier.alerts == []


def test_drops_fuzzy_duplicate_from_other_source(db_conn):
    notifier = FakeNotifier()
    process_job(db_conn, make_job(source_job_id="5"), BLACKLIST, PROFILE, notifier)
    duplicate = make_job(source="remoteok", source_job_id="6", title="Engineer, Python")
    outcome = process_job(db_conn, duplicate, BLACKLIST, PROFILE, notifier)
    assert outcome == "fuzzy_duplicate"
    assert len(notifier.alerts) == 1


def test_resurface_snoozed_realerts_expired_jobs(db_conn):
    old = (datetime.utcnow() - timedelta(hours=25)).isoformat()
    db_conn.execute(
        """INSERT INTO jobs (source, source_job_id, title, company, url,
                              location, employment_type, fetched_at, status)
           VALUES ('wwr', '9', 'Python Engineer', 'Acme', 'http://x', 'Remote',
                   'unknown', ?, 'snoozed')""",
        (old,),
    )
    db_conn.commit()
    notifier = FakeNotifier()
    count = resurface_snoozed(db_conn, notifier)
    assert count == 1
    assert len(notifier.alerts) == 1
    row = db_conn.execute("SELECT status FROM jobs WHERE source_job_id = '9'").fetchone()
    assert row["status"] == "alerted"


def test_resurface_snoozed_ignores_jobs_within_window(db_conn):
    recent = (datetime.utcnow() - timedelta(hours=1)).isoformat()
    db_conn.execute(
        """INSERT INTO jobs (source, source_job_id, title, company, url,
                              location, employment_type, fetched_at, status)
           VALUES ('wwr', '10', 'Python Engineer', 'Acme', 'http://x', 'Remote',
                   'unknown', ?, 'snoozed')""",
        (recent,),
    )
    db_conn.commit()
    notifier = FakeNotifier()
    count = resurface_snoozed(db_conn, notifier)
    assert count == 0
