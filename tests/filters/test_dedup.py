from datetime import datetime, timedelta
from src.ingestion.models import JobPosting
from src.filters.dedup import is_duplicate


def make_job(title, company):
    return JobPosting(
        source="test", source_job_id="1", title=title, company=company,
        description="", url="http://x", location="Remote",
        employment_type="unknown", salary_raw=None, posted_at=None,
    )


def insert_job(conn, title, company, fetched_at):
    conn.execute(
        """INSERT INTO jobs (source, source_job_id, title, company, url,
                              location, employment_type, fetched_at)
           VALUES ('other_source', ?, ?, ?, 'http://y', 'Remote', 'unknown', ?)""",
        (title, title, company, fetched_at),
    )
    conn.commit()


def test_detects_reordered_title_as_duplicate(db_conn):
    insert_job(db_conn, "Senior Backend Engineer", "Acme Corp", datetime.utcnow().isoformat())
    job = make_job("Backend Engineer, Senior", "Acme Corp")
    assert is_duplicate(db_conn, job) is True


def test_does_not_flag_clearly_different_jobs(db_conn):
    insert_job(db_conn, "Senior Backend Engineer", "Acme Corp", datetime.utcnow().isoformat())
    job = make_job("Junior Frontend Developer", "Globex")
    assert is_duplicate(db_conn, job) is False


def test_ignores_jobs_outside_lookback_window(db_conn):
    old = (datetime.utcnow() - timedelta(days=20)).isoformat()
    insert_job(db_conn, "Senior Backend Engineer", "Acme Corp", old)
    job = make_job("Senior Backend Engineer", "Acme Corp")
    assert is_duplicate(db_conn, job) is False
