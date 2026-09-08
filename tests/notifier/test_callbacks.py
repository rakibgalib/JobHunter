import pytest
from src.notifier.callbacks import apply_callback


def insert_job(conn):
    conn.execute(
        """INSERT INTO jobs (source, source_job_id, title, company, url,
                              location, employment_type, fetched_at, status)
           VALUES ('wwr', '1', 't', 'c', 'u', 'l', 'unknown', '2026-01-01', 'alerted')"""
    )
    conn.commit()
    return conn.execute("SELECT id FROM jobs").fetchone()["id"]


def test_snooze_sets_status_and_updates_fetched_at(db_conn):
    job_id = insert_job(db_conn)
    action = apply_callback(db_conn, f"snooze:{job_id}")
    assert action == "snooze"
    row = db_conn.execute("SELECT status, fetched_at FROM jobs WHERE id = ?", (job_id,)).fetchone()
    assert row["status"] == "snoozed"
    assert row["fetched_at"] != "2026-01-01"


def test_skip_sets_status(db_conn):
    job_id = insert_job(db_conn)
    action = apply_callback(db_conn, f"skip:{job_id}")
    assert action == "skip"
    row = db_conn.execute("SELECT status FROM jobs WHERE id = ?", (job_id,)).fetchone()
    assert row["status"] == "skipped"


def test_unknown_action_raises(db_conn):
    job_id = insert_job(db_conn)
    with pytest.raises(ValueError):
        apply_callback(db_conn, f"bogus:{job_id}")
