import sqlite3
from src.db import get_connection, init_db

def test_init_db_creates_expected_tables():
    conn = get_connection(":memory:")
    init_db(conn)
    tables = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    assert {"seen_jobs", "jobs", "source_health"} <= tables

def test_jobs_table_enforces_unique_source_job_id():
    conn = get_connection(":memory:")
    init_db(conn)
    conn.execute(
        """INSERT INTO jobs (source, source_job_id, title, company, url,
                              location, employment_type, fetched_at)
           VALUES ('wwr', '1', 't', 'c', 'u', 'l', 'unknown', '2026-01-01')"""
    )
    conn.commit()
    import pytest
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            """INSERT INTO jobs (source, source_job_id, title, company, url,
                                  location, employment_type, fetched_at)
               VALUES ('wwr', '1', 't2', 'c2', 'u2', 'l2', 'unknown', '2026-01-02')"""
        )
