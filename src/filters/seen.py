import sqlite3
from datetime import datetime


def has_seen(conn: sqlite3.Connection, source: str, source_job_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM seen_jobs WHERE source = ? AND source_job_id = ?",
        (source, source_job_id),
    ).fetchone()
    return row is not None


def mark_seen(conn: sqlite3.Connection, source: str, source_job_id: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO seen_jobs (source, source_job_id, first_seen_at) VALUES (?, ?, ?)",
        (source, source_job_id, datetime.utcnow().isoformat()),
    )
    conn.commit()
