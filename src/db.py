import sqlite3
from pathlib import Path

DB_PATH = Path("data/app_database.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS seen_jobs (
    source TEXT NOT NULL,
    source_job_id TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    PRIMARY KEY (source, source_job_id)
);

CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    source_job_id TEXT NOT NULL,
    title TEXT NOT NULL,
    company TEXT NOT NULL,
    description TEXT,
    url TEXT NOT NULL,
    location TEXT,
    employment_type TEXT NOT NULL,
    salary_raw TEXT,
    posted_at TEXT,
    fetched_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'new',
    UNIQUE (source, source_job_id)
);

CREATE TABLE IF NOT EXISTS source_health (
    source TEXT PRIMARY KEY,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    last_success_at TEXT,
    last_failure_at TEXT,
    disabled INTEGER NOT NULL DEFAULT 0,
    disabled_at TEXT
);
"""


def get_connection(db_path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()
