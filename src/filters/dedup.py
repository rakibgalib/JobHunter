import sqlite3
from datetime import datetime, timedelta
from rapidfuzz import fuzz
from src.ingestion.models import JobPosting

SIMILARITY_THRESHOLD = 90
LOOKBACK_DAYS = 14


def is_duplicate(conn: sqlite3.Connection, job: JobPosting) -> bool:
    cutoff = (datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)).isoformat()
    rows = conn.execute(
        "SELECT title, company FROM jobs WHERE fetched_at >= ?", (cutoff,)
    ).fetchall()
    candidate = f"{job.title} {job.company}".lower()
    for row in rows:
        existing = f"{row['title']} {row['company']}".lower()
        if fuzz.token_sort_ratio(candidate, existing) >= SIMILARITY_THRESHOLD:
            return True
    return False
