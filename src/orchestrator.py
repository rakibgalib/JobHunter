import sqlite3
from datetime import datetime, timedelta, timezone

from src.filters.seen import has_seen, mark_seen
from src.filters.blacklist import is_blacklisted
from src.filters.employment_type import passes_employment_filter
from src.filters.keywords import matches_tech_stack
from src.filters.dedup import is_duplicate
from src.ingestion.models import JobPosting

SNOOZE_HOURS = 24


def process_job(conn: sqlite3.Connection, job: JobPosting, blacklist: dict,
                 profile: dict, notifier) -> str:
    if has_seen(conn, job.source, job.source_job_id):
        return "duplicate_seen"
    mark_seen(conn, job.source, job.source_job_id)

    if is_blacklisted(job, blacklist):
        return "blacklisted"

    if not passes_employment_filter(job, profile.get("employment_preferences", {})):
        return "employment_filtered"

    if not matches_tech_stack(job, profile.get("tech_stack_keywords", [])):
        return "keyword_filtered"

    if is_duplicate(conn, job):
        return "fuzzy_duplicate"

    now = datetime.now(timezone.utc).isoformat()
    cursor = conn.execute(
        """INSERT INTO jobs (source, source_job_id, title, company, description, url,
                              location, employment_type, salary_raw, posted_at, fetched_at, status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'new')""",
        (
            job.source, job.source_job_id, job.title, job.company, job.description, job.url,
            job.location, job.employment_type, job.salary_raw,
            job.posted_at.isoformat() if job.posted_at else None, now,
        ),
    )
    conn.commit()
    job_id = cursor.lastrowid

    notifier.send_job_alert(job_id, job.title, job.company, job.location,
                             job.employment_type, job.source, job.url)
    conn.execute("UPDATE jobs SET status = 'alerted' WHERE id = ?", (job_id,))
    conn.commit()
    return "alerted"


def retry_unalerted(conn: sqlite3.Connection, notifier) -> int:
    rows = conn.execute("SELECT * FROM jobs WHERE status = 'new'").fetchall()
    for row in rows:
        notifier.send_job_alert(row["id"], row["title"], row["company"], row["location"],
                                 row["employment_type"], row["source"], row["url"])
        conn.execute("UPDATE jobs SET status = 'alerted' WHERE id = ?", (row["id"],))
    conn.commit()
    return len(rows)


def resurface_snoozed(conn: sqlite3.Connection, notifier) -> int:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=SNOOZE_HOURS)).isoformat()
    rows = conn.execute(
        "SELECT * FROM jobs WHERE status = 'snoozed' AND fetched_at <= ?", (cutoff,)
    ).fetchall()
    for row in rows:
        notifier.send_job_alert(row["id"], row["title"], row["company"], row["location"],
                                 row["employment_type"], row["source"], row["url"])
        conn.execute(
            "UPDATE jobs SET status = 'alerted', fetched_at = ? WHERE id = ?",
            (datetime.now(timezone.utc).isoformat(), row["id"]),
        )
    conn.commit()
    return len(rows)
