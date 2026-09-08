import sqlite3
from datetime import datetime, timezone


def apply_callback(conn: sqlite3.Connection, callback_data: str) -> str:
    action, _, job_id_str = callback_data.partition(":")
    job_id = int(job_id_str)
    if action == "snooze":
        conn.execute(
            "UPDATE jobs SET status = 'snoozed', fetched_at = ? WHERE id = ?",
            (datetime.now(timezone.utc).isoformat(), job_id),
        )
    elif action == "skip":
        conn.execute("UPDATE jobs SET status = 'skipped' WHERE id = ?", (job_id,))
    else:
        raise ValueError(f"Unknown callback action: {action}")
    conn.commit()
    return action
