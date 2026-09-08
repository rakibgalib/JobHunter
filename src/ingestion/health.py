import sqlite3
from datetime import datetime, timedelta

FAILURE_THRESHOLD = 5


def record_success(conn: sqlite3.Connection, source: str) -> None:
    now = datetime.utcnow().isoformat()
    conn.execute(
        """INSERT INTO source_health (source, consecutive_failures, last_success_at, disabled)
           VALUES (?, 0, ?, 0)
           ON CONFLICT(source) DO UPDATE SET
             consecutive_failures = 0,
             last_success_at = excluded.last_success_at""",
        (source, now),
    )
    conn.commit()


def record_failure(conn: sqlite3.Connection, source: str) -> bool:
    now = datetime.utcnow().isoformat()
    row = conn.execute(
        "SELECT consecutive_failures, disabled FROM source_health WHERE source = ?", (source,)
    ).fetchone()
    failures = (row["consecutive_failures"] if row else 0) + 1
    was_disabled = bool(row["disabled"]) if row else False
    disabled = 1 if failures >= FAILURE_THRESHOLD else 0
    conn.execute(
        """INSERT INTO source_health (source, consecutive_failures, last_failure_at, disabled, disabled_at)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(source) DO UPDATE SET
             consecutive_failures = excluded.consecutive_failures,
             last_failure_at = excluded.last_failure_at,
             disabled = excluded.disabled,
             disabled_at = COALESCE(source_health.disabled_at, excluded.disabled_at)""",
        (source, failures, now, disabled, now if disabled else None),
    )
    conn.commit()
    return bool(disabled) and not was_disabled


def is_disabled(conn: sqlite3.Connection, source: str) -> bool:
    row = conn.execute(
        "SELECT disabled FROM source_health WHERE source = ?", (source,)
    ).fetchone()
    return bool(row["disabled"]) if row else False


def should_poll(conn: sqlite3.Connection, source: str, poll_interval_minutes: int) -> bool:
    row = conn.execute(
        "SELECT last_success_at, last_failure_at FROM source_health WHERE source = ?",
        (source,),
    ).fetchone()
    if row is None:
        return True
    timestamps = [t for t in (row["last_success_at"], row["last_failure_at"]) if t]
    if not timestamps:
        return True
    last = max(timestamps)
    elapsed = datetime.utcnow() - datetime.fromisoformat(last)
    return elapsed >= timedelta(minutes=poll_interval_minutes)
