from src.ingestion.health import record_success, record_failure, is_disabled, should_poll


def test_should_poll_true_for_never_polled_source(db_conn):
    assert should_poll(db_conn, "wwr", 30) is True


def test_record_success_resets_failure_count(db_conn):
    for _ in range(3):
        record_failure(db_conn, "wwr")
    record_success(db_conn, "wwr")
    row = db_conn.execute(
        "SELECT consecutive_failures FROM source_health WHERE source = ?", ("wwr",)
    ).fetchone()
    assert row["consecutive_failures"] == 0


def test_record_failure_disables_after_five_consecutive():
    import sqlite3
    from src.db import get_connection, init_db

    conn = get_connection(":memory:")
    init_db(conn)
    disabled_flags = [record_failure(conn, "wwr") for _ in range(5)]
    assert disabled_flags == [False, False, False, False, True]
    assert is_disabled(conn, "wwr") is True


def test_should_poll_false_within_interval(db_conn):
    record_success(db_conn, "wwr")
    assert should_poll(db_conn, "wwr", 30) is False


def test_should_poll_true_after_interval_elapsed(db_conn):
    from datetime import datetime, timedelta
    past = (datetime.utcnow() - timedelta(minutes=31)).isoformat()
    db_conn.execute(
        """INSERT INTO source_health (source, consecutive_failures, last_success_at, disabled)
           VALUES ('wwr', 0, ?, 0)""",
        (past,),
    )
    db_conn.commit()
    assert should_poll(db_conn, "wwr", 30) is True
