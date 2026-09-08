from src.filters.seen import has_seen, mark_seen


def test_has_seen_false_before_marking(db_conn):
    assert has_seen(db_conn, "wwr", "job-1") is False


def test_has_seen_true_after_marking(db_conn):
    mark_seen(db_conn, "wwr", "job-1")
    assert has_seen(db_conn, "wwr", "job-1") is True


def test_mark_seen_is_idempotent(db_conn):
    mark_seen(db_conn, "wwr", "job-1")
    mark_seen(db_conn, "wwr", "job-1")  # must not raise
    assert has_seen(db_conn, "wwr", "job-1") is True
