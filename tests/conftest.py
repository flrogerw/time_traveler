import os

import psycopg
import pytest
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

# Sentinel replication_year used by every test that writes to schedule_template/broadcast_log.
# Never a real value, so it can't collide with real schedule data even if a test's rollback
# somehow failed to happen.
TEST_REPLICATION_YEAR = -1


@pytest.fixture
def db_conn():
    """
    A live connection to the real dev DB, wrapped so nothing a test does is ever actually
    persisted -- even though the code under test (Episodes.get_next_episode,
    Shows.get_replacement_show, ...) calls conn.commit() internally as part of normal operation.

    conn.commit() is patched to a no-op for the duration of the test (the transaction stays
    open underneath it), then the real commit is restored and a real rollback discards
    everything. This is the only way to get transaction-per-test isolation against code that
    commits mid-flow -- a plain "rollback at teardown" fixture would not work here, since the
    code under test's own commit() calls would have already persisted the writes.
    """
    conn = psycopg.connect(
        host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        dbname=os.environ["DB_NAME"],
    )
    real_commit = conn.commit
    conn.commit = lambda: None
    try:
        yield conn
    finally:
        conn.commit = real_commit
        conn.rollback()
        conn.close()
