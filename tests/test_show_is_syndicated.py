"""
The syndicated scheduler only picks shows with shows.show_is_syndicated = TRUE: a show that was never syndicated
(PBS-only, cable-only, network-only) can't air on a syndicated channel, whatever its eligibility row says.
Real dev DB, rolled back per test; test-only category names so nothing real is matched.
"""
from datetime import time

from playlists.classes.SyndicatedScheduler import SyndicatedScheduler
from tests.test_syndicated_scheduler import _make_show_eligible

CHANNEL = 3
SLOT = time(6, 5)
CAT = "test_syndication_only"


def _show(conn, name):
    cur = conn.cursor()
    cur.execute("SELECT show_id FROM shows WHERE show_name = %s", (name,))
    return cur.fetchone()[0]


def _set_syndicated(conn, show_id, value):
    conn.cursor().execute("UPDATE shows SET show_is_syndicated = %s WHERE show_id = %s", (value, show_id))


def _rule(conn):
    conn.cursor().execute(
        """INSERT INTO fill_rules (channel_id, start_time, end_time, category, weight, dows, year_start, year_end)
           VALUES (1, %s, %s, %s, 1.0, %s, 1970, 1974)""",
        (SLOT, time(6, 6), CAT, [0, 1, 2, 3, 4]),
    )


def test_a_non_syndicated_show_is_never_offered_a_slot(db_conn):
    show = _show(db_conn, "Shazam")           # show_is_syndicated = FALSE in the DB
    _make_show_eligible(db_conn, show, CAT, 1970, 1974, "weekday_early_morning")
    _set_syndicated(db_conn, show, False)
    scheduler = SyndicatedScheduler(db_conn)

    assert scheduler.get_eligible_shows(CAT, 1972, "weekday_early_morning", CHANNEL, -999) == []
    assert scheduler.get_era_eligible_shows(CAT, 1972, "weekday_early_morning") == []

    _set_syndicated(db_conn, show, True)
    assert scheduler.get_eligible_shows(CAT, 1972, "weekday_early_morning", CHANNEL, -999) == [show]
    assert scheduler.get_era_eligible_shows(CAT, 1972, "weekday_early_morning") == [show]


def test_the_filler_will_not_rerun_a_non_syndicated_show_either(db_conn):
    show = _show(db_conn, "Shazam")
    _make_show_eligible(db_conn, show, CAT, 1970, 1974, "weekday_early_morning")
    _rule(db_conn)
    scheduler = SyndicatedScheduler(db_conn)

    _set_syndicated(db_conn, show, False)
    # even as an already-used show (a rerun candidate) it must not come back
    assert scheduler.resolve_syndicated_slot(1, 1972, 0, SLOT, -999, {show}) is None
    assert scheduler.resolve_syndicated_slot(1, 1972, 0, SLOT, -999) is None

    _set_syndicated(db_conn, show, True)
    assert scheduler.resolve_syndicated_slot(1, 1972, 0, SLOT, -999) == show
    assert scheduler.resolve_syndicated_slot(1, 1972, 0, SLOT, -999, {show}) == show
