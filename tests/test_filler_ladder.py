"""
Tests for SyndicatedScheduler.resolve_filler (the accuracy-first filler ladder tried when a slot's
own fill_rules categories have no unused eligible show).

Same approach as test_syndicated_scheduler.py: real dev DB, rolled back per test, replication_year
-999 so nothing collides with real schedule_template rows, test-only category names, and a
temporary overwrite of real shows' eligibility rows. Slots sit inside a real daypart block
(weekday 06:05, early morning) because daypart_column_for() must resolve; channel 1 (a network
channel with no era-aware fill_rules) isolates the rules each test inserts.
"""
from datetime import time

import playlists.classes.SyndicatedScheduler as ss
from playlists.classes.SyndicatedScheduler import SyndicatedScheduler
from tests.test_syndicated_scheduler import _make_show_eligible

RULE_CHANNEL = 1
RERUN_CHANNEL = 3  # real syndicated channel, used as the "this channel" for rerun counting
OTHER_CHANNEL = 5  # a different real syndicated channel
SLOT = time(6, 5)
YEAR = 1972
REPL = -999
CAT_A, CAT_B = "test_cat_a", "test_cat_b"


def _two_show_ids(conn):
    cur = conn.cursor()
    cur.execute("""SELECT e.show_id FROM show_syndication_eligibility e JOIN shows s USING (show_id)
                   WHERE s.show_is_syndicated ORDER BY e.show_id LIMIT 2""")
    return [r[0] for r in cur.fetchall()]


def _rule(conn, category, channel_id=RULE_CHANNEL):
    conn.cursor().execute(
        """INSERT INTO fill_rules (channel_id, start_time, end_time, category, weight, dows, year_start, year_end)
           VALUES (%s, %s, %s, %s, 1.0, %s, 1970, 1974)""",
        (channel_id, SLOT, time(6, 6), category, [0, 1, 2, 3, 4]),
    )


def test_related_category_fills_slot_when_own_category_is_empty(db_conn, monkeypatch):
    s1, _ = _two_show_ids(db_conn)
    _make_show_eligible(db_conn, s1, CAT_B, 1970, 1974, "weekday_early_morning")
    monkeypatch.setitem(ss.FILLER_RELATED_CATEGORIES, CAT_A, [CAT_B])
    _rule(db_conn, CAT_A)

    scheduler = SyndicatedScheduler(db_conn)
    assert scheduler.resolve_syndicated_slot(RULE_CHANNEL, YEAR, 0, SLOT, REPL) == s1


def test_filler_can_be_disabled_to_restore_old_behavior(db_conn, monkeypatch):
    s1, _ = _two_show_ids(db_conn)
    _make_show_eligible(db_conn, s1, CAT_A, 1970, 1974, "weekday_early_morning")
    _rule(db_conn, CAT_A)

    scheduler = SyndicatedScheduler(db_conn)
    assert scheduler.resolve_syndicated_slot(RULE_CHANNEL, YEAR, 0, SLOT, REPL, {s1}, use_filler=False) is None


def test_exhausted_pool_reruns_a_show_already_on_this_channel(db_conn):
    s1, _ = _two_show_ids(db_conn)
    _make_show_eligible(db_conn, s1, CAT_A, 1970, 1974, "weekday_early_morning")
    _rule(db_conn, CAT_A)

    scheduler = SyndicatedScheduler(db_conn)
    # s1 was already used earlier in this run, so tier A finds nothing unused; the filler reruns it.
    assert scheduler.resolve_syndicated_slot(RULE_CHANNEL, YEAR, 0, SLOT, REPL, {s1}) == s1


def test_rerun_avoids_back_to_back_when_another_show_is_available(db_conn):
    s1, s2 = _two_show_ids(db_conn)
    _make_show_eligible(db_conn, s1, CAT_A, 1970, 1974, "weekday_early_morning")
    _make_show_eligible(db_conn, s2, CAT_A, 1970, 1974, "weekday_early_morning")
    _rule(db_conn, CAT_A)

    scheduler = SyndicatedScheduler(db_conn)
    for _ in range(20):
        assert scheduler.resolve_syndicated_slot(RULE_CHANNEL, YEAR, 0, SLOT, REPL, {s1, s2}, prev_show_id=s1) == s2


def test_back_to_back_is_the_last_resort_not_a_failure(db_conn):
    s1, _ = _two_show_ids(db_conn)
    _make_show_eligible(db_conn, s1, CAT_A, 1970, 1974, "weekday_early_morning")
    _rule(db_conn, CAT_A)

    scheduler = SyndicatedScheduler(db_conn)
    assert scheduler.resolve_syndicated_slot(RULE_CHANNEL, YEAR, 0, SLOT, REPL, {s1}, prev_show_id=s1) == s1


def test_same_channel_rerun_is_preferred_over_another_channels_show(db_conn):
    here, there = _two_show_ids(db_conn)
    _make_show_eligible(db_conn, here, CAT_A, 1970, 1974, "weekday_early_morning")
    _make_show_eligible(db_conn, there, CAT_A, 1970, 1974, "weekday_early_morning")
    _rule(db_conn, CAT_A)
    # `there` is already licensed to a different channel this replication year, `here` was used
    # earlier in this run on the channel being scheduled.
    # `here` is also placed twice on the other channel, so it has MORE placements overall than
    # `there` -- a global least-used pick (tier D2) would choose `there`; only the same-channel
    # preference (D1) explains choosing `here`.
    cur = db_conn.cursor()
    for channel_id, slot, show_id in (
        (OTHER_CHANNEL, time(6, 0), there),
        (OTHER_CHANNEL, time(6, 30), here),
        (OTHER_CHANNEL, time(7, 0), here),
    ):
        cur.execute(
            """INSERT INTO schedule_template (channel_id, time_slot, show_id, runtime, replication_year, days_of_week)
               VALUES (%s, %s, %s, interval '30 minutes', %s, %s)""",
            (channel_id, slot, show_id, REPL, [0, 1, 2, 3, 4]),
        )

    scheduler = SyndicatedScheduler(db_conn)
    for _ in range(20):
        assert scheduler.resolve_syndicated_slot(RULE_CHANNEL, YEAR, 0, SLOT, REPL, {here}) == here


def test_filler_returns_none_when_no_in_era_show_matches_any_category(db_conn):
    _rule(db_conn, "category_nobody_has")

    scheduler = SyndicatedScheduler(db_conn)
    assert scheduler.resolve_syndicated_slot(RULE_CHANNEL, YEAR, 0, SLOT, REPL) is None
