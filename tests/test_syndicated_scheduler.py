"""
Tests for SyndicatedScheduler (era-aware syndicated daytime scheduling -- see
CLAUDE_SYNDICATED_SCHEDULING_HANDOFF.md on the Desktop for the design).

Run against the real dev DB (see conftest.db_conn). fill_rules/syndicated_fixed_slots.channel_id
has an FK to channels, so tests use a real syndicated channel (3 = TV-SYN-5) but at an off-grid
time (03:17-03:19, never used by any real daypart block or fixed slot) with a test-only category
name ('test_only_category') so they can't collide with real production rows; a temporary
overwrite (restored by the transaction rollback) of one real show's show_syndication_eligibility
row covers eligibility tests.
"""
from datetime import time

from playlists.classes.SyndicatedScheduler import SyndicatedScheduler

TEST_CHANNEL_ID = 3  # TV-SYN-5 -- a real syndicated channel (FK-constrained column).
TEST_START = time(3, 17)  # Off-grid time, outside every real daypart block/fixed slot.
TEST_END = time(3, 19)
TEST_CATEGORY = "test_only_category"


def _insert_fill_rule(conn, start_time, end_time, category, weight, dows, year_start=None, year_end=None):
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO fill_rules (channel_id, start_time, end_time, category, weight, dows, year_start, year_end)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
        (TEST_CHANNEL_ID, start_time, end_time, category, weight, dows, year_start, year_end),
    )


def _insert_fixed_slot(conn, dow, start_time, end_time, show_id, year_start=None, year_end=None):
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO syndicated_fixed_slots (channel_id, dow, start_time, end_time, show_id, year_start, year_end)
           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        (TEST_CHANNEL_ID, dow, start_time, end_time, show_id, year_start, year_end),
    )


ALL_DAYPART_COLUMNS = [
    "weekday_early_morning", "weekday_morning", "weekday_late_morning", "weekday_noon",
    "weekday_early_afternoon", "weekday_after_school", "weekday_early_evening",
    "saturday_early_morning", "saturday_morning", "saturday_midday", "saturday_afternoon", "saturday_early_evening",
    "sunday_early_morning", "sunday_morning", "sunday_midday", "sunday_afternoon", "sunday_early_evening",
]


def _make_show_eligible(conn, show_id, mapped_genre, eligible_year_start, eligible_year_end, daypart_column):
    """Temporarily overwrite one real show's eligibility row so it's the sole match for
    `mapped_genre`/`daypart_column` -- restored by the test transaction's rollback."""
    cur = conn.cursor()
    other_columns_false = ", ".join(f"{c} = FALSE" for c in ALL_DAYPART_COLUMNS if c != daypart_column)
    cur.execute(
        f"""UPDATE show_syndication_eligibility
            SET mapped_genre = %s, eligible_year_start = %s, eligible_year_end = %s,
                {other_columns_false}, {daypart_column} = TRUE
            WHERE show_id = %s""",
        (mapped_genre, eligible_year_start, eligible_year_end, show_id),
    )
    assert cur.rowcount == 1, f"show_id {show_id} has no show_syndication_eligibility row to overwrite"


def _any_real_show_id(conn):
    cur = conn.cursor()
    cur.execute("SELECT show_id FROM show_syndication_eligibility LIMIT 1")
    return cur.fetchone()[0]


def test_era_filtering_matches_within_range_not_outside(db_conn):
    _insert_fill_rule(db_conn, TEST_START, TEST_END, TEST_CATEGORY, 1.0, [0, 1, 2, 3, 4], 1970, 1974)
    scheduler = SyndicatedScheduler(db_conn)

    rules_in_range = scheduler.get_fill_rules(TEST_CHANNEL_ID, 1972, 0, TEST_START)
    assert any(r["category"] == TEST_CATEGORY for r in rules_in_range)

    rules_out_of_range = scheduler.get_fill_rules(TEST_CHANNEL_ID, 1985, 0, TEST_START)
    assert not any(r["category"] == TEST_CATEGORY for r in rules_out_of_range)


def test_generic_rule_matches_when_no_year_specific_rule_exists(db_conn):
    _insert_fill_rule(db_conn, TEST_START, TEST_END, TEST_CATEGORY, 1.0, [0, 1, 2, 3, 4], None, None)
    scheduler = SyndicatedScheduler(db_conn)

    rules = scheduler.get_fill_rules(TEST_CHANNEL_ID, 1972, 0, TEST_START)
    assert any(r["category"] == TEST_CATEGORY for r in rules)


def test_year_specific_rule_takes_precedence_over_generic(db_conn):
    _insert_fill_rule(db_conn, TEST_START, TEST_END, "generic_version", 1.0, [0, 1, 2, 3, 4], None, None)
    _insert_fill_rule(db_conn, TEST_START, TEST_END, "specific_version", 1.0, [0, 1, 2, 3, 4], 1970, 1974)
    scheduler = SyndicatedScheduler(db_conn)

    rules = scheduler.get_fill_rules(TEST_CHANNEL_ID, 1972, 0, TEST_START)
    categories = {r["category"] for r in rules}
    assert categories == {"specific_version"}, "generic and year-specific rules must not both apply"


def test_day_of_week_filtering(db_conn):
    _insert_fill_rule(db_conn, TEST_START, TEST_END, TEST_CATEGORY, 1.0, [0, 1, 2, 3, 4], None, None)
    scheduler = SyndicatedScheduler(db_conn)

    weekday_rules = scheduler.get_fill_rules(TEST_CHANNEL_ID, 1972, 0, TEST_START)  # Monday
    assert any(r["category"] == TEST_CATEGORY for r in weekday_rules)

    weekend_rules = scheduler.get_fill_rules(TEST_CHANNEL_ID, 1972, 5, TEST_START)  # Saturday
    assert not any(r["category"] == TEST_CATEGORY for r in weekend_rules)


def test_fixed_slot_takes_precedence_over_fill_rules(db_conn):
    show_id = _any_real_show_id(db_conn)
    _insert_fixed_slot(db_conn, 0, TEST_START, TEST_END, show_id)
    _insert_fill_rule(db_conn, TEST_START, TEST_END, TEST_CATEGORY, 1.0, [0, 1, 2, 3, 4], None, None)
    scheduler = SyndicatedScheduler(db_conn)

    resolved = scheduler.resolve_syndicated_slot(TEST_CHANNEL_ID, 1972, 0, TEST_START, 1972)
    assert resolved == show_id


def test_weighted_category_selection_uses_configured_weights(db_conn, monkeypatch):
    scheduler = SyndicatedScheduler(db_conn)
    rules = [
        {"category": "heavy", "weight": 0.9},
        {"category": "light", "weight": 0.1},
    ]

    captured = {}
    import playlists.classes.SyndicatedScheduler as module

    def fake_choices(population, weights, k):
        captured["population"] = population
        captured["weights"] = weights
        return [population[0]]

    monkeypatch.setattr(module.random, "choices", fake_choices)
    scheduler.choose_category(rules)

    assert captured["population"] == ["heavy", "light"]
    assert captured["weights"] == [0.9, 0.1]


def test_show_not_eligible_before_or_after_its_eligible_year_range(db_conn):
    show_id = _any_real_show_id(db_conn)
    _make_show_eligible(db_conn, show_id, TEST_CATEGORY, 1970, 1974, "weekday_early_morning")
    scheduler = SyndicatedScheduler(db_conn)

    too_early = scheduler.get_eligible_shows(TEST_CATEGORY, 1969, "weekday_early_morning", TEST_CHANNEL_ID, -999)
    assert show_id not in too_early

    too_late = scheduler.get_eligible_shows(TEST_CATEGORY, 1975, "weekday_early_morning", TEST_CHANNEL_ID, -999)
    assert show_id not in too_late

    within_range = scheduler.get_eligible_shows(TEST_CATEGORY, 1972, "weekday_early_morning", TEST_CHANNEL_ID, -999)
    assert show_id in within_range


def test_empty_category_is_dropped_and_retried_instead_of_failing_the_slot(db_conn):
    """Mirrors the real 'religious' gap: a fill_rules category with zero eligible shows must not
    prevent the slot from resolving via a different category, and must never raise.

    Uses a network channel_id (1 = TV-CBS-2) for the fill_rules rows -- it has no real
    era-specific rows imported (only the 4 syndicated channels do), so get_fill_rules() returns
    exactly the two rows this test inserts, isolating it from real 'kids'/'family'/etc rows that
    would otherwise also have real eligible shows and defeat the assertion below. The slot time
    must still fall inside a real daypart block (06:00-19:00) for daypart_column_for() to
    resolve, which is why this can't reuse the off-grid TEST_START used elsewhere in this file.
    """
    in_daypart_channel_id = 1  # TV-CBS-2 -- no era-aware fill_rules rows exist for it.
    in_daypart_start, in_daypart_end = time(6, 5), time(6, 6)

    show_id = _any_real_show_id(db_conn)
    _make_show_eligible(db_conn, show_id, "has_a_show", 1970, 1974, "weekday_early_morning")
    cur = db_conn.cursor()
    cur.execute(
        """INSERT INTO fill_rules (channel_id, start_time, end_time, category, weight, dows, year_start, year_end)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
        (in_daypart_channel_id, in_daypart_start, in_daypart_end, "no_shows_for_this_one", 0.99, [0, 1, 2, 3, 4], 1970, 1974),
    )
    cur.execute(
        """INSERT INTO fill_rules (channel_id, start_time, end_time, category, weight, dows, year_start, year_end)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
        (in_daypart_channel_id, in_daypart_start, in_daypart_end, "has_a_show", 0.01, [0, 1, 2, 3, 4], 1970, 1974),
    )
    scheduler = SyndicatedScheduler(db_conn)

    for _ in range(20):
        resolved = scheduler.resolve_syndicated_slot(in_daypart_channel_id, 1972, 0, in_daypart_start, -999)
        assert resolved == show_id


def test_network_scheduling_path_is_unaffected(db_conn):
    """create_playlist.py's network branch must never touch SyndicatedScheduler -- a static
    check that the class isn't referenced outside the syndicated branch/module."""
    import inspect

    import playlists.create_playlist as create_playlist_module

    source = inspect.getsource(create_playlist_module)
    syndicated_branch, network_branch = source.split("else:", 1)
    assert "SyndicatedScheduler" in syndicated_branch
    assert "SyndicatedScheduler(" not in network_branch
