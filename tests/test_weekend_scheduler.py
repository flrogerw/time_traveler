"""
Tests for the Saturday/Sunday additive patch on top of the era-aware syndicated scheduler --
see CLAUDE_WEEKEND_SCHEDULING_HANDOFF.md and CLAUDE_WEEKEND_GENRE_FIX.md (Desktop).

Reuses test_syndicated_scheduler.py's isolation approach (real syndicated channel_id=3, off-grid
TEST_START/TEST_END, TEST_CATEGORY, _make_show_eligible) for the weekend-specific fill_rules/
fixed_slot/eligibility checks. The network-weekend-seed tests assert on real ingested historical
data (ABC/CBS 1964-1969 Saturday schedule seeds) the same way test_episode_rotation.py does.
"""
from datetime import date, time

from playlists.classes.Shows import Shows
from playlists.classes.SyndicatedScheduler import SyndicatedScheduler, daypart_column_for
from tests.test_syndicated_scheduler import (
    TEST_CATEGORY,
    TEST_CHANNEL_ID,
    TEST_END,
    TEST_START,
    _any_real_show_id,
    _insert_fill_rule,
    _insert_fixed_slot,
    _make_show_eligible,
)

SATURDAY = 5
SUNDAY = 6


def _insert_weekend_fill_rule(conn, dows, category, weight=1.0, year_start=None, year_end=None):
    _insert_fill_rule(conn, TEST_START, TEST_END, category, weight, dows, year_start, year_end)


def test_saturday_rule_lookup_loads_only_saturday_rules_for_the_matching_era(db_conn):
    _insert_weekend_fill_rule(db_conn, [SATURDAY], TEST_CATEGORY, 1.0, 1970, 1974)
    scheduler = SyndicatedScheduler(db_conn)

    in_range = scheduler.get_fill_rules(TEST_CHANNEL_ID, 1972, SATURDAY, TEST_START)
    assert any(r["category"] == TEST_CATEGORY for r in in_range)

    out_of_range = scheduler.get_fill_rules(TEST_CHANNEL_ID, 1985, SATURDAY, TEST_START)
    assert not any(r["category"] == TEST_CATEGORY for r in out_of_range)

    wrong_day = scheduler.get_fill_rules(TEST_CHANNEL_ID, 1972, SUNDAY, TEST_START)
    assert not any(r["category"] == TEST_CATEGORY for r in wrong_day)


def test_sunday_rule_lookup_loads_only_sunday_rules_for_the_matching_era(db_conn):
    _insert_weekend_fill_rule(db_conn, [SUNDAY], TEST_CATEGORY, 1.0, 1965, 1969)
    scheduler = SyndicatedScheduler(db_conn)

    in_range = scheduler.get_fill_rules(TEST_CHANNEL_ID, 1968, SUNDAY, TEST_START)
    assert any(r["category"] == TEST_CATEGORY for r in in_range)

    wrong_day = scheduler.get_fill_rules(TEST_CHANNEL_ID, 1968, SATURDAY, TEST_START)
    assert not any(r["category"] == TEST_CATEGORY for r in wrong_day)


def test_weekday_behavior_is_unchanged_by_the_weekend_patch(db_conn):
    """A Monday request must not see Saturday/Sunday-only rows, and must still resolve via the
    already-implemented weekday daypart system (06:00-19:00, weekday_* columns)."""
    _insert_weekend_fill_rule(db_conn, [SATURDAY, SUNDAY], TEST_CATEGORY, 1.0, 1970, 1974)
    scheduler = SyndicatedScheduler(db_conn)

    monday_rules = scheduler.get_fill_rules(TEST_CHANNEL_ID, 1972, 0, TEST_START)
    assert not any(r["category"] == TEST_CATEGORY for r in monday_rules)

    assert daypart_column_for(time(6, 30), 0) == "weekday_early_morning"
    assert daypart_column_for(time(6, 30), SATURDAY) is None  # weekend day scope starts at 07:00
    assert daypart_column_for(time(7, 30), SATURDAY) == "saturday_early_morning"


def test_saturday_fixed_slot_takes_precedence_over_weekend_fill_rules(db_conn):
    show_id = _any_real_show_id(db_conn)
    _insert_fixed_slot(db_conn, SATURDAY, TEST_START, TEST_END, show_id)
    _insert_weekend_fill_rule(db_conn, [SATURDAY], TEST_CATEGORY, 1.0)
    scheduler = SyndicatedScheduler(db_conn)

    resolved = scheduler.resolve_syndicated_slot(TEST_CHANNEL_ID, 1972, SATURDAY, TEST_START, 1972)
    assert resolved == show_id


def test_sunday_fixed_slot_takes_precedence_over_weekend_fill_rules(db_conn):
    show_id = _any_real_show_id(db_conn)
    _insert_fixed_slot(db_conn, SUNDAY, TEST_START, TEST_END, show_id)
    _insert_weekend_fill_rule(db_conn, [SUNDAY], TEST_CATEGORY, 1.0)
    scheduler = SyndicatedScheduler(db_conn)

    resolved = scheduler.resolve_syndicated_slot(TEST_CHANNEL_ID, 1972, SUNDAY, TEST_START, 1972)
    assert resolved == show_id


def test_saturday_eligibility_respects_weekend_daypart_flag(db_conn):
    show_id = _any_real_show_id(db_conn)
    _make_show_eligible(db_conn, show_id, TEST_CATEGORY, 1970, 1974, "saturday_early_morning")
    scheduler = SyndicatedScheduler(db_conn)

    matching = scheduler.get_eligible_shows(TEST_CATEGORY, 1972, "saturday_early_morning", TEST_CHANNEL_ID, -999)
    assert show_id in matching

    # Same show/category/year, but a daypart column it wasn't flagged for -- must not match.
    non_matching = scheduler.get_eligible_shows(TEST_CATEGORY, 1972, "saturday_morning", TEST_CHANNEL_ID, -999)
    assert show_id not in non_matching


def test_network_weekend_seed_selects_real_historical_show_before_fallback(db_conn):
    """CBS's real 1968 Saturday schedule (from network_weekend_schedules_import.csv) seeded
    Jonny Quest (show_id 182) at 12:30 -- the existing (untouched) network scheduling path,
    Shows.get_scheduled_shows_id, must surface it.

    Deliberately not using ABC/1965 here: that (network, day, year) combination has multiple
    rows sharing the same air_time (e.g. Jonny Quest and The Beatles both at 08:30) -- a
    pre-existing characteristic that's now expressed as overlapping effective_start_date/
    effective_end_date ranges rather than a year collision (see
    migrate_schedule_date_ranges.py's printed summary for the still-unresolved cases). Asserting
    on one of those would be asserting on the ORDER BY schedule_id tiebreak, not real behavior."""
    shows = Shows(db_conn, "TV-CBS-2")
    schedule = shows.get_scheduled_shows_id(date(1968, 6, 15), SATURDAY)
    assert schedule.get("12:30:00") == 182


def test_network_weekend_unmatched_historical_row_does_not_crash(db_conn):
    """A historical weekend row with no matched show_id (e.g. 'Sandy Dreams', 1950, ABC Saturday
    -- never ingested into this library) must come back as None, not raise, so the existing
    fallback (Shows.get_available_show_id) can fill the slot instead."""
    shows = Shows(db_conn, "TV-ABC-7")
    schedule = shows.get_scheduled_shows_id(date(1950, 6, 15), SATURDAY)
    assert "19:00:00" in schedule
    assert schedule["19:00:00"] is None
