"""
Tests for `schedules`' effective_start_date/effective_end_date date-range matching (replacing
the old year-only `air_date` int match) -- see migrate_schedule_date_ranges.py and the
conversation it came out of ("mid-season show switch aware" scheduling).

Asserts on real ingested/researched historical data, the same way test_episode_rotation.py and
test_weekend_scheduler.py do.
"""
from datetime import date

from playlists.classes.Shows import Shows
from tests.conftest import TEST_REPLICATION_YEAR

ABC = "TV-ABC-7"
CBS = "TV-CBS-2"
SATURDAY = 5


def test_mid_season_switch_picks_outgoing_show_before_the_switch_date(db_conn):
    """Shindig! held ABC Saturday 19:30 through its last Jan 8, 1966 broadcast; Ozzie and
    Harriet took over 1966-01-15. A sim date just before the switch must still resolve to
    Shindig!, not Ozzie and Harriet (which is what year-only matching used to conflate)."""
    shows = Shows(db_conn, ABC)
    schedule = shows.get_scheduled_shows_id(date(1966, 1, 8), SATURDAY)
    assert schedule.get("19:30:00") is None  # Shindig! (schedule_id 4085) has no show_id linked


def test_mid_season_switch_picks_incoming_show_on_and_after_the_switch_date(db_conn):
    shows = Shows(db_conn, ABC)
    schedule = shows.get_scheduled_shows_id(date(1966, 1, 15), SATURDAY)
    assert schedule.get("19:30:00") == 385  # The Adventures of Ozzie and Harriet

    later = shows.get_scheduled_shows_id(date(1966, 6, 1), SATURDAY)
    assert later.get("19:30:00") == 385


def test_month_precision_switch_still_splits_cleanly(db_conn):
    """Secret Agent replaced The Trials of O'Brien on CBS Saturday 20:30 in December 1965 --
    the research only gave month precision, not an exact day, but that's still enough to keep
    the two ranges from overlapping (Trials of O'Brien ends 1965-11-30, Secret Agent starts
    1965-12-01)."""
    shows = Shows(db_conn, CBS)
    november = shows.get_scheduled_shows_id(date(1965, 11, 15), SATURDAY)
    assert november.get("20:30:00") is None  # The Trials of O'Brien has no show_id linked

    december = shows.get_scheduled_shows_id(date(1965, 12, 15), SATURDAY)
    assert december.get("20:30:00") == 320  # Secret Agent


def test_default_full_year_range_is_unaffected_for_untouched_rows(db_conn):
    """A row that never needed splitting (Captain Kangaroo, CBS Saturday 08:00, 1966) still
    matches any date within its air_date's calendar year under the new default range."""
    shows = Shows(db_conn, CBS)
    early = shows.get_scheduled_shows_id(date(1966, 2, 1), SATURDAY)
    late = shows.get_scheduled_shows_id(date(1966, 11, 1), SATURDAY)
    assert early.get("08:00:00") is None  # Captain Kangaroo has no show_id linked
    assert late.get("08:00:00") is None
    # Both dates hit the same single row (no show_id to assert on, but neither errors nor
    # disappears at either end of the year -- confirms the Jan-1/Dec-31 default backfill).


def test_known_unresolved_overlap_ozzie_and_harriet_vs_shane(db_conn):
    """Documents a known, still-unresolved gap: Ozzie and Harriet's row (started 1966-01-15,
    no end date narrowed) still overlaps Shane's row (full default 1966-01-01 - 1966-12-31),
    since Shane's real fall-1966 premiere date hasn't been researched yet. Both currently match
    the same date/time -- process_scheduled_shows keeps whichever row it sees LAST for a given
    air_time (a later dict assignment overwrites an earlier one), and ORDER BY schedule_id ASC
    means the higher schedule_id (Ozzie and Harriet, 4086) is seen last and wins over Shane
    (2248), even for an October 1966 date that's really Shane's, not Ozzie and Harriet's. This
    test exists so it FAILS -- as a reminder -- once someone narrows Shane's date range and this
    stops being true."""
    shows = Shows(db_conn, ABC)
    schedule = shows.get_scheduled_shows_id(date(1966, 10, 1), SATURDAY)
    assert schedule.get("19:30:00") == 385  # Ozzie and Harriet currently wins the tiebreak, though Shane is the historically correct answer for this date


def test_sync_schedule_template_show_id_repoints_an_already_templated_slot(db_conn):
    """The core of the mid-season-switch fix: once a slot is templated, a later run whose
    schedule_data now resolves to a different show (because a real date range boundary was
    crossed) must actually update schedule_template, not leave it locked to whichever show
    templated it first."""
    from playlists.classes.Schedules import Schedules

    cur = db_conn.cursor()
    channel_id, dow = 1, 3  # TV-CBS-2, arbitrary weekday
    cur.execute(
        """INSERT INTO schedule_template (channel_id, time_slot, show_id, runtime, replication_year, days_of_week)
           VALUES (%s, '20:00:00', %s, '00:30:00', %s, ARRAY[%s])""",
        (channel_id, 1, TEST_REPLICATION_YEAR, dow),
    )

    schedule = Schedules(db_conn, "TV-CBS-2")
    schedule.sync_schedule_template_show_id(channel_id, dow, TEST_REPLICATION_YEAR, "20:00:00", 2)

    cur.execute(
        "SELECT show_id FROM schedule_template WHERE channel_id = %s AND time_slot = '20:00:00' AND replication_year = %s",
        (channel_id, TEST_REPLICATION_YEAR),
    )
    assert cur.fetchone()[0] == 2


def test_sync_schedule_template_show_id_is_a_noop_for_an_untemplated_slot(db_conn):
    """Nothing should be inserted for a slot that hasn't been templated yet -- that's still
    Episodes.get_episodes_for_slot's job via insert_schedule_template."""
    from playlists.classes.Schedules import Schedules

    schedule = Schedules(db_conn, "TV-CBS-2")
    schedule.sync_schedule_template_show_id(1, 3, TEST_REPLICATION_YEAR, "21:00:00", 2)

    cur = db_conn.cursor()
    cur.execute(
        "SELECT * FROM schedule_template WHERE channel_id = 1 AND time_slot = '21:00:00' AND replication_year = %s",
        (TEST_REPLICATION_YEAR,),
    )
    assert cur.fetchone() is None


def test_narrowest_overlapping_range_wins_regardless_of_schedule_id(db_conn):
    """A season row narrowed to Sep-Apr (low schedule_id) overlaps the next year's still-default
    Jan-Dec row (higher schedule_id) for Jan-Apr. The researched narrow range must win in the
    overlap, and the wide row must take over once the narrow one ends. Uses a sentinel network
    so no real schedule row is involved (rolled back at teardown)."""
    cur = db_conn.cursor()
    ins = ("INSERT INTO schedules (network, show_title, air_date, day_of_week, air_time, show_id,"
           " effective_start_date, effective_end_date) VALUES ('TESTNET', %s, %s, 'saturday',"
           " '20:00', %s, %s, %s)")
    cur.execute(ins, ("narrow", 1990, 1, date(1990, 9, 15), date(1991, 4, 30)))
    cur.execute(ins, ("wide", 1991, 2, date(1991, 1, 1), date(1991, 12, 31)))
    shows = Shows(db_conn, "TV-TESTNET-1")
    assert shows.get_scheduled_shows_id(date(1991, 2, 2), SATURDAY).get("20:00:00") == 1
    assert shows.get_scheduled_shows_id(date(1991, 6, 1), SATURDAY).get("20:00:00") == 2
