"""
Tests for Episodes.get_next_episode()'s umbrella/rotation handling (NBC Mystery Movie and
similar anthology shows backed by the `sub_shows` table) and the ordinary single-show path it
must leave unaffected. See CLAUDE.md's "Umbrella/rotation shows" section for the design.

These tests run against the real dev DB (see conftest.db_conn) and assert on real ingested data
(NBC Mystery Movie's component shows: Columbo=101, Banacek=119, McMillan & Wife=130,
McCloud=404). If that seed data changes, these tests will need updating to match.
"""
from playlists.classes.Episodes import Episodes

from tests.conftest import TEST_REPLICATION_YEAR

NBC_MYSTERY_MOVIE_CHANNEL_ID = 2  # TV-NBC-4
NBC_MYSTERY_MOVIE_SHOW_ID = 178
NBC_MYSTERY_MOVIE_COMPONENT_IDS = {101, 119, 130, 404}  # Columbo, Banacek, McMillan & Wife, McCloud


def _insert_umbrella_slot(conn, channel_id=NBC_MYSTERY_MOVIE_CHANNEL_ID, dow=3):
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO schedule_template (channel_id, time_slot, show_id, runtime, replication_year, days_of_week)
           VALUES (%s, '20:00:00', %s, '01:30:00', %s, ARRAY[%s])""",
        (channel_id, NBC_MYSTERY_MOVIE_SHOW_ID, TEST_REPLICATION_YEAR, dow),
    )


def _time_slot(channel_id=NBC_MYSTERY_MOVIE_CHANNEL_ID):
    return {"channel_id": channel_id, "start_time": "20:00:00", "schedule_id": TEST_REPLICATION_YEAR}


def test_umbrella_rotation_uses_real_airdate_order_and_no_repeats(db_conn):
    _insert_umbrella_slot(db_conn)
    episodes = Episodes(db_conn, "TV-NBC-4", TEST_REPLICATION_YEAR)
    slot = _time_slot()

    seen_episode_ids = []
    seen_show_ids = set()
    last_airdate = None
    for _ in range(15):
        ep = episodes.get_next_episode(slot, 3)
        assert ep is not None, "umbrella slot ran out of episodes during the test window"

        # Every pick must be a real component show, never the umbrella's own id.
        assert ep["show_id"] in NBC_MYSTERY_MOVIE_COMPONENT_IDS
        assert ep["show_id"] != NBC_MYSTERY_MOVIE_SHOW_ID

        # No episode should ever be picked twice.
        assert ep["episode_id"] not in seen_episode_ids
        seen_episode_ids.append(ep["episode_id"])
        seen_show_ids.add(ep["show_id"])

        # Real historical airdate order must be non-decreasing across picks.
        if last_airdate is not None:
            assert ep["episode_airdate"] >= last_airdate
        last_airdate = ep["episode_airdate"]

    # Over 15 weeks starting Sept 1971, the rotation should have pulled from more than one
    # component series (this is the actual "rotation" behavior, not just one show on a loop).
    assert len(seen_show_ids) > 1


def test_umbrella_rotation_records_real_component_show_id_in_broadcast_log(db_conn):
    _insert_umbrella_slot(db_conn)
    episodes = Episodes(db_conn, "TV-NBC-4", TEST_REPLICATION_YEAR)
    slot = _time_slot()

    ep = episodes.get_next_episode(slot, 3)
    assert ep is not None

    cur = db_conn.cursor()
    cur.execute(
        "SELECT show_id FROM broadcast_log WHERE channel_id = %s AND replication_year = %s AND episode_id = %s",
        (NBC_MYSTERY_MOVIE_CHANNEL_ID, TEST_REPLICATION_YEAR, ep["episode_id"]),
    )
    row = cur.fetchone()
    assert row is not None
    assert row[0] == ep["show_id"]
    assert row[0] != NBC_MYSTERY_MOVIE_SHOW_ID


def test_ordinary_show_unaffected_by_umbrella_lookup(db_conn):
    """A show with no row in sub_shows (e.g. McCloud on its own) must still be selected by
    the normal season/episode order, not accidentally treated as an umbrella."""
    mccloud_show_id = 404
    channel_id = NBC_MYSTERY_MOVIE_CHANNEL_ID
    cur = db_conn.cursor()
    cur.execute(
        """INSERT INTO schedule_template (channel_id, time_slot, show_id, runtime, replication_year, days_of_week)
           VALUES (%s, '21:00:00', %s, '01:00:00', %s, ARRAY[3])""",
        (channel_id, mccloud_show_id, TEST_REPLICATION_YEAR),
    )

    episodes = Episodes(db_conn, "TV-NBC-4", TEST_REPLICATION_YEAR)
    slot = {"channel_id": channel_id, "start_time": "21:00:00", "schedule_id": TEST_REPLICATION_YEAR}

    ep = episodes.get_next_episode(slot, 3)
    assert ep is not None
    assert ep["show_id"] == mccloud_show_id
    assert ep["show_season_number"] == 1
    assert ep["episode_number"] == 1
