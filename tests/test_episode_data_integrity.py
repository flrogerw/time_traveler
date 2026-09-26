"""
Regression guards for the episode-numbering data-quality bugs found and fixed in the
2026-09-21 session (see COMMERCIAL_BREAK_AUDIT.md). Episode selection (Episodes.get_next_episode)
assumes (show_id, show_season_number, episode_number) is unique -- these tests catch that
invariant breaking again, whether from a future bad ingestion run or a bad manual fix.
"""


def test_no_episode_number_collisions(db_conn):
    """No show should have two episodes sharing (season, episode_number) -- this was the root
    cause of get_next_episode() returning the same/wrong episode, and of ~130 mis-numbered
    cartoon-package/mis-filed episodes across ten-plus shows."""
    cur = db_conn.cursor()
    cur.execute(
        """SELECT show_id, show_season_number, episode_number, COUNT(*)
           FROM episodes
           GROUP BY show_id, show_season_number, episode_number
           HAVING COUNT(*) > 1"""
    )
    collisions = cur.fetchall()
    assert collisions == [], f"episode_number collisions found: {collisions}"


def test_nbc_mystery_movie_sub_shows_includes_all_components(db_conn):
    """sub_shows[178] (NBC Mystery Movie) must list all four real component series -- McCloud
    was missing from this until fixed this session, which made it unreachable by the
    umbrella-rotation logic in Episodes.get_next_episode()."""
    cur = db_conn.cursor()
    cur.execute("SELECT sub_show_ids FROM sub_shows WHERE show_id = 178")
    row = cur.fetchone()
    assert row is not None, "NBC Mystery Movie (show_id 178) has no sub_shows row"
    assert set(row[0]) == {101, 119, 130, 404}  # Columbo, Banacek, McMillan & Wife, McCloud
