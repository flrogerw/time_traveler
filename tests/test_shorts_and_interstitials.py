"""
Tests for short-form packing, interstitial shows (School House Rock), and how the playlist
treats both: a block of packed shorts gets ONE commercial budget with a pod between the shorts,
and an interstitial gets no commercials of its own.

Real dev DB, rolled back per test (see conftest.db_conn). Playlists are written to pytest's tmp_path
via is_dev_mode, never to the real playlist directory. replication_year TEST_REPLICATION_YEAR (-1)
is used for everything that writes; playlist commercial selection uses year 1971 (read-only).
"""
from datetime import time

import pytest

import playlists.classes.Episodes as ep_module
from playlists.classes.Episodes import Episodes, channel_airs_interstitials
from playlists.classes.Playlists import Playlists
from playlists.classes.SyndicatedScheduler import SyndicatedScheduler
from tests.conftest import TEST_REPLICATION_YEAR
from tests.test_syndicated_scheduler import _make_show_eligible

CHANNEL_ID = 3  # TV-SYN-5
HOSTNAME = "TV-SYN-5"
SLOT_START = "09:00:00"
SLOT_SECONDS = 1800


def _show_id(conn, name):
    cur = conn.cursor()
    cur.execute("SELECT show_id FROM shows WHERE show_name = %s", (name,))
    return cur.fetchone()[0]


def _template_slot(conn, show_id, runtime="00:30:00"):
    conn.cursor().execute(
        """INSERT INTO schedule_template (channel_id, time_slot, show_id, runtime, replication_year, days_of_week)
           VALUES (%s, %s, %s, %s, %s, ARRAY[0])""",
        (CHANNEL_ID, SLOT_START, show_id, runtime, TEST_REPLICATION_YEAR),
    )
    return {"channel_id": CHANNEL_ID, "start_time": SLOT_START, "schedule_id": TEST_REPLICATION_YEAR,
            "duration": runtime}


def _slot_episodes(conn, show_id):
    slot = _template_slot(conn, show_id)
    episodes = Episodes(conn, HOSTNAME, TEST_REPLICATION_YEAR)
    # `year` must be a real year here (Shows.get_replacement_show builds a date from it); the
    # template rows still use TEST_REPLICATION_YEAR via slot["schedule_id"].
    return episodes.get_episodes_for_slot(slot, CHANNEL_ID, 0, 1971, "SYN", {SLOT_START: show_id})


def _parse_playlist(path):
    """[(media_path, start, stop)] in file order."""
    entries, start, stop = [], None, None
    for line in open(path):
        line = line.strip()
        if line.startswith("#EXTVLCOPT:start-time="):
            start = float(line.split("=")[1])
        elif line.startswith("#EXTVLCOPT:stop-time="):
            stop = float(line.split("=")[1])
        elif line and not line.startswith("#"):
            entries.append((line, start, stop))
            start = stop = None
    return entries


def _write_playlist(conn, tmp_path, monkeypatch, episodes):
    monkeypatch.chdir(tmp_path)
    for e in episodes:
        e["replication_year"] = 1971  # commercial selection year
    Playlists(conn, HOSTNAME, is_dev_mode=True).get_playlist(episodes, None)
    return [e for e in _parse_playlist(tmp_path / f"{HOSTNAME}_playlist.m3u")
            if "signoff" not in e[0] and "test_pattern" not in e[0]]


# ---------------------------------------------------------------- packing

def test_short_show_is_packed_into_one_slot_and_tagged(db_conn):
    eps = _slot_episodes(db_conn, _show_id(db_conn, "Bullwinkle Show"))

    assert len(eps) >= 3, "a 4-5 minute short should be packed several-to-a-slot"
    assert len({e["show_id"] for e in eps}) == 1
    content = sum(e["end_point"] - e["start_point"] for e in eps)
    assert content <= 0.85 * SLOT_SECONDS, "packed content must leave room for commercials"
    assert [e["pack_index"] for e in eps] == list(range(len(eps)))
    assert {e["pack_size"] for e in eps} == {len(eps)}
    assert {e["pack_slot_seconds"] for e in eps} == {SLOT_SECONDS}


def test_ordinary_half_hour_show_is_not_packed(db_conn):
    eps = _slot_episodes(db_conn, _show_id(db_conn, "Dick Van Dyke Show"))

    assert len(eps) == 1
    assert "pack_id" not in eps[0]


# ------------------------------------------------------------ playlist ads

def test_packed_block_gets_one_commercial_budget_with_pods_between_shorts(db_conn, tmp_path, monkeypatch):
    eps = _slot_episodes(db_conn, _show_id(db_conn, "Bullwinkle Show"))
    entries = _write_playlist(db_conn, tmp_path, monkeypatch, eps)

    ads = [e for e in entries if "/commercials/" in e[0]]
    shows = [e for e in entries if "/commercials/" not in e[0]]
    ad_seconds = sum(stop - start for _, start, stop in ads)
    content = sum(stop - start for _, start, stop in shows)

    assert len({path for path, _, _ in shows}) == len(eps), "one file per packed short (segments share it)"
    # One budget for the block: never a full half hour of ads per short.
    assert ad_seconds <= SLOT_SECONDS - content + 1
    assert ad_seconds < SLOT_SECONDS
    # Ads appear BETWEEN shorts too, not only before the first and after the last.
    if ads:
        first_show = entries.index(shows[0])
        last_show = entries.index(shows[-1])
        assert any("/commercials/" in e[0] for e in entries[first_show:last_show])


def test_interstitial_gets_no_commercials_of_its_own(db_conn, tmp_path, monkeypatch):
    shr = _show_id(db_conn, "School House Rock")
    episodes = Episodes(db_conn, HOSTNAME, TEST_REPLICATION_YEAR)
    clip = episodes.get_next_interstitial(shr, CHANNEL_ID, TEST_REPLICATION_YEAR, SLOT_START)
    assert clip is not None and clip["interstitial"] is True

    entries = _write_playlist(db_conn, tmp_path, monkeypatch, [clip])

    assert len(entries) == 1
    assert "/commercials/" not in entries[0][0]


# ---------------------------------------------------------- interstitials

def test_interstitial_cycles_unaired_first_then_least_recently_aired(db_conn):
    shr = _show_id(db_conn, "School House Rock")
    episodes = Episodes(db_conn, HOSTNAME, TEST_REPLICATION_YEAR)
    cur = db_conn.cursor()
    # Start from a channel with no School House Rock history (a real playlist run may have logged
    # some); rolled back with the rest of the test.
    cur.execute("DELETE FROM broadcast_log WHERE channel_id = %s AND show_id = %s", (CHANNEL_ID, shr))
    cur.execute("SELECT count(*) FROM episodes WHERE show_id = %s", (shr,))
    total = cur.fetchone()[0]

    picked = [episodes.get_next_interstitial(shr, CHANNEL_ID, TEST_REPLICATION_YEAR, SLOT_START)["episode_id"]
              for _ in range(total)]
    assert len(set(picked)) == total, "every episode airs once before any repeats"

    again = episodes.get_next_interstitial(shr, CHANNEL_ID, TEST_REPLICATION_YEAR, SLOT_START)
    assert again["episode_id"] == picked[0], "after a full cycle, the least recently aired comes back first"

    # The replay is the same episode on the same date, so it is not logged a second time.
    cur.execute("SELECT count(*) FROM broadcast_log WHERE channel_id = %s AND show_id = %s AND replication_year = %s",
                (CHANNEL_ID, shr, TEST_REPLICATION_YEAR))
    assert cur.fetchone()[0] == total


def test_interstitial_show_is_found_only_in_its_eligible_block_and_years(db_conn):
    shr = _show_id(db_conn, "School House Rock")
    _make_show_eligible(db_conn, shr, "interstitial", 1973, 1985, "saturday_morning")
    episodes = Episodes(db_conn, HOSTNAME, TEST_REPLICATION_YEAR)

    assert episodes.get_interstitial_show_id(1975, 5, time(9, 30)) == shr
    assert episodes.get_interstitial_show_id(1986, 5, time(9, 30)) is None    # after its 1985 end
    assert episodes.get_interstitial_show_id(1972, 5, time(9, 30)) is None    # before its 1973 start
    assert episodes.get_interstitial_show_id(1975, 0, time(9, 30)) is None    # weekday
    assert episodes.get_interstitial_show_id(1975, 6, time(9, 30)) is None    # Sunday
    assert episodes.get_interstitial_show_id(1975, 5, time(14, 30)) is None   # Saturday afternoon


def test_interstitials_air_on_syndicated_and_abc_channels_only():
    assert channel_airs_interstitials("syndicated", "SYN")
    assert channel_airs_interstitials("network", "ABC")
    assert not channel_airs_interstitials("network", "CBS")
    assert not channel_airs_interstitials("network", "NBC")


def test_interstitial_show_is_never_picked_to_fill_a_slot(db_conn):
    shr = _show_id(db_conn, "School House Rock")
    _make_show_eligible(db_conn, shr, "interstitial", 1973, 1985, "saturday_morning")
    scheduler = SyndicatedScheduler(db_conn)

    for category in ("kids", "family", "interstitial_is_not_a_fill_rule"):
        assert shr not in scheduler.get_eligible_shows(category, 1975, "saturday_morning", CHANNEL_ID, TEST_REPLICATION_YEAR)


# ------------------------------------------------------- ad audience rules

def test_children_audience_never_gets_beer_smoking_or_other_adult_ads(db_conn):
    from playlists.classes.Commercials import CHILDREN_ALLOWED_TYPES, Commercials, AUDIENCE_CHILDREN

    commercials = Commercials(db_conn)
    for year in (1962, 1971, 1975, 1980):
        for ad in commercials.get_commercials(1800, year, audience=AUDIENCE_CHILDREN):
            assert ad["commercial_type"] in CHILDREN_ALLOWED_TYPES + ["hygiene"], ad
            if ad["commercial_type"] == "hygiene":
                assert any(k in ad["commercial_sponser"].lower() for k in ("crest", "toothpaste", "baby", "bubble"))

    unrestricted = {ad["commercial_type"] for ad in commercials.get_commercials(1800, 1965)}
    assert {"beer", "smoking"} & unrestricted, "no audience = every type, so the filter is what removes them"


def _episode_stub(show_id, hour, interstitial=False):
    return {"type": "episode", "show_id": show_id, "time_slot": time(hour, 0), "interstitial": interstitial}


def test_audience_is_a_morning_rule_for_childrens_programming(db_conn, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    playlists = Playlists(db_conn, HOSTNAME, is_dev_mode=True)
    cartoon = _show_id(db_conn, "Bullwinkle Show")
    sitcom = _show_id(db_conn, "Dick Van Dyke Show")

    assert playlists._audience_for(_episode_stub(cartoon, 9), dow=0) == "children"     # morning cartoon
    assert playlists._audience_for(_episode_stub(cartoon, 14), dow=0) is None          # afternoon cartoon: any ad
    assert playlists._audience_for(_episode_stub(cartoon, 20), dow=2) is None          # primetime: any ad
    assert playlists._audience_for(_episode_stub(sitcom, 10), dow=0) is None           # weekday-morning adult show
    assert playlists._audience_for(_episode_stub(sitcom, 9), dow=5) == "children"      # Saturday-morning slot
    assert playlists._audience_for(_episode_stub(sitcom, 14), dow=5) is None           # Saturday afternoon
    assert playlists._audience_for(_episode_stub(cartoon, 9, interstitial=True), dow=5) == "children"


def test_interstitial_time_comes_out_of_the_programs_ad_break_not_the_schedule(db_conn, tmp_path, monkeypatch):
    program = _slot_episodes(db_conn, _show_id(db_conn, "Dick Van Dyke Show"))
    clip = Episodes(db_conn, HOSTNAME, TEST_REPLICATION_YEAR).get_next_interstitial(
        _show_id(db_conn, "School House Rock"), CHANNEL_ID, TEST_REPLICATION_YEAR, SLOT_START)
    entries = _write_playlist(db_conn, tmp_path, monkeypatch, program + [clip])

    airtime = sum(stop - start for _, start, stop in entries)
    assert airtime <= SLOT_SECONDS + 1, "program + ads + interstitial must still fit the 30-minute slot"


def test_true_duration_comes_from_the_episodes_table(db_conn):
    from psycopg.rows import dict_row
    cur = db_conn.cursor()
    cur.execute("""SELECT e.episode_id, e.end_point - e.start_point,
                          COALESCE((SELECT SUM(resume_point - break_point) FROM commercial_breaks WHERE media_id = e.episode_id), 0)
                   FROM episodes e WHERE e.show_id = %s ORDER BY e.episode_id LIMIT 1""",
                (_show_id(db_conn, "Nanny and the Professor"),))  # has no episode_durations rows at all
    episode_id, span, breaks = cur.fetchone()

    duration = Episodes.get_true_duration(db_conn.cursor(row_factory=dict_row), episode_id)
    assert abs(float(duration) - (float(span) - float(breaks))) < 0.01


def test_next_episode_prefers_ones_that_fit_the_slot(db_conn):
    """It Takes a Thief is a one-hour show with a 64-minute episode; that episode must not be picked
    for its 60-minute slot while episodes that fit remain."""
    thief = _show_id(db_conn, "It Takes a Thief")
    cur = db_conn.cursor()
    cur.execute("""SELECT count(*) FROM episodes WHERE show_id = %s AND end_point - start_point <= 3600 * 1.05""", (thief,))
    fitting = cur.fetchone()[0]
    cur.execute("""SELECT count(*) FROM episodes WHERE show_id = %s AND end_point - start_point > 3600 * 1.05""", (thief,))
    assert cur.fetchone()[0] >= 1, "test data changed: no over-long It Takes a Thief episode left"

    slot = _template_slot(db_conn, thief, "01:00:00")
    episodes = Episodes(db_conn, HOSTNAME, TEST_REPLICATION_YEAR)
    for _ in range(fitting):
        ep = episodes.get_next_episode(slot, 0)
        assert ep is not None
        assert ep["end_point"] - ep["start_point"] <= 3600 * 1.05

    # Once every episode that fits has aired, an over-long one is still picked rather than starving the show.
    leftover = episodes.get_next_episode(slot, 0)
    assert leftover is not None and leftover["end_point"] - leftover["start_point"] > 3600 * 1.05


def test_show_duration_lookup_does_not_crash_when_an_episode_exceeds_every_listed_length(db_conn):
    from playlists.classes.Shows import Shows

    thief = _show_id(db_conn, "It Takes a Thief")  # listed at [3600]
    assert Shows(db_conn, HOSTNAME).get_show_duration(thief, 3853.6) == 3600
