#!/usr/bin/env python3
"""
schedule_filler.py

Heuristic TV schedule filler:
- Loads shows metadata CSV and historical schedule CSV
- Builds slots for a target day (configurable 30/60m)
- Fills missing slots using heuristics:
    * time-of-day -> genre bias
    * network affinity
    * adjacency (try to match neighbor genre for blocks)
- Validates and writes out a filled schedule CSV

CSV formats expected (simple examples below):
shows.csv header:
show_id,title,genre,duration_seconds,start_year,end_year,network,popularity

historical_schedule.csv header:
date,channel,start_time,show_id
( date ISO YYYY-MM-DD, start_time HH:MM )

Example usage:
python schedule_filler.py --shows shows.csv --historical historical.csv \
    --out filled.csv --date 1972-09-15 --channel CBS --slot-minutes 30
"""

from __future__ import annotations

import calendar
import random
from datetime import datetime, date, time, timedelta
from pprint import pprint
from typing import List, Dict, Tuple, Optional
import argparse
import sys
import logging
import psycopg2
from psycopg2.extras import DictCursor, RealDictCursor, RealDictRow


def get_db_connection() -> psycopg2.extensions.connection:
    """Establish and return a database connection."""
    try:
        conn = psycopg2.connect(
            database="time_traveler",
            user="postgres",
            password="m06Ar14u",
            host="192.168.1.201",
            port=5432
        )
        logging.info("Database connection established.")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to the database: {e}")
        raise


def load_shows_db() -> dict[int, dict]:
    conn = get_db_connection()
    shows = {}
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM shows WHERE show_type != 'children';")
        for row in cur.fetchall():
            genres = [g.lower() for g in row['show_genre'].split(',')]  # already a Python list
            networks = [n.lower() for n in row['show_network']]  # already a Python list
            durations = [int(d) for d in row['show_duration']]
            shows[row['show_id']] = {
                'show_id': row['show_id'],
                'title': row['show_name'],
                'genres': genres,
                'durations': durations,
                'start_year': row['airdate_start'].year,
                'end_year': row['airdate_end'].year,
                'networks': networks,
                # 'popularity': row['popularity'] or 1.0
                'popularity': 1.0
            }
    conn.close()
    return shows


def load_history_db() -> dict[tuple, int]:
    conn = get_db_connection()
    history = {}
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT air_date, network, air_time, show_id FROM schedules;")
        for row in cur.fetchall():
            key = (row['air_date'], row['network'], row['air_time'])
            history[key] = row['show_id']
    conn.close()
    return history


def get_nearest_shows(show_date: date, network: str) -> list[RealDictRow]:
    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""WITH ranked AS (
                                SELECT
                                    e.episode_id,
                                    e.episode_airdate,
                                    s.show_id,
                                    s.show_name,
                                    ROW_NUMBER() OVER (
                                        PARTITION BY s.show_id
                                        ORDER BY ABS(e.episode_airdate - DATE %s)
                                    ) AS rn
                                FROM episodes e
                                JOIN shows s ON s.show_id = e.show_id
                                WHERE %s = ANY(s.show_network)
                                  AND s.show_type != 'children'
                                  AND e.episode_airdate <= DATE %s
                            )
                            SELECT *
                            FROM ranked
                            WHERE rn = 1 
                            ORDER BY episode_airdate DESC;""", (show_date, network, show_date))
        rows = cur.fetchall()

        for row in rows:
            airdate = row["episode_airdate"]  # dict lookup by column name
            if isinstance(airdate, date):  # make sure it's a datetime.date
                day_of_week = airdate.strftime("%A")  # e.g. "Friday"
                row["day_of_week"] = day_of_week  # add new field

        conn.close()
    return rows


def build_slots_for_day(start: time, end: time, slot_minutes: int) -> List[time]:
    slots = []
    # create a temporary reference day for arithmetic
    dt = datetime.combine(datetime.today(), start)
    end_dt = datetime.combine(datetime.today(), end)
    while dt < end_dt:
        slots.append(dt.time())
        dt += timedelta(minutes=slot_minutes)
    return slots


def get_genre_bias(t: time, show_date: date) -> list[str]:
    """
    Return the preferred genres for a given year and hour of day.
    Automatically selects the closest decade and hour range.
    """
    # Define genre bias by decade
    TIME_OF_DAY_GENRE_BIAS_50s = {
        range(6, 18): ['local', 'family'],
        range(18, 19): ['news', 'family'],
        range(19, 21): ['family', 'comedy'],
        range(21, 23): ['variety', 'drama'],
        range(23, 24): ['rerun', 'talk']
    }

    TIME_OF_DAY_GENRE_BIAS_60s = {
        range(6, 18): ['local', 'family', 'comedy'],
        range(18, 19): ['news', 'family', 'comedy'],
        range(19, 21): ['comedy', 'western', 'family', 'drama'],
        range(21, 23): ['drama', 'crime', 'variety', 'thriller'],
        range(23, 24): ['talk', 'rerun', 'variety']
    }

    TIME_OF_DAY_GENRE_BIAS_70s = {
        range(6, 18): ['local', 'family', 'comedy'],
        range(18, 19): ['news', 'family', 'comedy'],
        range(19, 21): ['comedy', 'family', 'drama', 'western'],
        range(21, 23): ['drama', 'crime', 'thriller', 'comedy'],
        range(23, 24): ['talk', 'rerun', 'drama']
    }

    TIME_OF_DAY_GENRE_BIAS_80s = {
        range(6, 18): ['local', 'family', 'comedy'],
        range(18, 19): ['news', 'family', 'comedy'],
        range(19, 21): ['comedy', 'family', 'drama', 'sitcom'],
        range(21, 23): ['drama', 'crime', 'thriller', 'action'],
        range(23, 24): ['talk', 'rerun', 'variety', 'drama']
    }

    GENRE_BIAS_BY_DECADE = {
        1950: TIME_OF_DAY_GENRE_BIAS_50s,
        1960: TIME_OF_DAY_GENRE_BIAS_60s,
        1970: TIME_OF_DAY_GENRE_BIAS_70s,
        1980: TIME_OF_DAY_GENRE_BIAS_80s
    }

    # Pick closest decade
    decade = min(GENRE_BIAS_BY_DECADE.keys(), key=lambda d: abs(d - show_date.year))
    bias_table = GENRE_BIAS_BY_DECADE[decade]

    # Find the hour range that matches
    for hour_range, genres in bias_table.items():
        if t.hour in hour_range:
            return genres

    # Fallback if hour not found
    return ['comedy', 'drama', 'family']


def pick_duration(show, slot_length):
    return min(show['durations'], key=lambda d: abs(d - slot_length))


def filter_candidates_by_year_and_duration(shows: Dict[int, Dict], year: int, slot_minutes: int) -> List[Dict]:
    """
    Return list of shows that were active in the given year and whose duration
    fits into the slot (allow equal or smaller durations).
    Assumes duration is in seconds.
    """
    slot_seconds = slot_minutes * 60
    out = []
    for s in shows.values():
        if s['start_year'] <= year <= s['end_year']:
            chosen_duration = pick_duration(s, slot_seconds)
            if chosen_duration <= slot_seconds:
                out.append(s)
    return out


def score_show_candidate(show: dict, target_network: str, preferred_genres: list[str], prev_genre: str | None) -> float:
    score = show.get('popularity', 1.0)

    # Network affinity
    if target_network and target_network.lower() in show['networks']:
        score *= 2.0

    # Genre preference
    show_genres = show.get('genres', [])
    for g in preferred_genres:
        if g in show_genres:
            rank = preferred_genres.index(g)
            score *= (1.5 + (len(preferred_genres) - rank) * 0.2)
            break  # only apply best match

    # Adjacency (if prev slot had a genre that overlaps)
    if prev_genre and prev_genre in show_genres:
        score *= 1.25

    return score


def fill_day_schedule(
        shows: Dict[int, Dict],
        history_map: Dict[Tuple[date, str, time], int],
        target_date: date,
        channel: str,
        start: time = time(18, 0),
        end: time = time(23, 0),
        slot_minutes: int = 30,
        seed: Optional[int] = None
) -> List[Dict]:
    if seed is not None:
        random.seed(seed)

    slots = build_slots_for_day(start, end, slot_minutes)
    filled = []

    # helper to find latest previous slot that has entry in 'filled'
    def last_filled_genre():
        if not filled:
            return None
        # take last entry's show_id -> genre
        last_show_id = filled[-1]['show_id']
        return shows[last_show_id]['genres'] if last_show_id in shows else None

    # Precompute eligible shows once per target year
    year_candidates = filter_candidates_by_year_and_duration(shows, target_date.year, slot_minutes)

    for slot in slots:
        key = (target_date, channel, slot)
        if key in history_map:
            # keep historical if present
            sid = history_map[key]
            filled.append({'date': target_date, 'channel': channel, 'start_time': slot, 'show_id': sid})
            continue

        # Determine preferred genres by time-of-day
        preferred_genres = get_genre_bias(slot, target_date)
        prev_genre = last_filled_genre()

        # Candidate pool: those in year_candidates whose genre is in preferred_genres first
        pool_primary = [
            s for s in year_candidates
            if any(g in preferred_genres for g in s['genres'])
        ]
        pool_secondary = [
            s for s in year_candidates
            if not any(g in preferred_genres for g in s['genres'])
        ]

        # Score and select from primary else secondary; if nothing, fall back to any show active that year
        chosen = None
        for pool in (pool_primary, pool_secondary, year_candidates):
            if not pool:
                continue
            weights = [score_show_candidate(s, channel, preferred_genres, prev_genre) for s in pool]
            # avoid zero-weight
            if sum(weights) <= 0:
                continue
            chosen = random.choices(pool, weights=weights, k=1)[0]
            break

        if chosen is None:
            # as last resort, put a local filler (we will use a placeholder show_id -1)
            filled.append({'date': target_date, 'channel': channel, 'start_time': slot, 'show_id': -1})
        else:
            filled.append({'date': target_date, 'channel': channel, 'start_time': slot, 'show_id': chosen['show_id']})

    return filled


# ---------- Validation helpers ----------

def validate_schedule(seq: List[Dict], shows: Dict[int, Dict], slot_minutes: int) -> List[str]:
    """
    Basic validation: ensure durations fit slot and no overlaps (by construction they shouldn't).
    Return list of error strings (empty if ok).
    """
    errors = []
    slot_seconds = slot_minutes * 60
    for e in seq:
        sid = e['show_id']
        if sid == -1:
            continue
        s = shows.get(sid)
        if not s:
            errors.append(f"Unknown show_id {sid} at {e['start_time']}")
            continue
        if all(d > slot_seconds for d in s['durations']):
            errors.append(
                f"Show {sid} (durs {s['durations']}) doesn't fit slot size {slot_seconds} at {e['start_time']}"
            )
    return errors


def nth_weekday_in_month(year, month, weekday, n):
    """
    Find the nth occurrence of a weekday in a given month/year.
    weekday: 0=Monday ... 6=Sunday
    n: occurrence index (1=first, 2=second, etc.)
    """
    c = calendar.Calendar()
    days = [d for d in c.itermonthdates(year, month)
            if d.month == month and d.weekday() == weekday]

    if n <= len(days):
        return days[n - 1]
    else:
        # fallback: return last occurrence if n too large
        return days[-1]


def equivalent_date(target_year, month=None, day=None):
    """
    Map a given date to its equivalent in target_year.
    If month/day not provided, defaults to today.
    Returns (date, weekday_name).
    """
    today = date.today()
    src_date = date(today.year, month or today.month, day or today.day)

    weekday = src_date.weekday()
    month = src_date.month

    # find which occurrence of that weekday in the source month
    c = calendar.Calendar()
    month_days = [d for d in c.itermonthdates(src_date.year, month)
                  if d.month == month and d.weekday() == weekday]
    occurrence = month_days.index(src_date) + 1

    # get equivalent date in target year
    eq_date = nth_weekday_in_month(target_year, month, weekday, occurrence)

    # return both date object and formatted string
    return eq_date, eq_date.strftime("%A")

def merge_episode_ids(shows_dict, episodes_list):
    for ep in episodes_list:
        sid = ep.get("show_id")
        if sid in shows_dict:
            shows_dict[sid]["episode_id"] = ep["episode_id"]
    return shows_dict

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Target date YYYY-MM-DD")
    parser.add_argument("--channel", required=True, help="Network/channel name")
    parser.add_argument("--start", default="18:00", help="Start time HH:MM")
    parser.add_argument("--end", default="23:00", help="End time HH:MM or 00:00")
    parser.add_argument("--seed", default=None, type=int)
    parser.add_argument("--slot-minutes", default=30, type=int)

    args = parser.parse_args()

    shows = load_shows_db()
    target_date = datetime.fromisoformat(args.date).date()
    start = datetime.strptime(args.start, "%H:%M").time()
    end = datetime.strptime(args.end, "%H:%M").time()

    eq_date, weekday = equivalent_date(target_date.year, target_date.month, target_date.day)
    nearest_shows = get_nearest_shows(eq_date, args.channel)

    pprint(merge_episode_ids(shows, nearest_shows))

    history = load_history_db()

    filled = fill_day_schedule(
        shows, history, target_date, args.channel, start=start, end=end, slot_minutes=args.slot_minutes, seed=args.seed
    )

    errs = validate_schedule(filled, shows, args.slot_minutes)
    if errs:
        print("Validation errors:", file=sys.stderr)
        for e in errs:
            print(" -", e, file=sys.stderr)

    # write_filled_csv(args.out, filled)
    #pprint(filled)
    # print(f"Wrote {len(filled)} rows to {args.out}")


if __name__ == "__main__":
    main()
