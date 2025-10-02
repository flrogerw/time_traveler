import logging
import random
from datetime import datetime, timedelta, time, date
from collections import defaultdict
from pprint import pprint
from typing import Any

import psycopg2
from more_itertools.more import raise_
from psycopg2.extras import RealDictCursor


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


def get_fixed_slots(channel_id: int, dow: int):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT * FROM syndicated_fixed_slots WHERE dow = %s
                        AND channel_id = %s;""", (dow, channel_id))
            rows = cur.fetchall()
        return rows

    except:
        raise
    finally:
        conn.close()


def get_fill_rules(channel_id: int):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM fill_rules WHERE channel_id = %s;", (channel_id,))
            rows = cur.fetchall()
        return rows

    except:
        raise
    finally:
        conn.close()


def get_movies(ratings: list[str] = ['G', 'PG']):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                   SELECT 
                       m.movie_id AS episode_id,
                       DATE(movie_release_date ||'-01-01') AS episode_airdate,
                       CEIL((m.end_point - m.start_point) / 1800) * 1800 AS episode_duration,
                       'movies' AS media_table
                   FROM movies m WHERE movie_rating = ANY(ARRAY[%s]);""", (ratings,))
            media = cur.fetchall()

    except Exception as e:
        raise
    else:
        return media
    finally:
        conn.close()


def get_specials():
    x = 1


def get_shows(include_childrens: bool = False, season: str = 'any'):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = f"""
                SELECT 
                    s.show_id,
                    s.show_name AS title,
                    s.show_genre AS genres,
                    s.show_duration AS durations,
                    EXTRACT(YEAR FROM airdate_start)::INT AS start_year,
                    EXTRACT(YEAR FROM airdate_end)::INT AS end_year,
                    s.show_network AS networks,
                    1 AS popularity
                FROM shows s"""

            # add filter if include_childrens is False
            if not include_childrens:
                query += " WHERE s.show_type != 'children'"

            cur.execute(query)
            shows = cur.fetchall()

            if not shows:
                return {}

            # map them by show_id
            show_map = {s["show_id"]: dict(s, episodes=[]) for s in shows}

            # now pull episodes for all those shows
            query = """
                SELECT 
                    e.show_id,
                    e.episode_id,
                    e.episode_airdate,
                    CEIL((e.end_point - e.start_point) / 1800) * 1800 AS episode_duration,
                    'shows' AS media_table
                FROM episodes e"""

            cur.execute(query)
            episodes = cur.fetchall()

            # attach them
            for ep in episodes:
                sid = ep["show_id"]
                if sid in show_map:
                    show_map[sid]["episodes"].append(ep)

    except Exception as e:
        raise
    else:
        return show_map
    finally:
        conn.close


def insert_seen_episode(channel_id: int, episode_id: str):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("INSERT INTO seen_episodes (channel_id, episode_id) VALUES (%s, %s)", (channel_id, episode_id))

    except Exception as e:
        raise
    finally:
        conn.commit()
        conn.close()


def get_seen_episodes() -> dict[int, set[int]]:
    conn = get_db_connection()
    seen = {}
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT channel_id, episode_id FROM seen_episodes;")
            rows = cur.fetchall()

            for row in rows:
                cid = row["channel_id"]
                eid = row["episode_id"]
                if cid not in seen:
                    seen[cid] = set()
                seen[cid].add(eid)

    except Exception as e:
        logging.error(f"Error fetching seen episodes: {e}")
        raise
    else:
        return seen
    finally:
        conn.close()


def get_channels(channels: list):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM channels WHERE channel_id = ANY(%s);", (channels,))
            rows = cur.fetchall()
        return rows

    except:
        raise
    finally:
        conn.close()


def in_time_range(start_time: time, end_time: time, slot_dt, base_date=None):
    """
    start_time, end_time: datetime.time
    slot_dt: datetime OR datetime.time
    base_date: the reference date (defaults to today if slot_dt is a time)
    """
    if isinstance(slot_dt, datetime):
        base_date = slot_dt.date()
        slot_time = slot_dt.time()
    elif isinstance(slot_dt, time):
        if base_date is None:
            base_date = date.today()
        slot_time = slot_dt
    else:
        raise TypeError(f"slot_dt must be datetime or time, got {type(slot_dt)}")

    dt_start = datetime.combine(base_date, start_time)
    dt_end = datetime.combine(base_date, end_time)
    slot_dt = datetime.combine(base_date, slot_time)

    # If the end time is "earlier" than start, it means it crosses midnight → add a day
    if dt_end <= dt_start:
        dt_end += timedelta(days=1)
        if slot_dt < dt_start:
            slot_dt += timedelta(days=1)

    return dt_start <= slot_dt < dt_end


def find_rule_for_slot(rules, slot_time: time):
    """
    Given a list of fill rules and a slot_time (datetime.time),
    return the rule whose (start_time, end_time) range covers slot_time.
    Supports ranges that cross midnight.
    """
    for rule in rules:
        if in_time_range(rule["start_time"], rule["end_time"], slot_time):
            return rule

    return None


def get_category_shows(category: str, year: int, shows: dict) -> list:
    """Return a list of shows active in `year` that match the category.
        Parameters:
        category: str, category/genre bucket (e.g. 'comedy', 'news')
        year: int, the schedule year
        shows: dict keyed by show_id, with each value containing:
            {
                'show_id': int,
                'title': str,
                'genres': [str],
                'durations': [int],
                'start_year': int,
                'end_year': int,
                'networks': [str],
                'popularity': float
            }"""
    candidates = []
    category_lower = category.lower()

    for s in shows.values():
        # check active in this year
        if not (s["end_year"] <= year or s['show_id'] in [173, 275]):
            continue

        # match genres
        categories = [c.strip().lower() for c in category_lower.split(",")]
        if any(g.lower() in categories for g in s["genres"].split(',')):
            candidates.append(s)

    return candidates


def score_show_candidate(show: dict, slot: dict, year: int) -> float:
    """
    Compute a score for how well a show fits a given slot.

    Parameters:
        show: dict with fields like title, genres, durations, start_year, end_year, popularity
        slot: dict with fields like day_of_week, start_time, category
        year: int, current scheduling year

    Returns:
        float score (higher = better fit)
    """

    score = 0.0

    # 1. Popularity baseline
    score += show.get("popularity", 0.5) * 10

    # 2. Recency bonus: prefer shows active close to the target year
    midpoint = (show["start_year"] + show["end_year"]) / 2
    recency_factor = max(0, 1 - (abs(year - midpoint) / 30))  # fade after ~30 years
    score += recency_factor * 5

    # 3. Category match boost
    if "category" in slot:
        if any(slot["category"].lower() in g.lower() for g in show.get("genres", '').split(',')):
            score += 5
        else:
            score -= 2  # small penalty if genre doesn’t align

    # 4. Duration sanity check
    slot_duration = slot.get("duration", 1800)  # assume 30 min if not set
    if slot_duration in show.get("durations", []):
        score += 3
    else:
        score -= 1

    # 5. Prime-time boost
    start_time_str = str(slot["start_time"])
    start_hour = int(start_time_str.split(":")[0])
    if 19 <= start_hour < 22:  # 7–10pm
        if "comedy" in show.get("genres", '').split(',') or "drama" in show.get("genres", '').split(','):
            score += 2

    return score


def choose_movie_for_show(movies: list[dict], year: int, duration: int,
                          seen_episodes: set[int] | None = None) -> dict | None:
    """
    Pick one episode from a show's episode list to air in this slot.

    Args:
        show: dict containing show_id and maybe a list of episodes.
              Example: {"show_id": 123, "title": "...", "episodes": [ { "episode_id": ..., "airdate": ...}, ...]}
        year: target year for the schedule (for historical filtering)
        seen_episodes: set of episode_ids that have already been scheduled (optional)

    Returns:
        dict with chosen episode, or None if no valid episodes found.
        :param seen_episodes:
        :param duration:
        :param year:
        :param movies:
    """
    if not movies:
        return None

    # Only consider episodes that aired before the simulated date
    slot_date = date(year, 1, 1)  # crude fallback (could be exact schedule date)
    valid_eps = [m for m in movies if
                 m.get("episode_airdate") and m["episode_airdate"] <= slot_date and m['episode_duration'] == duration]

    if not valid_eps:
        valid_eps = movies  # fallback: if nothing fits, allow all

    # Avoid repeats if a set of seen episodes is passed in
    if seen_episodes:
        valid_eps = [m for m in valid_eps if f"movies_{m['episode_id']}" not in seen_episodes]

    if not valid_eps:
        return None  # no unseen episodes left

    # Strategy: pick the one closest to slot date, or just random
    chosen = min(
        valid_eps,
        key=lambda e: abs((slot_date - e["episode_airdate"]).days) if e.get("episode_airdate") else 999999
    )

    # Track it if we're avoiding repeats
    if seen_episodes is not None:
        seen_episodes.add(f'{chosen["media_table"]}_{chosen["episode_id"]}')
    return chosen


def get_random_slots(current, current_end, slot_minutes=30, slot_variation=None):
    random_slots = []
    while current < current_end:
        if slot_variation:
            dur = random.choices(
                slot_variation,
                weights=[1.0 / (i + 1) for i in range(len(slot_variation))],  # bias small durations
                k=1
            )[0]
        else:
            dur = slot_minutes

        dur_seconds = dur * 60
        if current + timedelta(seconds=dur_seconds) > current_end:
            dur_seconds = int((current_end - current).total_seconds())

        random_slots.append((current, dur_seconds))
        current += timedelta(seconds=dur_seconds)
    return random_slots


def build_time_slots(start_dt, end_dt, slot_minutes=30, slot_variation=None, fixed_slots=None):
    """
    Build sequential slots from start_dt to end_dt.
    - fixed_slots: dict {channel_id: [datetime.time,...]} or just a list of times
    """
    fixed_show_slots = []
    slots = []
    cur = start_dt

    # Normalize fixed slots into a dict of {time -> duration}
    if fixed_slots:
        for slot in fixed_slots:
            fixed_start = datetime.combine(date.today(), slot['start_time'])
            fixed_end = datetime.combine(date.today(), slot['end_time'])
            if fixed_end <= fixed_start:
                fixed_end += timedelta(days=1)

            delta = fixed_end - fixed_start
            fixed_show_slots.append((fixed_start, delta.seconds))

        fixed_show_slots = sorted(fixed_show_slots, key=lambda x: x[0])

        for i, slot in enumerate(fixed_show_slots):
            slots += get_random_slots(cur, slot[0], slot_minutes=slot_minutes, slot_variation=slot_variation)
            slots.append(fixed_show_slots[i])
            cur += timedelta(seconds=((slot[0] - cur).seconds + fixed_show_slots[i][1]))

            if i == len(fixed_show_slots) - 1:
                slots += get_random_slots(cur, end_dt, slot_minutes=slot_minutes, slot_variation=slot_variation)

    else:
        slots = get_random_slots(start_dt, end_dt, slot_minutes=slot_minutes, slot_variation=slot_variation)

    sorted_slots = sorted(slots, key=lambda x: x[0])
    return sorted_slots


def pick_duration(show, slot_seconds):
    """
    Return an exact match for duration == slot_seconds.
    If none exists, return None to signal caller to handle splitting.
    """
    durations = show.get("durations", [])
    if slot_seconds in durations:
        return slot_seconds
    return None


def normalize_start_end(start_time: time, end_time: time):
    """
    Normalize start and end into datetimes, pushing end into the next day
    if it is logically after midnight.
    """
    today = datetime.today().date()
    dt_start = datetime.combine(today, start_time)
    dt_end = datetime.combine(today, end_time)

    # guard: if end is before or equal to start, assume rollover
    if dt_end <= dt_start:
        dt_end += timedelta(days=1)

    return dt_start, dt_end


def get_fixed_episode(show: dict, seen: list):
    episodes = show.get('episodes', [])
    unseen = [ep for ep in episodes if ep["episode_id"] not in seen]
    if unseen:
        return random.choice(unseen)
    else:
        # fallback to any (allow repeat if all seen)
        return random.choice(episodes)


def get_candidate(pool, movies, slot_dict, target_date, slot_seconds, seen_episodes):
    weights = [score_show_candidate(s, slot_dict, target_date.year) for s in pool]
    random_pool = random.choices(pool, weights=weights, k=len(pool))
    for idx, candidate in enumerate(random_pool):
        if not candidate['episodes']:  ## movie
            movie_choice = choose_movie_for_show(movies, target_date.year, slot_seconds, seen_episodes)
            return candidate, movie_choice

        unseen_episodes = [s for s in candidate['episodes'] if f'shows_{s["episode_id"]}' not in seen_episodes]

        if not unseen_episodes:  ## No pool to draw from, move along
            continue

        return candidate, random.choice(unseen_episodes)

    return None, None


def update_memory_sets(record, seen_shows, sid, ep_key, seen_episodes_ids, schedules) -> None:
    seen_shows.append(record["show_id"])
    seen_episodes_ids.add(sid)
    insert_seen_episode(sid, ep_key)
    schedules[sid].append(record)


def filter_candidates(candidates, slot_time, slot_seconds, target_date, rule, seen_shows):
    filtered = [(s, pick_duration(s, slot_seconds)) for s in candidates if pick_duration(s, slot_seconds)]
    unseen_shows = [s for s in filtered if s[0]["show_id"] not in seen_shows]
    pool = [s for (s, d) in unseen_shows]
    slot_dict = {"day_of_week": target_date.weekday(), "start_time": slot_time.time(),
                 "category": rule["category"],
                 "duration": slot_seconds}
    return pool, slot_dict


def split_slot(candidates, movies, s_slot, target_date, rule, seen_shows, seen_episodes):
    unseen_shows, slot_dict = filter_candidates(candidates, s_slot[0], s_slot[1], target_date, rule, seen_shows)
    show, episode = get_candidate(unseen_shows, movies, slot_dict, target_date, s_slot[0], seen_episodes)
    ep_key = f"{episode['media_table']}_{episode['episode_id']}"

    record = {
        "start_time": s_slot[0].time(),
        "show_id": show["show_id"],
        "episode_id": ep_key,
        "title": show["title"],
        "duration": episode['episode_duration']
    }

    yield ep_key, record


def schedule_for_channels(
    channels: list[dict[str, Any]],
    target_date: datetime,
    start_time: time,
    end_time: time,
    slot_minutes: int = 30,
    seed: int | None = None,
    slot_variation: list[int] | None = None
) -> dict[int, list[dict[str, Any]]]:
    """
    Build TV schedules for multiple channels for a given day.

    Args:
        channels (list[dict[str, Any]]): A list of channel dicts, each containing channel metadata (e.g., channel_id).
        target_date (datetime): The target date for scheduling.
        start_time (datetime): The starting time for scheduling.
        end_time (datetime): The ending time for scheduling.
        slot_minutes (int, optional): Default slot size in minutes. Defaults to 30.
        seed (Optional[int], optional): Random seed for reproducibility. Defaults to None.
        slot_variation (Optional[list[int]], optional): Possible slot durations in minutes. Defaults to None.

    Returns:
        dict[int, list[dict[str, Any]]]: A dictionary mapping channel_id → list of scheduled program dicts.

    Raises:
        ValueError: If a slot cannot be filled with a valid show or episode.
    """

    # Seed random generator if provided
    if seed is not None:
        random.seed(seed)

    try:
        shows = get_shows()
        movies = get_movies(['G', 'PG', 'R'])
        seen_episodes = get_seen_episodes()
        seen_episodes_ids = set().union(*seen_episodes.values())
    except Exception as e:
        raise RuntimeError(f"Failed to fetch base data: {e}")

    schedules: dict[int, list[dict[str, Any]]] = {}

    # Normalize start and end times
    try:
        start_dt, end_dt = normalize_start_end(start_time, end_time)
    except Exception as e:
        raise ValueError(f"Invalid start/end times: {e}")

    # Process each channel
    for ch in channels:
        seen_shows: list[int] = []
        sid = ch["channel_id"]
        schedules[sid] = []

        try:
            # Fetch channel-specific settings
            fixed = get_fixed_slots(sid, target_date.weekday())
            fixed_map = {f["start_time"]: f for f in fixed}
            rules = get_fill_rules(sid)
        except Exception as e:
            raise RuntimeError(f"Failed to load rules for channel {sid}: {e}")

        # Generate time slots for this channel
        try:
            slots = build_time_slots(
                start_dt, end_dt,
                slot_minutes=slot_minutes,
                slot_variation=slot_variation,
                fixed_slots=fixed
            )
        except Exception as e:
            raise RuntimeError(f"Error building slots for channel {sid}: {e}")

        # Iterate over slots
        for slot_time, slot_seconds in slots:
            show = episode = record = ep_key = None

            # Case 1: Fixed slot content
            if slot_time.time() in fixed_map.keys():
                try:
                    entry = fixed_map[slot_time.time()].copy()
                    show = shows.get(entry['show_id'])
                    entry['episode_id'] = get_fixed_episode(
                        show, list(seen_episodes.get(sid, set()))
                    )['episode_id']

                    record = {
                        "start_time": slot_time.time(),
                        "show_id": entry["show_id"],
                        "title": show['title'],
                        "episode_id": f"shows_{entry['episode_id']}",
                        "duration": slot_seconds
                    }

                    update_memory_sets(record, seen_shows, sid,
                                       f"shows_{entry['episode_id']}",
                                       seen_episodes_ids, schedules)
                    continue
                except Exception as e:
                    raise RuntimeError(f"Failed to process fixed slot at {slot_time} for channel {sid}: {e}")

            # Case 2: Rule-based content
            try:
                rule = find_rule_for_slot(rules, slot_time)
                rule = rule if rule else {"category": 'news,drama,comedy,movie,special,documentary'}

                # Candidate shows for this slot
                candidates = get_category_shows(rule["category"], target_date.year, shows)
                unseen_shows, slot_dict = filter_candidates(
                    candidates, slot_time, slot_seconds, target_date, rule, seen_shows
                )

                if unseen_shows:
                    show, episode = get_candidate(
                        unseen_shows, movies, slot_dict,
                        target_date, slot_seconds, seen_episodes_ids
                    )

                # Fallback: split into smaller slots if nothing fits
                if not unseen_shows or show is None or episode is None:
                    if slot_seconds > 1800:
                        slots_end = slot_time + timedelta(seconds=slot_seconds)
                        split_slots = get_random_slots(slot_time, slots_end,
                                                       slot_minutes=30,
                                                       slot_variation=[30, 60])
                        for s_slot in split_slots:
                            for ep_key, record in split_slot(
                                candidates, movies, s_slot, target_date,
                                rule, seen_shows, seen_episodes
                            ):
                                if not record:
                                    raise ValueError(f"No record generated for split slot {s_slot}")
                                update_memory_sets(record, seen_shows, sid,
                                                   ep_key, seen_episodes_ids,
                                                   schedules)
                        continue
            except Exception as e:
                raise RuntimeError(f"Failed to process slot {slot_time} for channel {sid}: {e}")

            # Case 3: Regular assignment
            try:
                ep_key = f"{episode['media_table']}_{episode['episode_id']}"
                record = {
                    "start_time": slot_time.time(),
                    "show_id": show["show_id"],
                    "episode_id": ep_key,
                    "title": show["title"],
                    "duration": episode['episode_duration']
                }
                update_memory_sets(record, seen_shows, sid,
                                   ep_key, seen_episodes_ids, schedules)
            except Exception as e:
                raise RuntimeError(f"Failed to finalize slot at {slot_time} for channel {sid}: {e}")

    return schedules


channels = get_channels([3, 5, 6, 7])
dt = datetime(1970, 3, 18)
start = time(18, 0)
end = time(1, 0)
pprint(schedule_for_channels(channels, dt, start, end, slot_variation=[30, 60, 90, 120]))
