#!/usr/bin/env python3
"""
Era-aware syndicated daytime scheduling, Monday-Friday plus the Saturday/Sunday additive patch.

See CLAUDE_SYNDICATED_SCHEDULING_HANDOFF.md and CLAUDE_WEEKEND_SCHEDULING_HANDOFF.md /
CLAUDE_WEEKEND_GENRE_FIX.md (Desktop) for the original designs, and
setup_syndicated_scheduling.py / setup_weekend_scheduling.py (repo root) for the one-time
schema/data setup this depends on (`fill_rules`/`syndicated_fixed_slots` year_start/year_end
columns, `show_syndication_eligibility` table and its weekday_*/saturday_*/sunday_* daypart
columns -- one shared `mapped_genre` per show across every day, per the genre-fix doc).

Only touches the syndicated channel path -- create_playlist.py's network branch is untouched.
"""
import random
from datetime import datetime, time, timedelta

from psycopg.rows import dict_row

SCHEDULE_RECURSION = 3  # Matches Shows.SCHEDULE_RECURSION -- kept independent to avoid coupling.

# Weekday daypart blocks, matching syndicated_mapping.csv's slot_profile bands exactly. A slot's
# start_time is bucketed into one of these by hour to know which fill_rules rows and which
# show_syndication_eligibility weekday_* column apply.
WEEKDAY_DAYPART_BLOCKS = [
    (6, 8, "weekday_early_morning"),
    (8, 10, "weekday_morning"),
    (10, 12, "weekday_late_morning"),
    (12, 14, "weekday_noon"),
    (14, 16, "weekday_early_afternoon"),
    (16, 18, "weekday_after_school"),
    (18, 19, "weekday_early_evening"),
]

# Saturday/Sunday daypart blocks, matching syndicated_mapping_weekend_FIXED.csv's 5 weekend
# blocks (narrower/coarser than the 7 weekday blocks -- e.g. one 09:00-12:00 "morning" block
# instead of splitting it further, matching real Saturday-morning cartoon-block scheduling).
WEEKEND_DAYPART_BLOCKS = [
    (7, 9, "early_morning"),
    (9, 12, "morning"),
    (12, 14, "midday"),
    (14, 17, "afternoon"),
    (17, 19, "early_evening"),
]

# Late night: one block for every day of the week, 23:00 through 05:59 (hours 23 and 0-5), backed by a
# single show_syndication_eligibility.late_night column. The hours between the daytime scope and
# 23:00 (primetime) and a weekend's 06:00 hour stay outside this system (daypart_column_for -> None).
LATE_NIGHT_COLUMN = "late_night"
LATE_NIGHT_START_HOUR = 23
LATE_NIGHT_END_HOUR = 6

SATURDAY_DOW = 5
SUNDAY_DOW = 6


def daypart_column_for(start_time: time, dow: int = None):
    """Return the show_syndication_eligibility column name for a slot start time/day, or None if
    the time falls outside this system's scope for that day: daytime (weekday 06:00-19:00;
    weekend 07:00-19:00) plus late night (23:00-06:00, any day). Primetime (19:00-23:00) is not
    covered on any day."""
    hour = start_time.hour
    if dow == SATURDAY_DOW:
        blocks, prefix = WEEKEND_DAYPART_BLOCKS, "saturday_"
    elif dow == SUNDAY_DOW:
        blocks, prefix = WEEKEND_DAYPART_BLOCKS, "sunday_"
    else:
        blocks, prefix = WEEKDAY_DAYPART_BLOCKS, ""

    for start, end, column in blocks:
        if start <= hour < end:
            return column if prefix == "" else f"{prefix}{column}"
    if hour >= LATE_NIGHT_START_HOUR or hour < LATE_NIGHT_END_HOUR:
        return LATE_NIGHT_COLUMN
    return None


# A slot is never shorter than this. A show whose longest allowed length is under 30 minutes (only
# the 5-minute Batfink shorts today) can't fill a slot on its own, so it is left out of the daytime
# fill until there is real packing logic -- a lone 5-minute short in a 30-minute slot would be wrong.
MIN_SLOT_SECONDS = 1800
# Episodes run slightly under/over their slot, so a show fits an option if the option is within 5%
# of (or above) the show's typical episode length.
EPISODE_FIT_TOLERANCE = 1.05
# SQL fragment: the show's slot length -- its smallest show_duration option that is at least 30
# minutes and holds its typical episode -- fits in the %s seconds that remain.
FITS_WITHIN_SQL = """
              AND EXISTS (
                  SELECT 1 FROM shows sh
                  WHERE sh.show_id = e.show_id
                    AND (
                        SELECT MIN(d) FROM unnest(sh.show_duration) d
                        WHERE d >= 1800
                          AND d * 1.05 >= COALESCE(
                              (SELECT AVG(ep.end_point - ep.start_point) FROM episodes ep WHERE ep.show_id = sh.show_id), 0)
                    ) <= %s
              )
"""

# Filler ladder (see resolve_filler): substitutions tried when a slot's own categories have no
# unused eligible show. A starting point to tune, not a historical claim.
FILLER_RELATED_CATEGORIES = {
    "kids": ["family"],
    "family": ["kids", "drama"],
    "adventure": ["western", "drama"],
    "western": ["adventure", "drama"],
    "drama": ["family", "adventure"],
    "variety": ["talk", "family"],
    "talk": ["variety", "family"],
    "game": ["talk", "variety"],
    "movie": ["drama", "adventure"],
    "other": ["family", "talk"],
    "local": ["family", "talk"],
    "religious": ["talk", "other"],
}
# Each step further down a slot's category list costs this many extra placements when choosing
# between candidate reruns, so one thin category can't hog a channel.
REPEAT_CATEGORY_PENALTY = 2


def adjacent_daypart_columns(start_time: time, dow: int = None):
    """Eligibility columns for the dayparts immediately before/after this slot's own block."""
    if dow == SATURDAY_DOW:
        names, prefix = [b[2] for b in WEEKEND_DAYPART_BLOCKS], "saturday_"
    elif dow == SUNDAY_DOW:
        names, prefix = [b[2] for b in WEEKEND_DAYPART_BLOCKS], "sunday_"
    else:
        names, prefix = [b[2] for b in WEEKDAY_DAYPART_BLOCKS], ""
    here = daypart_column_for(start_time, dow)
    names = [f"{prefix}{n}" for n in names]
    if here not in names:   # outside the daytime blocks (None) or late night, which has no neighbours
        return []
    i = names.index(here)
    return [names[j] for j in (i - 1, i + 1) if 0 <= j < len(names)]


class SyndicatedScheduler:
    def __init__(self, db):
        self.db_connection = db
        self.cur = db.cursor(row_factory=dict_row)

    def get_fixed_slot(self, channel_id, year, dow, start_time):
        """Return the show_id for a syndicated_fixed_slots row matching this channel/day/time,
        preferring a year-specific row over a generic (year_start/year_end NULL) one. None if
        no fixed slot exists for this time (the normal case -- most slots are fill-rule driven)."""
        self.cur.execute(
            """
            SELECT show_id, year_start, year_end
            FROM syndicated_fixed_slots
            WHERE channel_id = %s
              AND dow = %s
              AND start_time <= %s
              AND end_time > %s
              AND (
                  (year_start IS NULL AND year_end IS NULL)
                  OR (%s BETWEEN year_start AND year_end)
              )
            ORDER BY
                CASE WHEN year_start IS NOT NULL THEN 0 ELSE 1 END
            LIMIT 1
            """,
            (channel_id, dow, start_time, start_time, year),
        )
        row = self.cur.fetchone()
        return row["show_id"] if row else None

    def get_fill_rules(self, channel_id, year, dow, start_time):
        """Return the matching fill_rules rows for this slot, preferring year-specific rows over
        generic ones (never mixing the two sets -- if any year-specific rule matches, only
        those are used)."""
        self.cur.execute(
            """
            SELECT rule_id, category, weight, year_start, year_end
            FROM fill_rules
            WHERE channel_id = %s
              AND %s = ANY(dows)
              AND start_time <= %s
              AND end_time > %s
              AND year_start IS NOT NULL
              AND year_end IS NOT NULL
              AND %s BETWEEN year_start AND year_end
            """,
            (channel_id, dow, start_time, start_time, year),
        )
        year_specific = self.cur.fetchall()
        if year_specific:
            return year_specific

        self.cur.execute(
            """
            SELECT rule_id, category, weight, year_start, year_end
            FROM fill_rules
            WHERE channel_id = %s
              AND %s = ANY(dows)
              AND start_time <= %s
              AND end_time > %s
              AND year_start IS NULL
              AND year_end IS NULL
            """,
            (channel_id, dow, start_time, start_time),
        )
        generic = self.cur.fetchall()

        # Legacy generic rows predate the one-category-per-row convention (e.g.
        # 'children,game,movie,talk,comedy' with a single weight=1.0) -- split into
        # equal-weight categories so choose_category() never has to special-case this format.
        expanded = []
        for row in generic:
            categories = row["category"].split(",")
            if len(categories) == 1:
                expanded.append(row)
                continue
            for category in categories:
                expanded.append({**row, "category": category.strip(), "weight": row["weight"] / len(categories)})
        return expanded

    def choose_category(self, rules):
        """Weighted-random pick of one category from the given fill_rules rows."""
        if not rules:
            return None
        categories = [r["category"] for r in rules]
        weights = [r["weight"] for r in rules]
        return random.choices(categories, weights=weights, k=1)[0]

    def get_eligible_shows(self, category, year, daypart_column, channel_id, replication_year, exclude_show_ids=None,
                           max_seconds=None):
        """Shows matching `category`/`daypart_column`, eligible for `year`, syndicated
        (shows.show_is_syndicated -- a show that was never syndicated can't air on a syndicated
        channel), and not already
        licensed to another channel for this replication_year (mirrors the exclusivity rule in
        Shows.get_available_show_id -- a syndicated show airs on one station in a market, not
        several at once)."""
        if daypart_column is None:
            return []
        exclude_show_ids = exclude_show_ids or set()

        query = f"""
            SELECT e.show_id
            FROM show_syndication_eligibility e
            WHERE e.mapped_genre = %s
              AND e.{daypart_column} = TRUE
              AND EXISTS (SELECT 1 FROM shows syn WHERE syn.show_id = e.show_id AND syn.show_is_syndicated)
              AND e.eligible_year_start IS NOT NULL
              AND e.eligible_year_end IS NOT NULL
              AND %s BETWEEN e.eligible_year_start AND e.eligible_year_end
              AND e.show_id NOT IN (
                  SELECT show_id FROM schedule_template WHERE replication_year = %s
              )
        """
        params = [category, year, replication_year]
        if exclude_show_ids:
            query += " AND NOT (e.show_id = ANY(%s))"
            params.append(list(exclude_show_ids))
        if max_seconds is not None:
            query += FITS_WITHIN_SQL
            params.append(max_seconds)

        self.cur.execute(query, params)
        return [row["show_id"] for row in self.cur.fetchall()]

    def choose_show(self, show_ids):
        return random.choice(show_ids) if show_ids else None

    def resolve_syndicated_slot(self, channel_id, year, dow, start_time, replication_year, exclude_show_ids=None,
                                prev_show_id=None, use_filler=True, max_seconds=None):
        """
        Full resolution for one syndicated daytime slot:
          1. A fixed slot always wins if one exists for this channel/day/time.
          2. Otherwise, weighted-pick a category from matching fill_rules, dropping any category
             that turns up zero eligible shows and re-picking from what's left (e.g. this repo
             has no 'religious' show in its library, so that category is simply skipped rather
             than failing the slot).
          3. If every candidate category comes up empty (a fully starved slot -- no fill_rules
             match, or none of their categories have any eligible show), runs the filler ladder
             (resolve_filler) so the slot is filled accurately instead of leaving it to the
             coarser genre-bias engine. Pass use_filler=False to get the old behavior (None).

        max_seconds limits the pick to shows that can fit in that much time (see FITS_WITHIN_SQL).

        prev_show_id is the show resolved for the immediately preceding slot on this channel/day,
        so the filler can avoid airing the same show back-to-back.

        Returns a show_id, or None if nothing could be resolved here (the caller should then fall
        back to Shows.get_genre_bias_for_slot / get_available_show_id).
        """
        fixed_show_id = self.get_fixed_slot(channel_id, year, dow, start_time)
        if fixed_show_id is not None:
            return fixed_show_id

        daypart_column = daypart_column_for(start_time, dow)
        rules = self.get_fill_rules(channel_id, year, dow, start_time)
        remaining = list(rules)

        while remaining:
            category = self.choose_category(remaining)
            show_ids = self.get_eligible_shows(
                category, year, daypart_column, channel_id, replication_year, exclude_show_ids, max_seconds
            )
            if show_ids:
                return self.choose_show(show_ids)
            remaining = [r for r in remaining if r["category"] != category]

        if not use_filler:
            return None
        return self.resolve_filler(
            channel_id, year, dow, start_time, replication_year, rules, exclude_show_ids, prev_show_id,
            max_seconds,
        )

    # ------------------------------------------------------------------ filler ladder

    def get_era_eligible_shows(self, category, year, daypart_column, max_seconds=None):
        """Like get_eligible_shows but WITHOUT the one-licence-per-replication-year exclusion --
        used by the rerun tiers, which deliberately reuse shows."""
        if daypart_column is None:
            return []
        self.cur.execute(
            f"""
            SELECT e.show_id FROM show_syndication_eligibility e
            WHERE e.mapped_genre = %s AND e.{daypart_column} = TRUE
              AND EXISTS (SELECT 1 FROM shows syn WHERE syn.show_id = e.show_id AND syn.show_is_syndicated)
              AND e.eligible_year_start IS NOT NULL AND e.eligible_year_end IS NOT NULL
              AND %s BETWEEN e.eligible_year_start AND e.eligible_year_end
            {FITS_WITHIN_SQL if max_seconds is not None else ""}
            ORDER BY e.show_id
            """,
            (category, year) + ((max_seconds,) if max_seconds is not None else ()),
        )
        return [row["show_id"] for row in self.cur.fetchall()]

    def get_show_placement_counts(self, replication_year, channel_id=None):
        """{show_id: number of schedule_template slots} for the replication year, optionally for
        one channel (a weekday strip is one row, so it counts once)."""
        query = "SELECT show_id, COUNT(*) AS n FROM schedule_template WHERE replication_year = %s AND show_id IS NOT NULL"
        params = [replication_year]
        if channel_id is not None:
            query += " AND channel_id = %s"
            params.append(channel_id)
        self.cur.execute(query + " GROUP BY show_id", params)
        return {row["show_id"]: row["n"] for row in self.cur.fetchall()}

    def resolve_filler(self, channel_id, year, dow, start_time, replication_year, rules,
                       exclude_show_ids=None, prev_show_id=None, max_seconds=None):
        """Accuracy-first filler for a slot whose own categories have no unused eligible show.
        Tried in order, each restricted to shows genuinely eligible for `year`:
          B. same category, adjacent daypart's flag, unused show
          C. related category (FILLER_RELATED_CATEGORIES), same daypart, unused show
          D1. rerun on THIS channel -- a show already templated here (or earlier in this run),
              fewest placements first, never the previous slot's show: stations rerun their own
              shows, and this keeps one-station-per-market licensing intact
          D2. rerun a show from another channel (breaks the exclusivity), fewest placements first,
              still never back-to-back
          D3. last resort: allow back-to-back
        Returns None only if no in-era show matches any category (the caller's genre-bias engine
        then takes over). Repeats are balanced across categories by REPEAT_CATEGORY_PENALTY.
        """
        column = daypart_column_for(start_time, dow)
        if column is None:
            return None
        exclude_show_ids = set(exclude_show_ids or ())
        categories = sorted(
            {r["category"] for r in rules},
            key=lambda c: (-sum(r["weight"] for r in rules if r["category"] == c), c),
        )

        for category in categories:  # B
            for adjacent in adjacent_daypart_columns(start_time, dow):
                show_ids = self.get_eligible_shows(
                    category, year, adjacent, channel_id, replication_year, exclude_show_ids, max_seconds
                )
                if show_ids:
                    return self.choose_show(show_ids)

        for category in categories:  # C
            for related in FILLER_RELATED_CATEGORIES.get(category, []):
                show_ids = self.get_eligible_shows(
                    related, year, column, channel_id, replication_year, exclude_show_ids, max_seconds
                )
                if show_ids:
                    return self.choose_show(show_ids)

        ordered = []
        for category in categories:
            for c in [category] + FILLER_RELATED_CATEGORIES.get(category, []):
                if c not in ordered:
                    ordered.append(c)
        pools = [(pos, self.get_era_eligible_shows(c, year, column, max_seconds)) for pos, c in enumerate(ordered)]

        channel_counts = self.get_show_placement_counts(replication_year, channel_id)
        for show_id in exclude_show_ids:  # placed earlier in this run, not yet in schedule_template
            channel_counts[show_id] = channel_counts.get(show_id, 0) + 1
        global_counts = self.get_show_placement_counts(replication_year)

        def pick(allowed, count_of):
            scored = [
                (count_of(show_id) + REPEAT_CATEGORY_PENALTY * pos, show_id)
                for pos, show_ids in pools
                for show_id in show_ids
                if allowed(show_id)
            ]
            if not scored:
                return None
            best = min(score for score, _ in scored)
            return random.choice(sorted({show_id for score, show_id in scored if score == best}))

        return (
            pick(lambda s: s in channel_counts and s != prev_show_id, lambda s: channel_counts[s])
            or pick(lambda s: s != prev_show_id, lambda s: global_counts.get(s, 0))
            or pick(lambda s: True, lambda s: global_counts.get(s, 0))
        )

    # ------------------------------------------------------- duration-aware daytime grid

    def get_show_slot_options(self, show_id):
        """The slot lengths (seconds, ascending) a show can fill: its show_duration options of at
        least MIN_SLOT_SECONDS."""
        self.cur.execute("SELECT show_duration FROM shows WHERE show_id = %s", (show_id,))
        row = self.cur.fetchone()
        return sorted({int(d) for d in ((row or {}).get("show_duration") or []) if d >= MIN_SLOT_SECONDS})

    def get_typical_episode_seconds(self, show_id):
        """Average real episode length (end_point - start_point), or 0 if the show has no episodes."""
        self.cur.execute(
            "SELECT AVG(end_point - start_point) AS avg_seconds FROM episodes WHERE show_id = %s", (show_id,)
        )
        row = self.cur.fetchone()
        return float(row["avg_seconds"]) if row and row["avg_seconds"] is not None else 0.0

    def choose_slot_seconds(self, show_id, remaining_seconds):
        """Smallest slot length that holds this show's typical episode and still fits in
        `remaining_seconds` (Wagon Train is [3600, 5400] with ~50-minute episodes -> 3600; McCloud
        is [3600, 5400, 7200] with ~83-minute episodes -> 5400), or None if nothing fits."""
        typical = self.get_typical_episode_seconds(show_id)
        holding = [o for o in self.get_show_slot_options(show_id) if o * EPISODE_FIT_TOLERANCE >= typical]
        fitting = [o for o in holding if o <= remaining_seconds]
        return fitting[0] if fitting else None

    def build_daytime_grid(self, channel_id, year, dow, start_time, end_time, replication_year):
        """Build one day-part's slots AROUND the shows' real lengths instead of fixing a random
        grid first: resolve a show for the current time (fill rules, then the filler ladder),
        size the slot from that show's own duration, advance, repeat until the window is full.
        Pure -- reads only, never writes. Returns [{"start_time": "HH:MM:SS", "seconds": int,
        "show_id": int | None}]; show_id None marks a 30-minute placeholder for a slot nothing in
        the library could fill (the caller falls back to the genre-bias engine for it)."""
        start = start_time.hour * 3600 + start_time.minute * 60 + start_time.second
        end = end_time if isinstance(end_time, int) else end_time.hour * 3600 + end_time.minute * 60 + end_time.second
        cursor, used, prev_show_id, grid = start, set(), None, []
        while end - cursor >= MIN_SLOT_SECONDS:
            slot_start = time(cursor // 3600, (cursor % 3600) // 60, cursor % 60)
            remaining = end - cursor
            show_id = self.resolve_syndicated_slot(
                channel_id, year, dow, slot_start, replication_year, used,
                prev_show_id=prev_show_id, max_seconds=remaining,
            )
            if show_id is None:
                seconds = MIN_SLOT_SECONDS
            else:
                # A fixed slot isn't filtered by max_seconds; truncate in the (rare) case it can't fit.
                seconds = self.choose_slot_seconds(show_id, remaining) or min(remaining, MIN_SLOT_SECONDS)
                used.add(show_id)
            grid.append({"start_time": slot_start.strftime("%H:%M:%S"), "seconds": seconds, "show_id": show_id})
            prev_show_id = show_id
            cursor += seconds
        return grid

    def build_daytime_schedule(self, channel_id, year, dow, start_str, duration_str):
        """Build and STORE a duration-aware slot grid for the requested window, replacing whatever
        random grid time_slot_schedules holds there. Returns (grid, schedule_data), where
        schedule_data maps "HH:MM:SS" -> show_id, or None when the caller should keep the existing
        flow instead: the window falls outside the scope this system covers (daytime: weekday
        06:00-19:00, weekend 07:00-19:00; late night: 23:00-24:00 or 00:00-06:00), or the window is already templated (a re-run must
        reuse the stored grid so the schedule stays stable)."""
        start_time = datetime.strptime(start_str, "%H:%M:%S").time()
        hours, minutes, seconds = map(int, duration_str.split(":"))
        start = start_time.hour * 3600 + start_time.minute * 60 + start_time.second
        end = start + hours * 3600 + minutes * 60 + seconds

        scope_start = 7 * 3600 if dow in (SATURDAY_DOW, SUNDAY_DOW) else 6 * 3600
        in_daytime = start >= scope_start and end <= 19 * 3600
        # Late night is calendar-day: 23:00-24:00 on a date, and 00:00-06:00 on the NEXT date (that date's
        # own day-of-week). A window can't cross midnight -- run the two halves separately.
        in_late_night = (start >= LATE_NIGHT_START_HOUR * 3600 and end <= 86400) or end <= LATE_NIGHT_END_HOUR * 3600
        if not (in_daytime or in_late_night) or end - start < MIN_SLOT_SECONDS:
            return None

        # 24:00 isn't a valid time; the last second of the day bounds the same slots (time_slot < end_time).
        end_time = time(23, 59, 59) if end >= 86400 else time(end // 3600, (end % 3600) // 60, end % 60)
        self.cur.execute(
            """SELECT 1 FROM schedule_template
               WHERE channel_id = %s AND replication_year = %s AND %s = ANY(days_of_week)
                 AND time_slot >= %s AND time_slot < %s LIMIT 1""",
            (channel_id, year, dow, start_time, end_time),
        )
        if self.cur.fetchone():
            return None

        grid = self.build_daytime_grid(channel_id, year, dow, start_time, end, year)

        # A weekday slot is one Mon-Fri strip (insert_schedule_template copies it to all five
        # days), so the grid has to exist identically for every weekday.
        dows = [0, 1, 2, 3, 4] if dow in (0, 1, 2, 3, 4) else [dow]
        self.cur.execute(
            """DELETE FROM time_slot_schedules
               WHERE channel_id = %s AND schedule_id = %s AND channel_dow = ANY(%s)
                 AND start_time < %s AND end_time > %s""",
            (channel_id, year, dows, end_time, start_time),
        )
        rows = []
        cursor = start
        for slot in grid:
            slot_start = time(cursor // 3600, (cursor % 3600) // 60, cursor % 60)
            for d in dows:
                rows.append((year, channel_id, slot_start, timedelta(seconds=slot["seconds"]), d))
            cursor += slot["seconds"]
        self.cur.executemany(
            """INSERT INTO time_slot_schedules (schedule_id, channel_id, start_time, duration, channel_dow)
               VALUES (%s, %s, %s, %s, %s)""",
            rows,
        )
        self.db_connection.commit()

        schedule_data = {slot["start_time"]: slot["show_id"] for slot in grid if slot["show_id"] is not None}
        return grid, schedule_data
