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
from datetime import time

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

SATURDAY_DOW = 5
SUNDAY_DOW = 6


def daypart_column_for(start_time: time, dow: int = None):
    """Return the show_syndication_eligibility column name for a slot start time/day, or None if
    the time falls outside this system's daytime scope for that day (weekday: 06:00-19:00;
    weekend: 07:00-19:00) -- e.g. primetime/late-night syndicated slots, which this system
    doesn't cover on any day."""
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
    return None


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

    def get_eligible_shows(self, category, year, daypart_column, channel_id, replication_year, exclude_show_ids=None):
        """Shows matching `category`/`daypart_column`, eligible for `year`, and not already
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

        self.cur.execute(query, params)
        return [row["show_id"] for row in self.cur.fetchall()]

    def choose_show(self, show_ids):
        return random.choice(show_ids) if show_ids else None

    def resolve_syndicated_slot(self, channel_id, year, dow, start_time, replication_year, exclude_show_ids=None):
        """
        Full resolution for one syndicated daytime slot:
          1. A fixed slot always wins if one exists for this channel/day/time.
          2. Otherwise, weighted-pick a category from matching fill_rules, dropping any category
             that turns up zero eligible shows and re-picking from what's left (e.g. this repo
             has no 'religious' show in its library, so that category is simply skipped rather
             than failing the slot).
          3. If every candidate category comes up empty (a fully starved slot -- no fill_rules
             match, or none of their categories have any eligible show), returns None so the
             caller can fall back to the existing genre-bias engine
             (Shows.get_genre_bias_for_slot / get_available_show_id) instead of leaving the slot
             unfilled.

        Returns a show_id, or None if nothing could be resolved here.
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
                category, year, daypart_column, channel_id, replication_year, exclude_show_ids
            )
            if show_ids:
                return self.choose_show(show_ids)
            remaining = [r for r in remaining if r["category"] != category]

        return None
