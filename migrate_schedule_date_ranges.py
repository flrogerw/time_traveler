#!/usr/bin/env python3
"""
Adds real date-range granularity to `schedules`, replacing its year-only `air_date` int as the
thing scheduling logic actually matches against. See the conversation this came out of (scoping
"mid-season show switch aware" scheduling) for the full design discussion.

What this does:
  1. Adds nullable `effective_start_date`/`effective_end_date` (date) columns.
  2. Backfills every row to (Jan 1, air_date)-(Dec 31, air_date) -- today's year-exact behavior
     becomes the explicit default, so nothing changes for rows that don't need splitting.
  3. Narrows the specific rows already researched (see weekend_schedule_collision_resolutions.csv
     on the Desktop) to their real sourced date ranges, resolving the CBS 1965 Secret Agent /
     Trials of O'Brien collision that was previously left flagged (the research gave "December
     1965" as Secret Agent's start -- month precision, but enough to split the two ranges so they
     no longer overlap).

Does NOT resolve the ABC Saturday 1966 19:30 collision (Ozzie and Harriet vs. Shane) --
discovered incidentally while fixing the other 27, and no source for Shane's actual fall-1966
premiere date has been researched yet. Ozzie and Harriet's start (1966-01-15) is applied; its
end stays at the year-end default, so it still overlaps with Shane's own full-year-default range.
Flagged in the printed summary; needs its own research pass (see this project's earlier weekend
research brief pattern) before it can be split cleanly.

Idempotent: adding columns/backfilling is IF NOT EXISTS / WHERE ... IS NULL gated; the specific
overrides just re-apply the same values on rerun.
"""
import os
from datetime import date

import psycopg
from dotenv import load_dotenv

load_dotenv()


def get_db_connection():
    return psycopg.connect(
        host=os.environ["DB_HOST"],
        port=os.environ.get("DB_PORT", "5432"),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        dbname=os.environ.get("DATABASE") or os.environ["DB_NAME"],
    )


# schedule_id -> (effective_start_date or None to leave the Jan-1-of-air_date default,
#                 effective_end_date or None to leave the Dec-31-of-air_date default)
# Sourced from weekend_schedule_collision_resolutions.csv (Desktop) and this project's own
# follow-up research; see fix_weekend_schedule_collisions.py for the row-by-row history.
DATE_RANGE_OVERRIDES = {
    4085: (None, date(1966, 1, 14)),        # Shindig! -- ends the week before Ozzie & Harriet took over
    4086: (date(1966, 1, 15), None),        # The Adventures of Ozzie and Harriet -- began 1966-01-15
    4087: (None, date(1966, 1, 14)),        # The King Family Show -- ends the week before Donna Reed took over
    4088: (date(1966, 1, 15), None),        # The Donna Reed Show -- began 1966-01-15
    4093: (date(1965, 12, 1), None),        # Secret Agent -- began December 1965 (month precision only)
    4094: (None, date(1965, 11, 30)),       # The Trials of O'Brien -- ends when Secret Agent took over
    4095: (date(1965, 9, 18), date(1966, 4, 30)),  # The Loner -- premiered 1965-09-18, ran (incl. repeats) through 1966-04-30
    4096: (date(1966, 5, 7), None),         # The Face is Familiar -- began 1966-05-07
    3012: (date(1977, 1, 30), None),        # The Hardy Boys/Nancy Drew Mysteries -- premiered 1977-01-30
}


def migrate_schema(conn):
    cur = conn.cursor()
    cur.execute("ALTER TABLE schedules ADD COLUMN IF NOT EXISTS effective_start_date date")
    cur.execute("ALTER TABLE schedules ADD COLUMN IF NOT EXISTS effective_end_date date")
    conn.commit()
    print("Schema migration done.")


def backfill_defaults(conn):
    cur = conn.cursor()
    cur.execute(
        """UPDATE schedules
           SET effective_start_date = make_date(air_date, 1, 1),
               effective_end_date = make_date(air_date, 12, 31)
           WHERE effective_start_date IS NULL OR effective_end_date IS NULL"""
    )
    print(f"Backfilled {cur.rowcount} rows with default (Jan 1-Dec 31 of air_date) ranges.")
    conn.commit()


def apply_researched_overrides(conn):
    cur = conn.cursor()
    applied = 0
    for schedule_id, (start, end) in DATE_RANGE_OVERRIDES.items():
        if start is not None:
            cur.execute("UPDATE schedules SET effective_start_date = %s WHERE schedule_id = %s", (start, schedule_id))
        if end is not None:
            cur.execute("UPDATE schedules SET effective_end_date = %s WHERE schedule_id = %s", (end, schedule_id))
        applied += 1
    conn.commit()
    print(f"Applied researched date-range overrides to {applied} rows.")


def main():
    conn = get_db_connection()
    try:
        migrate_schema(conn)
        backfill_defaults(conn)
        apply_researched_overrides(conn)

        cur = conn.cursor()
        cur.execute(
            """
            SELECT a.schedule_id, a.show_title, b.schedule_id, b.show_title,
                   a.network, a.day_of_week, a.effective_start_date, a.effective_end_date,
                   b.effective_start_date, b.effective_end_date
            FROM schedules a
            JOIN schedules b ON a.network = b.network AND a.day_of_week = b.day_of_week
                AND a.air_time = b.air_time AND a.schedule_id < b.schedule_id
                AND a.effective_start_date <= b.effective_end_date
                AND b.effective_start_date <= a.effective_end_date
            WHERE a.day_of_week IN ('saturday', 'sunday')
            """
        )
        remaining = cur.fetchall()
        print(f"\nRemaining overlapping weekend date ranges: {len(remaining)} (expected: 1 -- Ozzie and Harriet vs. Shane, unresolved)")
        for row in remaining:
            print(" ", row)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
