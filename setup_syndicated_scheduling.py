#!/usr/bin/env python3
"""
One-time (but re-runnable) setup for era-aware syndicated daytime scheduling.

See CLAUDE_SYNDICATED_SCHEDULING_HANDOFF.md (on the Desktop) for the design this implements.

What this does, in order:
  1. Adds nullable `year_start`/`year_end` columns to `fill_rules` and `syndicated_fixed_slots`
     (existing NULL/NULL rows keep meaning "applies to all years" -- untouched).
  2. Creates `show_syndication_eligibility` (one row per show: mapped_genre, eligible year
     range, and which of the 7 weekday daypart blocks it's suitable for). Kept as its own table
     rather than new columns on `shows`, matching how `fill_rules`/`syndicated_fixed_slots`
     already live outside `shows` for the same reason -- this is scheduling metadata, not a show
     fact.
  3. Imports `syndicated_mapping.csv`'s era/daypart/genre weights into `fill_rules`, one row per
     (syndicated channel, era, daypart, category) -- exploding `primary_genre` only.
     `secondary_genre` is a descriptive/format tag (rerun, cartoon, classic, ...), not a second
     selectable dimension, so it is NOT inserted as its own weighted category.
  4. Imports `show_syndication_tags_FIXED.csv` into `show_syndication_eligibility`.

Idempotent: re-running deletes and reinserts only the rows this script owns (era-aware
fill_rules rows, identified by non-null year_start, and the full contents of
show_syndication_eligibility), so it never touches the pre-existing generic/legacy fill_rules
or syndicated_fixed_slots rows.

Known data gap (see conversation notes): `syndicated_mapping.csv` includes a `religious`
category with no matching show in `show_syndication_tags_FIXED.csv`. This is handled at
*runtime* by SyndicatedScheduler (it drops empty categories and re-picks -- see that class),
not by editing the CSV, so no fix is needed here.
"""
import csv
import os

import psycopg
from dotenv import load_dotenv

load_dotenv()

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
DESKTOP = os.path.expanduser("~/Desktop")
MAPPING_CSV = os.path.join(DESKTOP, "syndicated_mapping.csv")
TAGS_CSV = os.path.join(DESKTOP, "show_syndication_tags_FIXED.csv")

# Monday=0 .. Friday=4, matching Python's date.weekday() already used throughout this codebase.
WEEKDAY_DOWS = [0, 1, 2, 3, 4]

WEEKDAY_FLAG_COLUMNS = [
    "weekday_early_morning",
    "weekday_morning",
    "weekday_late_morning",
    "weekday_noon",
    "weekday_early_afternoon",
    "weekday_after_school",
    "weekday_early_evening",
]


def get_db_connection():
    return psycopg.connect(
        host=os.environ["DB_HOST"],
        port=os.environ.get("DB_PORT", "5432"),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        dbname=os.environ.get("DATABASE") or os.environ["DB_NAME"],
    )


def migrate_schema(conn):
    cur = conn.cursor()
    cur.execute("ALTER TABLE fill_rules ADD COLUMN IF NOT EXISTS year_start integer")
    cur.execute("ALTER TABLE fill_rules ADD COLUMN IF NOT EXISTS year_end integer")
    cur.execute("ALTER TABLE syndicated_fixed_slots ADD COLUMN IF NOT EXISTS year_start integer")
    cur.execute("ALTER TABLE syndicated_fixed_slots ADD COLUMN IF NOT EXISTS year_end integer")
    cur.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = 'show_syndication_eligibility'"
    )
    if cur.fetchone():
        # Table pre-dates the nullable-eligibility-years fix -- relax the constraint in place.
        cur.execute("ALTER TABLE show_syndication_eligibility ALTER COLUMN eligible_year_start DROP NOT NULL")
        cur.execute("ALTER TABLE show_syndication_eligibility ALTER COLUMN eligible_year_end DROP NOT NULL")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS show_syndication_eligibility (
            show_id integer PRIMARY KEY REFERENCES shows(show_id),
            mapped_genre text NOT NULL,
            eligible_year_start integer,
            eligible_year_end integer,
            eligibility_basis text,
            weekday_early_morning boolean NOT NULL DEFAULT false,
            weekday_morning boolean NOT NULL DEFAULT false,
            weekday_late_morning boolean NOT NULL DEFAULT false,
            weekday_noon boolean NOT NULL DEFAULT false,
            weekday_early_afternoon boolean NOT NULL DEFAULT false,
            weekday_after_school boolean NOT NULL DEFAULT false,
            weekday_early_evening boolean NOT NULL DEFAULT false
        )
        """
    )
    conn.commit()
    print("Schema migration done.")


def get_syndicated_channel_ids(conn):
    cur = conn.cursor()
    cur.execute("SELECT channel_id FROM channels WHERE channel_type = 'syndicated' ORDER BY channel_id")
    return [r[0] for r in cur.fetchall()]


def import_fill_rules(conn, channel_ids):
    cur = conn.cursor()
    # Only remove rows this script owns (era-aware = year_start IS NOT NULL); the pre-existing
    # generic/legacy rows (year_start/year_end NULL) are left alone per the handoff doc's
    # backward-compatibility requirement.
    cur.execute("DELETE FROM fill_rules WHERE year_start IS NOT NULL")

    rows = list(csv.DictReader(open(MAPPING_CSV)))
    inserted = 0
    for row in rows:
        for channel_id in channel_ids:
            cur.execute(
                """
                INSERT INTO fill_rules
                    (channel_id, start_time, end_time, category, weight, dows, year_start, year_end)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    channel_id,
                    row["start_time"],
                    row["end_time"],
                    row["primary_genre"],
                    float(row["weight"]) / 100.0,
                    WEEKDAY_DOWS,
                    int(row["year_start"]),
                    int(row["year_end"]),
                ),
            )
            inserted += 1
    conn.commit()
    print(f"Imported {inserted} era-aware fill_rules rows ({len(rows)} mapping rows x {len(channel_ids)} channels).")


def import_show_eligibility(conn):
    cur = conn.cursor()
    cur.execute("DELETE FROM show_syndication_eligibility")

    rows = list(csv.DictReader(open(TAGS_CSV)))
    columns = ["show_id", "mapped_genre", "eligible_year_start", "eligible_year_end", "eligibility_basis"] + WEEKDAY_FLAG_COLUMNS
    skipped = 0
    for row in rows:
        # A handful of shows (e.g. Cheers, Golden Girls, King of the Hill) run past the CSV's
        # 1989 era-band ceiling, so their "eligible from" year was left blank rather than
        # guessed -- they're simply not eligible for this era-aware system (out of its 1960-1989
        # scope), not a data error. Stored as NULL/NULL; get_eligible_shows() requires non-null
        # eligible years, so these rows are inert rather than wrongly matching every year.
        if not row["eligible_year_start"] or not row["eligible_year_end"]:
            skipped += 1
            year_start = year_end = None
        else:
            year_start = int(row["eligible_year_start"])
            year_end = int(row["eligible_year_end"])
        values = [
            int(row["show_id"]),
            row["mapped_genre"],
            year_start,
            year_end,
            row["eligibility_basis"],
        ] + [row[c] == "1" for c in WEEKDAY_FLAG_COLUMNS]
        cur.execute(
            f"INSERT INTO show_syndication_eligibility ({', '.join(columns)}) "
            f"VALUES ({', '.join(['%s'] * len(columns))})",
            values,
        )
    conn.commit()
    print(f"Imported {len(rows)} show_syndication_eligibility rows ({skipped} with no eligible year range -- out of era-band scope).")


def main():
    conn = get_db_connection()
    try:
        migrate_schema(conn)
        channel_ids = get_syndicated_channel_ids(conn)
        print(f"Syndicated channel_ids: {channel_ids}")
        import_fill_rules(conn, channel_ids)
        import_show_eligibility(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
