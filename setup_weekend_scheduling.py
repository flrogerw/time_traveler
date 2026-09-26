#!/usr/bin/env python3
"""
Additive weekend (Saturday/Sunday) patch on top of the already-implemented era-aware syndicated
weekday scheduling (setup_syndicated_scheduling.py). See CLAUDE_WEEKEND_SCHEDULING_HANDOFF.md and
CLAUDE_WEEKEND_GENRE_FIX.md (Desktop) for the design.

Does NOT touch the Monday-Friday implementation: no changes to weekday fill_rules rows, to
show_syndication_eligibility's mapped_genre/eligible_year_* columns, or to SyndicatedScheduler's
weekday behavior.

Per CLAUDE_WEEKEND_GENRE_FIX.md, a show has exactly one canonical `mapped_genre` (the weekday
value, already imported) -- this script does NOT add a second weekend genre column. It only
adds the 10 Saturday/Sunday daypart boolean columns (new information: which dayparts a show is
suitable for on weekends, same idea as the 7 existing weekday_* columns) and imports the
already-genre-reconciled *_FIXED weekend CSVs:
  - show_syndication_tags_weekend_FIXED.csv -> UPDATE existing show_syndication_eligibility rows,
    setting only the 10 weekend daypart columns (never mapped_genre/eligible_year_*).
  - fill_rules_weekend_import_FIXED.csv -> INSERT into fill_rules (dow 5/6 rows only).
  - network_weekend_schedules_import.csv -> INSERT into schedules (historical network seeds for
    Saturday/Sunday, 1965-1969; rows with a blank show_id are kept with show_id NULL per the
    handoff doc, since the existing schedule-seeding fallback already handles that gracefully).

Idempotent: re-running deletes and reinserts only the rows this script owns (fill_rules rows with
dows @> '{5}' or '{6}' among the 4 syndicated channels; schedules rows matching this CSV's
(network, show_title, air_date, air_time, day_of_week) keys).
"""
import csv
import os

import psycopg
from dotenv import load_dotenv

load_dotenv()

DESKTOP = os.path.expanduser("~/Desktop")
TAGS_CSV = os.path.join(DESKTOP, "show_syndication_tags_weekend_FIXED.csv")
FILL_RULES_CSV = os.path.join(DESKTOP, "fill_rules_weekend_import_FIXED.csv")
NETWORK_SCHEDULES_CSV = os.path.join(DESKTOP, "network_weekend_schedules_import.csv")

WEEKEND_FLAG_COLUMNS = [
    "saturday_early_morning", "saturday_morning", "saturday_midday", "saturday_afternoon", "saturday_early_evening",
    "sunday_early_morning", "sunday_morning", "sunday_midday", "sunday_afternoon", "sunday_early_evening",
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
    for column in WEEKEND_FLAG_COLUMNS:
        cur.execute(f"ALTER TABLE show_syndication_eligibility ADD COLUMN IF NOT EXISTS {column} boolean NOT NULL DEFAULT false")
    conn.commit()
    print("Weekend daypart columns added to show_syndication_eligibility.")


def import_weekend_eligibility(conn):
    cur = conn.cursor()
    rows = list(csv.DictReader(open(TAGS_CSV)))
    set_clause = ", ".join(f"{c} = %s" for c in WEEKEND_FLAG_COLUMNS)
    updated = 0
    for row in rows:
        values = [row[c] == "1" for c in WEEKEND_FLAG_COLUMNS] + [int(row["show_id"])]
        cur.execute(f"UPDATE show_syndication_eligibility SET {set_clause} WHERE show_id = %s", values)
        updated += cur.rowcount
    conn.commit()
    print(f"Updated weekend daypart flags on {updated} show_syndication_eligibility rows.")


def import_weekend_fill_rules(conn):
    cur = conn.cursor()
    # Only remove rows this script owns: syndicated channels' weekend (dow 5/6) rows. Weekday
    # rows (dows = {0,1,2,3,4}) and any other channel/dow combination are left untouched.
    cur.execute(
        """DELETE FROM fill_rules
           WHERE channel_id IN (SELECT channel_id FROM channels WHERE channel_type = 'syndicated')
           AND (dows @> ARRAY[5] OR dows @> ARRAY[6])"""
    )

    rows = list(csv.DictReader(open(FILL_RULES_CSV)))
    for row in rows:
        dows = [int(d) for d in row["dows"].strip("{}").split(",")]
        cur.execute(
            """INSERT INTO fill_rules (channel_id, start_time, end_time, category, weight, dows, year_start, year_end)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                int(row["channel_id"]), row["start_time"], row["end_time"], row["category"],
                float(row["weight"]), dows, int(row["year_start"]), int(row["year_end"]),
            ),
        )
    conn.commit()
    print(f"Imported {len(rows)} weekend fill_rules rows.")


def import_weekend_network_schedules(conn):
    cur = conn.cursor()
    rows = list(csv.DictReader(open(NETWORK_SCHEDULES_CSV)))

    cur.execute("SELECT network, show_title, air_date, air_time, day_of_week FROM schedules")
    existing_keys = set(cur.fetchall())

    inserted = skipped_existing = with_null_show_id = 0
    for row in rows:
        air_date = int(row["air_date"])
        key = (row["network"], row["show_title"], air_date, row["air_time"], row["day_of_week"])
        if key in existing_keys:
            skipped_existing += 1
            continue
        show_id = int(row["show_id"]) if row["show_id"] else None
        if show_id is None:
            with_null_show_id += 1
        cur.execute(
            """INSERT INTO schedules (network, show_title, air_date, air_time, day_of_week, show_id)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (row["network"], row["show_title"], air_date, row["air_time"], row["day_of_week"], show_id),
        )
        existing_keys.add(key)
        inserted += 1
    conn.commit()
    print(f"Imported {inserted} network weekend schedule rows ({with_null_show_id} with no matched show_id, "
          f"{skipped_existing} already present and skipped).")


def main():
    conn = get_db_connection()
    try:
        migrate_schema(conn)
        import_weekend_eligibility(conn)
        import_weekend_fill_rules(conn)
        import_weekend_network_schedules(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
