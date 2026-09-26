#!/usr/bin/env python3
"""
Collapse true exact-duplicate rows in `episodes` -- groups sharing the same
(show_id, show_season_number, episode_number) AND the identical episode_file.

Does NOT touch groups where duplicate (show_id, season, episode_number) rows point at
DIFFERENT episode_files -- those are numbering collisions between genuinely different
episodes (cartoon package-segment numbering, a broken multi-part episode, a show mis-filed
under the wrong show_id, conflicting numbering conventions), not duplicates, and need
per-show manual correction instead. See COMMERCIAL_BREAK_AUDIT.md / CLAUDE.md for context.

Dry-run by default; pass --apply to actually commit deletes. Always writes a JSON report of
every group's decision, whether dry-run or applied.
"""
import argparse
import json
import os
from datetime import datetime

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row

GROUP_QUERY = """
    SELECT show_id, show_season_number, episode_number
    FROM episodes
    GROUP BY show_id, show_season_number, episode_number
    HAVING COUNT(*) > 1 AND COUNT(DISTINCT episode_file) = 1
"""


def connect():
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
    return psycopg.connect(
        host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        dbname=os.environ["DB_NAME"],
    )


def find_groups(cur):
    cur.execute(GROUP_QUERY)
    return cur.fetchall()


def plan_group(cur, group):
    cur.execute(
        """
        SELECT e.episode_id, e.episode_file,
               COALESCE((SELECT COUNT(*) FROM commercial_breaks cb WHERE cb.media_id = e.episode_id), 0) AS break_count
        FROM episodes e
        WHERE e.show_id = %s AND e.show_season_number = %s AND e.episode_number = %s
        ORDER BY break_count DESC, e.episode_id ASC
        """,
        (group["show_id"], group["show_season_number"], group["episode_number"]),
    )
    rows = cur.fetchall()
    keeper = rows[0]
    losers = rows[1:]
    return keeper, losers, rows


def delete_loser(cur, episode_id):
    cur.execute("DELETE FROM commercial_breaks WHERE media_id = %s", (episode_id,))
    cb_deleted = cur.rowcount
    cur.execute("DELETE FROM commercials_processed WHERE media_id = %s", (episode_id,))
    cp_deleted = cur.rowcount
    cur.execute("DELETE FROM episodes WHERE episode_id = %s", (episode_id,))
    return cb_deleted, cp_deleted


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Actually commit the deletes (default: dry-run).")
    parser.add_argument("--report", default="dedup_exact_episodes_report.json", help="Path to write the JSON report.")
    args = parser.parse_args()

    conn = connect()
    cur = conn.cursor(row_factory=dict_row)

    groups = find_groups(cur)
    print(f"Found {len(groups)} exact-duplicate groups.")

    report = {
        "run_at": datetime.now().isoformat(),
        "applied": args.apply,
        "groups": [],
    }

    total_deleted = 0
    for group in groups:
        keeper, losers, all_rows = plan_group(cur, group)
        entry = {
            "show_id": group["show_id"],
            "show_season_number": group["show_season_number"],
            "episode_number": str(group["episode_number"]),
            "episode_file": keeper["episode_file"],
            "kept_episode_id": keeper["episode_id"],
            "kept_break_count": keeper["break_count"],
            "deleted": [],
        }
        for loser in losers:
            if args.apply:
                cb_deleted, cp_deleted = delete_loser(cur, loser["episode_id"])
            else:
                cb_deleted = loser["break_count"]
                cp_deleted = None
            entry["deleted"].append({
                "episode_id": loser["episode_id"],
                "break_count": loser["break_count"],
                "commercial_breaks_deleted": cb_deleted,
                "commercials_processed_deleted": cp_deleted,
            })
            total_deleted += 1
        report["groups"].append(entry)

    if args.apply:
        conn.commit()
        print(f"Applied: deleted {total_deleted} duplicate episode rows.")
    else:
        conn.rollback()
        print(f"Dry-run: would delete {total_deleted} duplicate episode rows. Re-run with --apply to commit.")

    with open(args.report, "w") as f:
        json.dump(report, f, indent=2)
    print(f"Report written to {args.report}")


if __name__ == "__main__":
    main()
