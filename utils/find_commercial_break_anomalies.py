import hashlib
import logging
import os
from collections import defaultdict
from typing import List, Dict, Tuple
from pprint import pprint
from typing import List, Dict
from dotenv import load_dotenv

import psycopg
from psycopg import Connection as connection
from psycopg.rows import dict_row

load_dotenv()
logging.basicConfig(level=logging.INFO)

CLOSE_GAP_SECONDS = 5
MAX_BREAK_LENGTH = 600  # 10 minutes


def get_db_connection() -> connection:
    """
    Establish and return a PostgreSQL database connection using environment variables.

    Returns:
        psycopg.Connection: PostgreSQL database connection object.
    """
    try:
        conn = psycopg.connect(
            dbname=os.getenv('DATABASE'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=5432
        )
        logging.info("Database connection established.")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to the database: {e}")
        raise


def make_pattern_fingerprint(breaks, duration):
    positions = [
        round(b['break_point'] / duration, 3)
        for b in breaks
    ]
    raw = f"{len(breaks)}|" + ",".join(map(str, positions))
    return hashlib.sha1(raw.encode()).hexdigest()


def find_break_anomalies(
        rows: List[Dict],
        min_start_seconds: float = 10,
        close_gap_seconds: float = 5,
        max_break_length: float = 600,
        max_breaks_per_minute: float = 1.5,
):
    anomalies = []
    delete_breaks = defaultdict(list)

    media_groups = defaultdict(list)
    pattern_fingerprints = defaultdict(lambda: defaultdict(list))

    # Group rows by media_id
    for r in rows:
        media_groups[r["media_id"]].append(r)

    for media_id, breaks in media_groups.items():
        breaks = sorted(breaks, key=lambda x: x["break_point"])
        duration = breaks[0].get("duration_seconds")
        show_name = breaks[0]['show_name']
        show_id = breaks[0]['show_id']
        end_point = breaks[0]['end_point']

        seen = set()

        fingerprint = make_pattern_fingerprint(
            breaks,
            duration
        )

        pattern_fingerprints[fingerprint][show_id].append({
            "media_id": media_id,
            "breaks": breaks
        })

        # ---- Commercial density ----
        if duration and duration > 0:
            minutes = duration / 60
            density = len(breaks) / minutes
            if density > max_breaks_per_minute:
                anomalies.append({
                    "media_id": media_id,
                    "show_name": show_name,
                    "type": "HIGH_COMMERCIAL_DENSITY",
                    "breaks": len(breaks),
                    "minutes": round(minutes, 2),
                    "density": round(density, 2),
                })

        for i, current in enumerate(breaks):
            s = current["break_point"]
            r = current["resume_point"]
            b = current["break_id"]

            key = (s, r)

            # ---- Invalid range ----
            if r <= s:
                anomalies.append({
                    "media_id": media_id,
                    "show_name": show_name,
                    "type": "INVALID_RANGE",
                    "details": current,
                })

            # ---- Break too early ----
            if s < min_start_seconds:
                anomalies.append({
                    "media_id": media_id,
                    "show_name": show_name,
                    "type": "BREAK_TOO_EARLY",
                    "break_point": s,
                    "break_id": b
                })
                delete_breaks.setdefault(show_name, []).append(b)

            # ---- Duplicate ----
            if key in seen:
                anomalies.append({
                    "media_id": media_id,
                    "show_name": show_name,
                    "type": "DUPLICATE_BREAK",
                    "details": current,
                })
            else:
                seen.add(key)

            # ---- Excessively long ----
            if (r - s) > max_break_length:
                anomalies.append({
                    "media_id": media_id,
                    "show_name": show_name,
                    "type": "LONG_BREAK",
                    "length": r - s,
                    "details": current,
                })

            # ---- Past end of file ----
            if end_point and r > end_point:
                anomalies.append({
                    "media_id": media_id,
                    "show_name": show_name,
                    "type": "BREAK_PAST_END",
                    "break_point": s,
                    "resume_point": r,
                    "duration": duration,
                })

            if i == 0:
                continue

            prev = breaks[i - 1]
            ps, pr = prev["break_point"], prev["resume_point"]

            # ---- Overlapping ----
            if s < pr:
                anomalies.append({
                    "media_id": media_id,
                    "show_name": show_name,
                    "type": "OVERLAPPING_BREAK",
                    "previous": prev,
                    "current": current,
                })

            # ---- Nested ----
            if s >= ps and r <= pr:
                anomalies.append({
                    "media_id": media_id,
                    "show_name": show_name,
                    "type": "NESTED_BREAK",
                    "outer": prev,
                    "inner": current,
                })

            # ---- Too close ----
            gap = s - pr
            if 0 <= gap < close_gap_seconds:
                anomalies.append({
                    "media_id": media_id,
                    "show_name": show_name,
                    "type": "CLOSE_BREAKS",
                    "gap_seconds": gap,
                    "previous": prev,
                    "current": current,
                })
                delete_breaks.setdefault(show_name, []).append(prev['break_id'])

    # ---- Repeated pattern detection across media ----
    for fingerprint, show_map in pattern_fingerprints.items():
        for show_id, entries in show_map.items():
            if len(entries) > 1:
                anomalies.append({
                    "type": "REPEATED_PATTERN_WITHIN_SHOW",
                    "show_id": show_id,
                    "pattern": fingerprint,
                    "occurrences": [
                        {
                            "media_id": e["media_id"],
                            "breaks": [
                                {
                                    "break_id": d["break_id"],
                                    "media_id": d["media_id"],
                                    "start": d["break_point"],
                                    "end": d["resume_point"],
                                    "show_name": d["show_name"]
                                }
                                for d in e['breaks']
                            ]
                        }
                        for e in entries
                    ],
                })

    pprint(delete_breaks)
    return anomalies


with get_db_connection() as db:
    cur = db.cursor(row_factory=dict_row)
    cur.execute("""
        SELECT break_id, media_id, break_point, resume_point, s.show_name,
        e.end_point - e.start_point as duration_seconds, end_point, s.show_id
        FROM commercial_breaks cb
        JOIN episodes e ON media_id = episode_id
        JOIN shows s on e.show_id = s.show_id
        ORDER BY media_id, break_point
    """)
    rows = cur.fetchall()
    rows = [dict(row) for row in rows]

anomalies = find_break_anomalies(rows)

for a in anomalies:
    pprint(a)
print(len(anomalies))
