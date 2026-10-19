"""Proposal (files only): replace CBS 1972 Saturday's 11.5-hour 08:30 slot in time_slot_schedules.

The slot is the New Scooby-Doo Movies anchor (show 152, 08:30) stretched to the next anchor at 20:00 -- the bug fixed in
Shows.calculate_time_differences. This writes the grid that code would have produced: the anchor keeps its own longest length,
and the rest of the gap is filled with 30/60-minute slots using Schedules' own weighted duration choice (seeded, so the file
is reviewable and repeatable). schedule_template / broadcast_log are untouched; new filler slots get templated on the next run.

    python -m tv_guide_research.build_cbs1972_grid_fix
"""
import os, random
import psycopg
from dotenv import load_dotenv

load_dotenv()
HERE = os.path.dirname(os.path.abspath(__file__))
CHANNEL_ID, YEAR, DOW, ANCHOR_SHOW, START, END = 1, 1972, 5, 152, 8.5 * 3600, 20 * 3600
DURATIONS, WEIGHTS = [1800, 3600], [0.8, 0.3]   # Schedules.durations / Schedules.weights


def main():
    conn = psycopg.connect(host=os.environ["DB_HOST"], port=os.environ.get("DB_PORT", "5432"), user=os.environ["DB_USER"],
                           password=os.environ["DB_PASSWORD"], dbname=os.environ.get("DATABASE") or os.environ["DB_NAME"])
    conn.read_only = True
    cur = conn.cursor()
    cur.execute("SELECT channel_name FROM channels WHERE channel_id=%s", (CHANNEL_ID,))
    assert cur.fetchone()[0] == "TV-CBS-2"
    cur.execute("SELECT start_time, duration FROM time_slot_schedules WHERE channel_id=%s AND schedule_id=%s AND channel_dow=%s "
                "AND start_time = '08:30'", (CHANNEL_ID, YEAR, DOW))
    old = cur.fetchone()
    assert old and int(old[1].total_seconds()) == 41400, old
    cur.execute("SELECT MAX(d) FROM (SELECT unnest(show_duration) d FROM shows WHERE show_id=%s) x", (ANCHOR_SHOW,))
    anchor = cur.fetchone()[0]
    conn.rollback()
    rng = random.Random(1972)
    slots, t = [(START, anchor)], START + anchor
    while t < END:
        d = rng.choices(DURATIONS, WEIGHTS)[0]
        d = min(d, END - t)
        slots.append((t, d)); t += d
    hms = lambda s: f"{int(s // 3600):02d}:{int(s % 3600 // 60):02d}"
    ins = "INSERT INTO time_slot_schedules (schedule_id, channel_id, start_time, duration, channel_dow) VALUES\n  " + ",\n  ".join(
        f"({YEAR}, {CHANNEL_ID}, '{hms(s)}', INTERVAL '{int(d)} seconds', {DOW})" for s, d in slots) + ";\n"
    where = f"channel_id = {CHANNEL_ID} AND schedule_id = {YEAR} AND channel_dow = {DOW} AND start_time >= '08:30' AND start_time < '20:00'"
    with open(os.path.join(HERE, "apply_cbs1972_grid_fix.sql"), "w") as f:
        f.write(f"-- PROPOSAL ONLY: not applied. Replaces the 690-minute 08:30 slot ({len(slots)} slots, 08:30-20:00).\nBEGIN;\n"
                f"DELETE FROM time_slot_schedules WHERE {where};\n{ins}COMMIT;\n")
    with open(os.path.join(HERE, "rollback_cbs1972_grid_fix.sql"), "w") as f:
        f.write(f"BEGIN;\nDELETE FROM time_slot_schedules WHERE {where};\n"
                f"INSERT INTO time_slot_schedules (schedule_id, channel_id, start_time, duration, channel_dow) VALUES\n"
                f"  ({YEAR}, {CHANNEL_ID}, '08:30', INTERVAL '41400 seconds', {DOW});\nCOMMIT;\n")
    print(f"anchor {anchor}s; {len(slots)} slots:", [(hms(s), int(d // 60)) for s, d in slots])


if __name__ == "__main__":
    main()
