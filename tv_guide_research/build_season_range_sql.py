"""Split proposed_schedules_season_ranges.csv into an applicable set and a held-back set, and
emit reviewable SQL (apply + rollback). Writes FILES ONLY -- never touches the DB."""
import csv
import datetime as d
import os

HERE = os.path.dirname(os.path.abspath(__file__))
D = d.date.fromisoformat
rows = list(csv.DictReader(open(os.path.join(HERE, "proposed_schedules_season_ranges.csv"))))

ok, held = [], []
for r in rows:
    start, end = D(r["proposed_start"]), D(r["proposed_end"])
    span = (end - start).days
    air = int(r["air_date(season year)"])
    if (r["current_start"], r["current_end"]) != ("%d-01-01" % air, "%d-12-31" % air):
        reason = "current range was already hand-researched/narrowed -- keep it"
    elif span <= 0:
        reason = "single-day range (1 episode) -- keep current range"
    elif span > 400:
        reason = "range > 400 days: episode airdates span multiple seasons -- bad/mixed airdate data"
    elif start.year > air:
        reason = "proposed start is after the row's air_date year -- row would leave its own year; verify"
    else:
        ok.append(r)
        continue
    held.append({**r, "reason": reason})

# overlaps among applicable rows in the same slot
slots = {}
for r in ok:
    slots.setdefault((r["network"], r["day"], r["time"]), []).append(r)
still = []
for v in slots.values():
    v.sort(key=lambda r: r["proposed_start"])
    for a, b in zip(v, v[1:]):
        if D(b["proposed_start"]) <= D(a["proposed_end"]):
            still.append((a, b))
assert not still, still

def w(name, fields, data):
    with open(os.path.join(HERE, name), "w", newline="") as f:
        cw = csv.DictWriter(f, fieldnames=fields)
        cw.writeheader()
        cw.writerows(data)

fields = list(rows[0].keys())
w("season_ranges_applicable.csv", fields, ok)
w("season_ranges_held_back.csv", fields + ["reason"], held)

with open(os.path.join(HERE, "apply_season_ranges.sql"), "w") as f:
    f.write("-- PROPOSAL ONLY: not applied. %d rows; widest-first ORDER BY in Shows.get_scheduled_shows_id\n" % len(ok))
    f.write("-- makes the narrowed range win over the next year's default Jan-Dec row.\nBEGIN;\n")
    for r in ok:
        f.write("UPDATE schedules SET effective_start_date='%s', effective_end_date='%s' WHERE schedule_id=%s;\n"
                % (r["proposed_start"], r["proposed_end"], r["schedule_id"]))
    f.write("-- Star Trek 1968 (row 2416) now covers 1968-09-20..1969-06-03, so row 4454 (1969-01-01..06-03) is redundant:\n")
    f.write("-- DELETE FROM schedules WHERE schedule_id=4454;\nCOMMIT;\n")

with open(os.path.join(HERE, "rollback_season_ranges.sql"), "w") as f:
    f.write("BEGIN;\n")
    for r in ok:
        f.write("UPDATE schedules SET effective_start_date='%s', effective_end_date='%s' WHERE schedule_id=%s;\n"
                % (r["current_start"], r["current_end"], r["schedule_id"]))
    f.write("COMMIT;\n")
print(len(ok), "applicable;", len(held), "held back")
for reason in sorted({h["reason"] for h in held}):
    print(sum(h["reason"] == reason for h in held), reason)
