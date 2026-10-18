"""Re-derive the held-back split-season rows that have a fixable cause, and compute summer
extensions for all narrowed rows. Writes FILES ONLY -- never touches the DB.

Held-back reasons handled here:
  * range > 400 days: the episode set mixed two seasons. Re-derive from episodes airdated inside
    the row's season window (Aug 1 of air_date year .. Jul 31 of the next year); needs >= 5 episodes.
  * proposed start after the row's year: accept a genuine mid-season premiere (starts Jan-Mar of
    the next year, >= 9 episodes, <= 300-day span); everything else stays held. MANUAL_HOLD lists
    rows rejected on outside knowledge.
"""
import csv, datetime as d, os
from dotenv import load_dotenv
import psycopg2

HERE = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(HERE, "..", ".env"))
D = d.date.fromisoformat
MANUAL_HOLD = {682: "Topper really premiered 1953-10-09 on CBS; DB airdates only start 1954-01"}

c = psycopg2.connect(dbname=os.getenv("DB_NAME"), user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD"),
                     host=os.getenv("DB_HOST"), port=os.getenv("DB_PORT", "5432"))
c.set_session(readonly=True)
cur = c.cursor()

held = list(csv.DictReader(open(os.path.join(HERE, "season_ranges_held_back.csv"))))
resolved, still = [], []
for r in held:
    sid_row, yr = int(r["schedule_id"]), int(r["air_date(season year)"])
    why = r["reason"]
    if not why.startswith(("range >", "proposed start")):
        still.append(r); continue
    cur.execute("select show_id from schedules where schedule_id=%s", (sid_row,))
    show_id = cur.fetchone()[0]
    start, end = D(r["proposed_start"]), D(r["proposed_end"])
    if why.startswith("range >"):
        cur.execute("select min(episode_airdate), max(episode_airdate), count(*) from episodes "
                    "where show_id=%s and episode_airdate between %s and %s",
                    (show_id, d.date(yr, 8, 1), d.date(yr + 1, 7, 31)))
        lo, hi, n = cur.fetchone()
        if n < 5 or (hi - lo).days > 400:
            r["reason"] = why + "; windowed re-derive had only %d episode(s) -- still unresolved" % n
            still.append(r); continue
        r.update(proposed_start=lo.isoformat(), proposed_end=hi.isoformat(), episodes_in_season=n)
        resolved.append(r)
    else:
        n, span = int(r["episodes_in_season"]), (end - start).days
        if sid_row in MANUAL_HOLD:
            r["reason"] = why + "; " + MANUAL_HOLD[sid_row]; still.append(r)
        elif start.year == yr + 1 and start.month <= 3 and n >= 9 and span <= 300:
            resolved.append(r)
        else:
            r["reason"] = why + "; not a clean mid-season premiere (n=%d, span=%dd) -- keep current range" % (n, span)
            still.append(r)

fields = list(held[0].keys())
def w(name, data):
    with open(os.path.join(HERE, name), "w", newline="") as f:
        cw = csv.DictWriter(f, fieldnames=fields); cw.writeheader(); cw.writerows(data)
w("season_ranges_resolved_from_held.csv", resolved)
w("season_ranges_held_back_remaining.csv", still)
print("resolved", len(resolved), "| still held", len(still))
for r in resolved: print(" +", r["schedule_id"], r["show_title"], r["proposed_start"], r["proposed_end"], r["episodes_in_season"])
for r in still:
    if r["reason"].startswith(("range >", "proposed start")): print(" -", r["schedule_id"], r["show_title"], "|", r["reason"][:110])

# ---- Summer extensions (item 2) --------------------------------------------------------------
# For every narrowed row (the applied set already in the DB + the rows resolved above) that ends
# Jan-Aug, extend the end to Aug 31 -- but only when (a) the same show has a LATER row on the same
# network (a true gap between seasons, not the tail after a series ends) and (b) no other row in
# the slot already covers the next day. The extension stops the day before the next row in the slot.
cur.execute("select schedule_id, network, day_of_week, air_time::text, show_id, air_date, "
            "effective_start_date, effective_end_date from schedules")
db = {r[0]: dict(net=r[1], dow=r[2], t=r[3], show=r[4], yr=r[5], s=r[6], e=r[7]) for r in cur.fetchall()}
for r in resolved:  # overlay the proposed (not yet applied) resolutions
    db[int(r["schedule_id"])].update(s=D(r["proposed_start"]), e=D(r["proposed_end"]))
default = lambda x: x["s"] == d.date(x["yr"], 1, 1) and x["e"] == d.date(x["yr"], 12, 31)
slots = {}
for sid_row, x in db.items():
    slots.setdefault((x["net"], x["dow"], x["t"]), []).append(sid_row)
ext = []
for sid_row, x in db.items():
    if default(x) or x["e"].month > 8 or x["show"] is None:
        continue
    if not any(o["show"] == x["show"] and o["net"] == x["net"] and o["s"] > x["e"] for o in db.values()):
        continue
    cap = d.date(x["e"].year, 8, 31)
    nxt = x["e"] + d.timedelta(days=1)
    others = [db[o] for o in slots[(x["net"], x["dow"], x["t"])] if o != sid_row]
    if any(o["s"] <= nxt <= o["e"] for o in others):
        continue
    later = [o["s"] for o in others if o["s"] > x["e"]]
    new_end = min([cap] + [s - d.timedelta(days=1) for s in later])
    if new_end > x["e"]:
        ext.append((sid_row, x["e"], new_end))
with open(os.path.join(HERE, "summer_extensions.csv"), "w", newline="") as f:
    cw = csv.writer(f); cw.writerow(["schedule_id", "show_title", "network", "current_end", "extended_end"])
    for sid_row, old, new in ext:
        cur.execute("select show_title, network from schedules where schedule_id=%s", (sid_row,))
        t, nw = cur.fetchone(); cw.writerow([sid_row, t, nw, old, new])

def sql(name, stmts):
    with open(os.path.join(HERE, name), "w") as f:
        f.write("-- PROPOSAL ONLY: not applied.\nBEGIN;\n" + "".join(stmts) + "COMMIT;\n")
U = "UPDATE schedules SET effective_start_date='%s', effective_end_date='%s' WHERE schedule_id=%s;\n"
sql("apply_resolved_held.sql", [U % (r["proposed_start"], r["proposed_end"], r["schedule_id"]) for r in resolved])
sql("rollback_resolved_held.sql", [U % (r["current_start"], r["current_end"], r["schedule_id"]) for r in resolved])
E = "UPDATE schedules SET effective_end_date='%s' WHERE schedule_id=%s;\n"
sql("apply_summer_extensions.sql", [E % (new, sid_row) for sid_row, old, new in ext])
sql("rollback_summer_extensions.sql", [E % (old, sid_row) for sid_row, old, new in ext])
print("summer extensions:", len(ext))
