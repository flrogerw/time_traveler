"""
Dry-run of the proposed syndicated "fill ladder" for weekday (06:00-19:00) and weekend
(07:00-19:00) dayparts. Read-only: never writes to
the DB (the connection is opened read-only) and never touches schedule_template.

Simulates one replication year at a time: every syndicated channel, weekday strip (Mon-Fri) plus Saturday and Sunday, on a
30-minute grid across each day's scope. A weekday slot is resolved once and applies to all five
weekdays (Schedules.insert_schedule_template does this for SYN channels), so it counts as one strip. Each show is licensed to one slot per
replication year (mirrors SyndicatedScheduler.get_eligible_shows's exclusivity), so pools run dry
as the simulated week fills (weekday and weekend slots share one pool) -- exactly the situation the ladder is meant to handle.

Ladder, accuracy first (a real in-era show is repeated before any out-of-era show is used):
  A. fixed slot / existing resolver behavior (weighted category, drop empties, re-pick)
  B. same category, adjacent daypart's flag, unused show, in era
  C. related category (RELATED below), same daypart, unused show, in era
  D1. rerun on the SAME channel: an in-era show already on this channel (target category first,
      then related), least-placed first, never the show in the immediately preceding slot --
      stations rerun their own shows, and this keeps one-station-per-market licensing intact
  D2. repeat from ANOTHER channel (breaks the licensing exclusivity), least-used first, still
      never back-to-back
  D3. last resort: allow back-to-back
  X. unresolved -> would fall to the network genre-bias engine

Usage:
  python dryrun_fill_ladder.py [--years 1965 1970 ...] [--seed 1] [--detail-year 1970] [--scope all|weekday|weekend]
"""
import argparse
import collections
import os
import random
from datetime import time

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row

from playlists.classes.SyndicatedScheduler import (
    SATURDAY_DOW,
    SUNDAY_DOW,
    WEEKDAY_DAYPART_BLOCKS,
    WEEKEND_DAYPART_BLOCKS,
    SyndicatedScheduler,
    daypart_column_for,
)

load_dotenv()

# Proposed substitutions for tier C -- a starting point to tune, not a historical claim.
RELATED = {
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

REPEAT_CATEGORY_PENALTY = 2
ADJACENT_DAYPART_PENALTY = 3
ALLOW_ADJACENT_DAYPART_REPEATS = False  # tried: lowers cross-channel repeats slightly but concentrates reruns on fewer shows
SIM_REPLICATION_YEAR = -2  # no schedule_template rows exist for this, so the DB-side exclusion is a no-op
TIERS = ["A", "B", "C", "D1", "D2", "D3", "X"]


def connect():
    conn = psycopg.connect(
        host=os.environ["DB_HOST"],
        port=os.environ.get("DB_PORT", "5432"),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        dbname=os.environ.get("DATABASE") or os.environ["DB_NAME"],
    )
    conn.read_only = True
    return conn


WEEKDAY_DOWS = [0, 1, 2, 3, 4]


def slot_times(dow):
    t = []
    for hour in range(6 if dow in WEEKDAY_DOWS else 7, 19):
        t += [time(hour, 0), time(hour, 30)]
    return t


def adjacent_columns(dow, start_time):
    if dow in WEEKDAY_DOWS:
        names = [name for _, _, name in WEEKDAY_DAYPART_BLOCKS]
    else:
        prefix = "saturday_" if dow == SATURDAY_DOW else "sunday_"
        names = [f"{prefix}{name}" for _, _, name in WEEKEND_DAYPART_BLOCKS]
    here = daypart_column_for(start_time, dow)
    i = names.index(here)
    return [names[j] for j in (i - 1, i + 1) if 0 <= j < len(names)]


_ELIG_CACHE = {}


def eligible_in_era(cur, category, column, year, exclude):
    key = (category, column, year)
    if key not in _ELIG_CACHE:
        _ELIG_CACHE[key] = _query_eligible(cur, category, column, year)
    return [sid for sid in _ELIG_CACHE[key] if sid not in exclude]


def _query_eligible(cur, category, column, year):
    cur.execute(
        f"""
        SELECT show_id FROM show_syndication_eligibility
        WHERE mapped_genre = %s AND {column} = TRUE
          AND eligible_year_start IS NOT NULL AND eligible_year_end IS NOT NULL
          AND %s BETWEEN eligible_year_start AND eligible_year_end
        ORDER BY show_id
        """,
        (category, year),
    )
    return [r["show_id"] for r in cur.fetchall()]


def resolve(sched, cur, rng, channel_id, year, dow, start, used, use_count, state):
    fixed = sched.get_fixed_slot(channel_id, year, dow, start)
    if fixed is not None:
        return fixed, "A", "fixed"

    column = daypart_column_for(start, dow)
    rules = sorted(sched.get_fill_rules(channel_id, year, dow, start), key=lambda r: (r['category'], r['weight']))
    categories = sorted({r["category"] for r in rules}, key=lambda c: (-sum(r["weight"] for r in rules if r["category"] == c), c))

    remaining = list(rules)
    while remaining:
        cat = sched.choose_category(remaining)
        pool = eligible_in_era(cur, cat, column, year, used)
        if pool:
            return rng.choice(pool), "A", cat
        remaining = [r for r in remaining if r["category"] != cat]

    for cat in categories:  # B: adjacent daypart
        for adj in adjacent_columns(dow, start):
            pool = eligible_in_era(cur, cat, adj, year, used)
            if pool:
                return rng.choice(pool), "B", cat

    for cat in categories:  # C: related category, same daypart
        for rel in RELATED.get(cat, []):
            pool = eligible_in_era(cur, rel, column, year, used)
            if pool:
                return rng.choice(pool), "C", f"{cat}->{rel}"

    prev = state["prev"].get(channel_id)
    chan = state["chan"][channel_id]
    ordered = []
    for cat in categories:
        for c in [cat] + RELATED.get(cat, []):
            if c not in ordered:
                ordered.append(c)
    # Candidates: (category, daypart column, penalty). Same daypart costs REPEAT_CATEGORY_PENALTY per
    # category step; a show flagged only for an adjacent daypart is allowed at a flat extra cost.
    cols = [(column, 0)] + ([(a, ADJACENT_DAYPART_PENALTY) for a in adjacent_columns(dow, start)] if ALLOW_ADJACENT_DAYPART_REPEATS else [])
    pools = {}
    for pos, c in enumerate(ordered):
        for col, extra in cols:
            pools[(c, col, REPEAT_CATEGORY_PENALTY * pos + extra)] = eligible_in_era(cur, c, col, year, set())

    # Balance repeats across categories: each extra placement costs 1, each step further from the
    # slot's top category costs REPEAT_CATEGORY_PENALTY, so one thin category can't hog a channel.
    def pick(filter_fn, key_fn, tier):
        scored = [
            (key_fn(sid) + pen, sid, c)
            for (c, col, pen), ids in pools.items()
            for sid in ids
            if filter_fn(sid)
        ]
        if scored:
            best = min(sc[0] for sc in scored)
            _, sid, c = rng.choice([sc for sc in scored if sc[0] == best])
            return sid, tier, c

    got = (
        pick(lambda s: s in chan and s != prev, lambda s: chan[s], "D1")
        or pick(lambda s: s != prev, lambda s: use_count[s], "D2")
        or pick(lambda s: True, lambda s: use_count[s], "D3")
    )
    if got:
        return got

    return None, "X", ",".join(categories) or "(no rules)"


def simulate_year(conn, year, seed, channels, dows):
    rng = random.Random(seed * 10000 + year)
    import random as _r
    _r.seed(seed * 10000 + year)  # SyndicatedScheduler.choose_category uses the module-level RNG
    sched = SyndicatedScheduler(conn)
    cur = conn.cursor(row_factory=dict_row)
    used, use_count = set(), collections.Counter()
    state = {"prev": {}, "chan": collections.defaultdict(collections.Counter)}
    tiers = collections.Counter()
    by_daytype = collections.defaultdict(collections.Counter)
    by_block = collections.defaultdict(collections.Counter)
    misses = collections.Counter()
    for channel_id in channels:
        for dow in dows:
            state["prev"].pop(channel_id, None)
            for start in slot_times(dow):
                show_id, tier, detail = resolve(sched, cur, rng, channel_id, year, dow, start, used, use_count, state)
                tiers[tier] += 1
                by_daytype["weekday" if dow in WEEKDAY_DOWS else "weekend"][tier] += 1
                col = daypart_column_for(start, dow)
                by_block[col][tier] += 1
                if show_id is not None:
                    used.add(show_id)
                    use_count[show_id] += 1
                    state["chan"][channel_id][show_id] += 1
                    state["prev"][channel_id] = show_id
                if tier in ("C", "D1", "D2", "D3", "X"):
                    misses[(col, tier, detail)] += 1
    stats = {"distinct": len(use_count), "max_repeat": max(use_count.values(), default=0),
             "cross_channel": sum(1 for sid in use_count if sum(1 for ch in state["chan"] if sid in state["chan"][ch]) > 1)}
    return tiers, by_daytype, by_block, misses, stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", nargs="*", type=int, default=list(range(1960, 1990)))
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--penalty", type=int, default=None, help="REPEAT_CATEGORY_PENALTY override")
    ap.add_argument("--adjacent", action="store_true", help="allow adjacent-daypart reruns")
    ap.add_argument("--seeds", type=int, nargs="*", help="run several seeds and print mean/max of the max-repeat column")
    ap.add_argument("--scope", choices=["all", "weekday", "weekend"], default="all")
    ap.add_argument("--detail-year", type=int, help="print per-block tiers and top fall-throughs for this year")
    args = ap.parse_args()

    global REPEAT_CATEGORY_PENALTY, ALLOW_ADJACENT_DAYPART_REPEATS
    if args.penalty is not None:
        REPEAT_CATEGORY_PENALTY = args.penalty
    ALLOW_ADJACENT_DAYPART_REPEATS = args.adjacent or ALLOW_ADJACENT_DAYPART_REPEATS
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT channel_id FROM channels WHERE channel_type = 'syndicated' ORDER BY channel_id")
    channels = [r[0] for r in cur.fetchall()]
    dows = {"all": [0, SATURDAY_DOW, SUNDAY_DOW], "weekday": [0], "weekend": [SATURDAY_DOW, SUNDAY_DOW]}[args.scope]
    per_channel = sum(len(slot_times(d)) for d in dows)
    if args.seeds:
        print(f"penalty={REPEAT_CATEGORY_PENALTY} adjacent={ALLOW_ADJACENT_DAYPART_REPEATS}")
        for year in args.years:
            runs = [simulate_year(conn, year, sd, channels, dows) for sd in args.seeds]
            d1 = sum(r[0]['D1'] for r in runs) / len(runs); d2 = sum(r[0]['D2'] for r in runs) / len(runs)
            mx = [r[4]['max_repeat'] for r in runs]
            print(f"  {year}: D1 {d1:.0f}  D2 {d2:.0f}  max reps mean {sum(mx)/len(mx):.1f} (min {min(mx)}, max {max(mx)})")
        conn.rollback()
        return
    print(f"{len(channels)} syndicated channels, {per_channel} slots each ({args.scope}; weekdays count as one Mon-Fri strip), seed={args.seed}\n")
    print("year  slots   A(today)  B   C  D1(rerun-same-ch) D2(cross-ch) D3(b2b)  X | distinct shows | max reps/show | shows on >1 channel")
    for year in args.years:
        tiers, by_daytype, by_block, misses, stats = simulate_year(conn, year, args.seed, channels, dows)
        n = sum(tiers.values())
        past = n - tiers["A"]
        print(f"{year}  {n:>5}  {tiers['A']:>8} {tiers['B']:>3} {tiers['C']:>3} {tiers['D1']:>12} {tiers['D2']:>12} {tiers['D3']:>8} {tiers['X']:>3} | {stats['distinct']:>14} | {stats['max_repeat']:>13} | {stats['cross_channel']:>6}")
        if args.detail_year == year:
            print("\n  per block (A/B/C/D/X):")
            for col, c in by_block.items():
                print(f"    {col:<26}", "/".join(str(c[t]) for t in TIERS))
            print("  top fall-throughs (block, tier, category):")
            for (col, tier, detail), v in misses.most_common(15):
                print(f"    {v:>3}  {col} {tier} {detail}")
            print()
    conn.rollback()


if __name__ == "__main__":
    main()
