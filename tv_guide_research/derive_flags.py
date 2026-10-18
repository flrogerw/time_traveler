"""
Diff the guide-derived daypart flags against show_syndication_eligibility. READ-ONLY: prints a plan.

    python -m tv_guide_research.derive_flags [--evidence tv_guide_research/guide_evidence.json] [--detail saturday_morning]

The rules live in guide_rules.py (see TV_GUIDE_RESEARCH.md for the evidence behind them).
"""
import argparse
import collections
import json
import os

import psycopg
from dotenv import load_dotenv

from tv_guide_research.guide_rules import FLAG_SLOT, derive_show_flags, merged_slot_shares

load_dotenv()
GUIDE_SLOTS = ["SAT 07-12", "SAT 12-19", "SUN 07-12", "SUN 12-19", "WKD 06-10", "WKD 10-16", "WKD 16-19"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--evidence", default="tv_guide_research/guide_evidence.json")
    ap.add_argument("--detail")
    args = ap.parse_args()
    evidence = json.load(open(args.evidence))
    shares = merged_slot_shares(evidence)
    print("genre share of each guide slot (series only):")
    for slot in GUIDE_SLOTS:
        print(f"  {slot}: " + ", ".join(f"{c} {v*100:.0f}%" for c, v in sorted(shares[slot].items(), key=lambda kv: -kv[1]) if v >= 0.02))

    conn = psycopg.connect(host=os.environ["DB_HOST"], port=os.environ.get("DB_PORT", "5432"), user=os.environ["DB_USER"],
                           password=os.environ["DB_PASSWORD"], dbname=os.environ.get("DATABASE") or os.environ["DB_NAME"])
    conn.read_only = True
    cur = conn.cursor()
    flags = list(FLAG_SLOT)
    cur.execute(f"""SELECT s.show_id, s.show_name, s.show_genre, e.mapped_genre, {', '.join('e.' + c for c in flags)}
                    FROM show_syndication_eligibility e JOIN shows s USING (show_id) ORDER BY s.show_name""")
    off, on = collections.Counter(), collections.Counter()
    changed = collections.defaultdict(lambda: {"off": [], "on": []})
    rows = cur.fetchall()
    for row in rows:
        show_id, name, genre, mapped = row[:4]
        if mapped == "interstitial":
            continue
        current = dict(zip(flags, row[4:]))
        for flag, verdict in derive_show_flags(evidence, show_id, genre, mapped, shares).items():
            if verdict != current[flag]:
                (on if verdict else off)[flag] += 1
                changed[flag]["on" if verdict else "off"].append(name.strip())
    print(f"\n{len(rows)} shows\n{'flag':<26}{'turn OFF':>9}{'turn ON':>9}")
    for flag in flags:
        print(f"{flag:<26}{off[flag]:>9}{on[flag]:>9}")
    if args.detail:
        d = changed[args.detail]
        print(f"\n{args.detail}: OFF ({len(d['off'])}): {', '.join(d['off'])}")
        print(f"\n{args.detail}: ON ({len(d['on'])}): {', '.join(d['on'])}")
    conn.rollback()


if __name__ == "__main__":
    main()
