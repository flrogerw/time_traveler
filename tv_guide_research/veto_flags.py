"""Veto-only guide check of show_syndication_eligibility daypart flags. READ-ONLY: writes a CSV.

Only ever proposes turning a flag that is currently ON to OFF -- it can never turn one ON, so it cannot
repeat the rejected guide-rules behaviour (Gunsmoke/Mannix on Saturday morning). Each row has a tier so
they can be accepted or rejected separately; a show's own guide listings in the slot always protect it.

  A  the show has >= WELL_SEEN guide listings overall but none in this slot (show-specific evidence)
  B  the show's PRIMARY genre (first token of show_genre) is < VETO_SHARE of the slot's tagged series
  C  judgment call: slot is >= KIDS_DOMINATED children's programming (Saturday 7-12) and the show's
     primary genre is neither children's, educational nor family

    python -m tv_guide_research.veto_flags [--out tv_guide_research/proposed_flag_vetoes.csv]
"""
import argparse
import collections
import csv
import json
import os

import psycopg
from dotenv import load_dotenv

from tv_guide_research.guide_rules import FLAG_SLOT, TOKEN_CLASS, WELL_SEEN, merged_slot_shares

load_dotenv()
VETO_SHARE = 0.02
KIDS_DOMINATED = 0.70
KIDS_OK = {"cartoons/children", "education", "FAMILY"}


def primary_class(genre, mapped):
    if mapped == "kids":
        return "cartoons/children"
    toks = [t.strip() for t in (genre or "").lower().split(",") if t.strip()]
    return TOKEN_CLASS.get(toks[0]) if toks else TOKEN_CLASS.get(mapped)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--evidence", default="tv_guide_research/guide_evidence.json")
    ap.add_argument("--out", default="tv_guide_research/proposed_flag_vetoes.csv")
    args = ap.parse_args()
    evidence = json.load(open(args.evidence))
    shares = merged_slot_shares(evidence)
    conn = psycopg.connect(host=os.environ["DB_HOST"], port=os.environ.get("DB_PORT", "5432"), user=os.environ["DB_USER"],
                           password=os.environ["DB_PASSWORD"], dbname=os.environ.get("DATABASE") or os.environ["DB_NAME"])
    conn.read_only = True
    cur = conn.cursor()
    flags = list(FLAG_SLOT)
    cur.execute(f"""SELECT s.show_id, s.show_name, s.show_genre, e.mapped_genre, {', '.join('e.' + c for c in flags)}
                    FROM show_syndication_eligibility e JOIN shows s USING (show_id) ORDER BY s.show_name""")
    out, per_flag = [], collections.Counter()
    for row in cur.fetchall():
        show_id, name, genre, mapped = row[:4]
        if mapped == "interstitial":
            continue
        primary = primary_class(genre, mapped)
        direct = evidence["show_slots"].get(str(show_id), {})
        total = sum(direct.values())
        for flag, on in zip(flags, row[4:]):
            slot = FLAG_SLOT[flag]
            if not on or direct.get(slot, 0) > 0:
                continue
            tier = None
            if total >= WELL_SEEN:
                tier = "A"
            elif primary and primary != "FAMILY" and shares[slot].get(primary, 0.0) < VETO_SHARE:
                tier = "B"
            elif primary and primary not in KIDS_OK and shares[slot].get("cartoons/children", 0.0) >= KIDS_DOMINATED:
                tier = "C"
            if tier:
                per_flag[(tier, flag)] += 1
                out.append([tier, show_id, name.strip(), genre, mapped, flag, slot, primary,
                            f"{shares[slot].get(primary, 0.0)*100:.1f}%", total])
    with open(args.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["tier", "show_id", "show_name", "show_genre", "mapped_genre", "flag_to_turn_off", "guide_slot",
                    "primary_class", "primary_class_share_in_slot", "show_total_guide_listings"])
        w.writerows(out)
    conn.rollback()
    print(len(out), "proposed vetoes across", len({r[1] for r in out}), "shows")
    for tier in "ABC":
        print(f" tier {tier}: {sum(v for (t, _), v in per_flag.items() if t == tier)} flags, "
              f"{len({r[1] for r in out if r[0] == tier})} shows")
        for flag in flags:
            if per_flag[(tier, flag)]:
                print(f"    {flag:<26}{per_flag[(tier, flag)]}")


if __name__ == "__main__":
    main()
