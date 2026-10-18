"""
Calibrates the era-aware syndicated schedule data to real TV Guide listings (see TV_GUIDE_RESEARCH.md; the
evidence lives in tv_guide_research/guide_evidence.json and the rules in tv_guide_research/guide_rules.py).

Run AFTER setup_syndicated_scheduling.py / setup_weekend_scheduling.py: those scripts delete and re-import every
era-aware fill_rules row and the whole eligibility table, which silently discards this patch. Idempotent -- every
change is a pure function of the evidence file and the shows' own columns. Dry-run by default; --apply to write.

By default ONLY steps 1 and 2 run (the changes you asked for). Steps 3 and 4 are the experimental guide-derived
daypart flags / fill weights: they were tried, put Gunsmoke and other adult shows on Saturday morning, and were rolled
back; they only run with --guide-rules and are not approved.

Steps:
  1. Early off-network syndication: a series with a 6+ season network run entered syndication while still on the
     network (typically after ~5 seasons), so eligible_year_start becomes min(run_end + 1, first_air_year + 5).
     An approximation applied uniformly. Placeholder rows (Movie/Special, years 1900/2300) are skipped.
  2. School House Rock (show_id 150) is an interstitial, not a show: it airs between programs on Saturday mornings
     (1973-1985, ABC and syndicated channels). mapped_genre 'interstitial' keeps every fill rule from giving it a slot.
  3. Daypart flags: each show's 17 flags come from the guides -- its own listings first, then its genres against what
     each slot really carried (Saturday morning is ~76% children's programming; crime series and adult sitcoms
     essentially never aired there). Flags the guides give no basis for are left alone.
  4. Fill weights: each syndicated channel's fill_rules for every (day group, block, era) become the guides' category
     mix for that block and era, restricted to categories that have an eligible show after step 3.
"""
import argparse
import collections
import json
import os

import psycopg
from dotenv import load_dotenv

from tv_guide_research.guide_rules import BLOCK_FLAG, FLAG_SLOT, derive_block_weights, derive_show_flags, merged_slot_shares

load_dotenv()

SCHOOL_HOUSE_ROCK_ID = 150
EVIDENCE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tv_guide_research", "guide_evidence.json")
ALL_DAYPART_FLAGS = list(FLAG_SLOT)


def day_group(dows):
    return "SAT" if 5 in dows else "SUN" if 6 in dows else "WKD"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--guide-rules", action="store_true",
                    help="EXPERIMENTAL steps 3-4 (guide-derived flags and fill weights). Not approved; off by default.")
    args = ap.parse_args()
    evidence = json.load(open(EVIDENCE))
    shares = merged_slot_shares(evidence)

    conn = psycopg.connect(
        host=os.environ["DB_HOST"], port=os.environ.get("DB_PORT", "5432"),
        user=os.environ["DB_USER"], password=os.environ["DB_PASSWORD"],
        dbname=os.environ.get("DATABASE") or os.environ["DB_NAME"],
    )
    cur = conn.cursor()

    # ---- 1. early off-network syndication
    early_where = """e.eligibility_basis = 'conservative_off_network_after_original_run'
             AND EXTRACT(YEAR FROM s.airdate_start) > 1900 AND EXTRACT(YEAR FROM s.airdate_end) < 2100
             AND EXTRACT(YEAR FROM s.airdate_end) - EXTRACT(YEAR FROM s.airdate_start) >= 5
             AND e.eligible_year_start > EXTRACT(YEAR FROM s.airdate_start) + 5"""
    cur.execute(f"SELECT count(*) FROM show_syndication_eligibility e JOIN shows s USING (show_id) WHERE {early_where}")
    early_rows = cur.fetchone()[0]
    if args.apply:
        cur.execute(f"""UPDATE show_syndication_eligibility e
                        SET eligible_year_start = EXTRACT(YEAR FROM s.airdate_start)::int + 5
                        FROM shows s WHERE s.show_id = e.show_id AND {early_where}""")

    # ---- 2. School House Rock = interstitial
    cur.execute("""SELECT count(*) FROM show_syndication_eligibility WHERE show_id = %s
                   AND (mapped_genre <> 'interstitial' OR eligible_year_start IS DISTINCT FROM 1973
                        OR eligible_year_end IS DISTINCT FROM 1985)""", (SCHOOL_HOUSE_ROCK_ID,))
    interstitial_rows = cur.fetchone()[0]
    if args.apply:
        morning = {"saturday_early_morning", "saturday_morning"}
        flag_sets = ", ".join(f"{c} = {'TRUE' if c in morning else 'FALSE'}" for c in ALL_DAYPART_FLAGS)
        cur.execute(f"""UPDATE show_syndication_eligibility
                        SET mapped_genre = 'interstitial', {flag_sets}, eligible_year_start = 1973, eligible_year_end = 1985
                        WHERE show_id = %s""", (SCHOOL_HOUSE_ROCK_ID,))

    # ---- 3. daypart flags from the guides (experimental, --guide-rules only)
    cur.execute(f"""SELECT s.show_id, s.show_name, s.show_genre, e.mapped_genre, {', '.join('e.' + c for c in ALL_DAYPART_FLAGS)}
                    FROM show_syndication_eligibility e JOIN shows s USING (show_id) ORDER BY s.show_name""")
    flags_after = {}          # show_id -> {flag: bool} after this patch
    mapped_of = {}
    turned_on, turned_off = collections.Counter(), collections.Counter()
    pending_updates = []
    for row in cur.fetchall():
        show_id, name, genre, mapped = row[:4]
        current = dict(zip(ALL_DAYPART_FLAGS, row[4:]))
        if show_id == SCHOOL_HOUSE_ROCK_ID:   # step 2 owns this row (its flags are Saturday morning only)
            current = {c: c in ("saturday_early_morning", "saturday_morning") for c in ALL_DAYPART_FLAGS}
            mapped = "interstitial"
        mapped_of[show_id] = mapped
        if mapped == "interstitial":
            flags_after[show_id] = current
            continue
        verdicts = derive_show_flags(evidence, show_id, genre, mapped, shares) if args.guide_rules else {}
        after = {**current, **verdicts}
        flags_after[show_id] = after
        changes = {f: v for f, v in verdicts.items() if v != current[f]}
        for f, v in changes.items():
            (turned_on if v else turned_off)[f] += 1
        if changes:
            pending_updates.append((show_id, changes))
    if args.apply:
        for show_id, changes in pending_updates:
            sets = ", ".join(f"{f} = {'TRUE' if v else 'FALSE'}" for f, v in changes.items())
            cur.execute(f"UPDATE show_syndication_eligibility SET {sets} WHERE show_id = %s", (show_id,))

    # ---- 4. fill weights from the guides' per-block category mix
    eligible_cats = collections.defaultdict(set)
    for show_id, flags in flags_after.items():
        for flag, on in flags.items():
            if on and mapped_of[show_id] not in (None, "interstitial"):
                eligible_cats[flag].add(mapped_of[show_id])
    cur.execute("""SELECT rule_id, channel_id, start_time, end_time, category, weight, dows, year_start, year_end
                   FROM fill_rules WHERE year_start IS NOT NULL ORDER BY channel_id, start_time, year_start""")
    groups = collections.defaultdict(list)
    for rule_id, ch, st, et, cat, w, dows, y0, y1 in cur.fetchall():
        groups[(ch, st, et, tuple(dows), y0, y1)].append((rule_id, cat, float(w)))

    updates = inserts = deletes = untouched = 0
    shown = set()
    first_channel = min(g[0] for g in groups)
    fmt = lambda d: " ".join(f"{k}:{v:.2f}" for k, v in sorted(d.items(), key=lambda kv: -kv[1]))
    for (ch, st, et, dows, y0, y1), rows in sorted(groups.items(), key=lambda kv: (kv[0][0], kv[0][1], kv[0][4])):
        grp = day_group(dows)
        flag = BLOCK_FLAG.get((grp, st.hour))
        new = derive_block_weights(evidence, grp, st.hour, y0, eligible_cats[flag]) if (flag and args.guide_rules) else {}
        old = {c: w for _, c, w in rows}
        if not new:
            untouched += 1
            continue
        if all(abs(new.get(c, 0) - old.get(c, 0)) < 0.0005 for c in set(old) | set(new)):
            continue
        key = (grp, st.hour, y0)
        if ch == first_channel and key not in shown and y0 in (1965, 1975, 1980) and \
                key[:2] in {("SAT", 7), ("SAT", 9), ("SAT", 14), ("SUN", 9), ("WKD", 8), ("WKD", 12), ("WKD", 16)}:
            shown.add(key)
            print(f"{grp} {st.hour:02d}h {y0}\n   old: {fmt(old)}\n   new: {fmt(new)}")
        by_cat = {c: rid for rid, c, _ in rows}
        for c, w in new.items():
            if c in by_cat:
                updates += 1
                if args.apply:
                    cur.execute("UPDATE fill_rules SET weight=%s WHERE rule_id=%s", (w, by_cat[c]))
            else:
                inserts += 1
                if args.apply:
                    cur.execute("""INSERT INTO fill_rules (channel_id, start_time, end_time, category, weight, dows, year_start, year_end)
                                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""", (ch, st, et, c, w, list(dows), y0, y1))
        for c, rid in by_cat.items():
            if c not in new:
                deletes += 1
                if args.apply:
                    cur.execute("DELETE FROM fill_rules WHERE rule_id=%s", (rid,))

    print(f"\nearly syndication: {early_rows} shows get eligible_year_start = first_air_year + 5")
    print(f"interstitial: {interstitial_rows} row(s) (School House Rock) set to mapped_genre='interstitial', 1973-1985")
    print(f"daypart flags: {sum(turned_on.values())} turned ON, {sum(turned_off.values())} turned OFF across {len(pending_updates)} shows")
    for flag in ALL_DAYPART_FLAGS:
        if turned_on[flag] or turned_off[flag]:
            print(f"   {flag:<26} +{turned_on[flag]:<4} -{turned_off[flag]}")
    print(f"fill_rules: {updates} updated, {inserts} inserted, {deletes} deleted ({untouched} rule groups left as they are)")
    if args.apply:
        conn.commit()
        print("APPLIED")
    else:
        conn.rollback()
        print("dry run -- pass --apply to write")


if __name__ == "__main__":
    main()
