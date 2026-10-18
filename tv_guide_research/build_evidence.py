"""
Distil the parsed TV Guide listings into a small evidence file the scheduling patch can use without the
130 MB dataset.  READ-ONLY on the DB.

    python -m tv_guide_research.build_evidence path/to/listings_all.jsonl [--out tv_guide_research/guide_evidence.json]

Writes:
  slot_shares      guide era -> coarse slot -> genre class -> share of that slot's tagged SERIES listings
  show_slots       show_id -> coarse slot -> number of that show's own guide listings (title match: exact normalised
                   name, or >= 0.9 fuzzy with the same word count to absorb OCR typos; no substring matching)
  block_mix        guide era -> "SAT|9" / "WKD|16" ... -> our category -> listings   (movies counted as 'movie')
  issues_per_era   how many issues back each era (thin eras are flagged by the consumer)
"""
import argparse
import collections
import difflib
import json
import os
import re

import psycopg
from dotenv import load_dotenv

from tv_guide_research.derive_flags import DUPLICATE_ISSUES, guide_slot, norm, tag_class

load_dotenv()

GUIDE_ERAS = ["1965-66", "1967-68", "1969-70", "1973-77", "1980-83"]
# guide genre class -> our fill_rules category (None = no show of that kind in the library)
CLASS_TO_CATEGORY = {
    "cartoons/children": "kids", "comedy": "family", "adventure/sci-fi": "adventure", "western": "western",
    "drama": "drama", "crime/mystery/suspense": "drama", "variety/music": "variety", "game/quiz": "game",
    "religion": "other", "education": "other", "serial": None,
}
WEEKDAY_BLOCK_STARTS = [6, 8, 10, 12, 14, 16, 18]
WEEKEND_BLOCK_STARTS = [7, 9, 12, 14, 17]
GENERIC_TITLES = {"movie", "special", "local", "film", "news", "weather", "sports"}


def guide_era(year):
    if year <= 1966:
        return "1965-66"
    if year <= 1968:
        return "1967-68"
    if year <= 1970:
        return "1969-70"
    if year <= 1977:
        return "1973-77"
    if year <= 1983:
        return "1980-83"
    return None  # 1988 is a single noisy issue


def block_key(day, minutes):
    if minutes is None:
        return None
    hour = minutes / 60
    if day in ("saturday", "sunday"):
        starts, tag = WEEKEND_BLOCK_STARTS, "SAT" if day == "saturday" else "SUN"
        ends = [9, 12, 14, 17, 19]
    else:
        starts, tag = WEEKDAY_BLOCK_STARTS, "WKD"
        ends = [8, 10, 12, 14, 16, 18, 19]
    for s, e in zip(starts, ends):
        if s <= hour < e:
            return f"{tag}|{s}"
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("listings")
    ap.add_argument("--out", default="tv_guide_research/guide_evidence.json")
    args = ap.parse_args()

    conn = psycopg.connect(host=os.environ["DB_HOST"], port=os.environ.get("DB_PORT", "5432"), user=os.environ["DB_USER"],
                           password=os.environ["DB_PASSWORD"], dbname=os.environ.get("DATABASE") or os.environ["DB_NAME"])
    conn.read_only = True
    cur = conn.cursor()
    cur.execute("SELECT show_id, show_name FROM shows")
    by_name = {}
    for show_id, name in cur.fetchall():
        n = norm(name)
        if len(n) >= 5 and n not in GENERIC_TITLES:
            by_name.setdefault(n, show_id)
    conn.rollback()
    names = sorted(by_name)

    fuzzy_cache = {}

    def show_for(title):
        if title in by_name:
            return by_name[title]
        if title in fuzzy_cache:
            return fuzzy_cache[title]
        found = None
        if len(title) >= 6 and title not in GENERIC_TITLES:
            cands = [n for n in names if abs(len(n) - len(title)) <= 2 and len(n.split()) == len(title.split())]
            m = difflib.get_close_matches(title, cands, n=1, cutoff=0.9)
            found = by_name[m[0]] if m else None
        fuzzy_cache[title] = found
        return found

    slot_counts = collections.defaultdict(lambda: collections.defaultdict(collections.Counter))
    show_slots = collections.defaultdict(collections.Counter)
    block_mix = collections.defaultdict(lambda: collections.defaultdict(collections.Counter))
    issues = collections.defaultdict(set)

    for line in open(args.listings):
        r = json.loads(line)
        if r["issue"] in DUPLICATE_ISSUES:
            continue
        era = guide_era(r["year"])
        if era is None:
            continue
        issues[era].add(r["issue"])
        title = norm(r["title"])
        is_movie = r["title"].upper().startswith(("MOVIE", "FILM"))
        slot = guide_slot(r["day"], r["time"])
        cls = tag_class(r["tag"])
        if slot and cls and not is_movie:
            slot_counts[era][slot][cls] += 1
        if slot and not is_movie:
            sid = show_for(title)
            if sid is not None:
                show_slots[sid][slot] += 1
        bk = block_key(r["day"], r["time"])
        if bk:
            if is_movie:
                block_mix[era][bk]["movie"] += 1
            elif cls and CLASS_TO_CATEGORY.get(cls):
                block_mix[era][bk][CLASS_TO_CATEGORY[cls]] += 1

    slot_shares = {}
    for era, slots in slot_counts.items():
        slot_shares[era] = {}
        for slot, counts in slots.items():
            total = sum(counts.values())
            slot_shares[era][slot] = {c: round(n / total, 4) for c, n in counts.items()} if total else {}

    out = {
        "issues_per_era": {e: len(v) for e, v in issues.items()},
        "slot_shares": slot_shares,
        "show_slots": {str(k): dict(v) for k, v in show_slots.items()},
        "block_mix": {e: {b: dict(c) for b, c in blocks.items()} for e, blocks in block_mix.items()},
    }
    with open(args.out, "w") as f:
        json.dump(out, f, indent=0, sort_keys=True)
    print(f"wrote {args.out}: {os.path.getsize(args.out) // 1024} KB; issues per era {out['issues_per_era']}; "
          f"{len(out['show_slots'])} of our shows found in the guides")


if __name__ == "__main__":
    main()
