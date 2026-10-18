"""
Which kids' shows do the guides show a lot that the library does not have? READ-ONLY.

    python -m tv_guide_research.missing_kids_shows path/to/listings_all.jsonl [--top 70]

Ranks guide titles by how often they aired in the kids' dayparts (Saturday 7-12, weekday 6-10 and 16-19, Sunday
7-12) and drops anything that matches a show already in `shows` (exact, contained, or >= 0.86 fuzzy match, which
absorbs OCR typos like "DENNS THE MENACE"). Local station kids' hosts (Cap'n Delta, Jack's Place...) are listed too:
they are not on DVD, so treat them as inspiration for filler, not ingest targets.
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
KIDS_SLOTS = {"SAT 07-12", "WKD 06-10", "WKD 16-19", "SUN 07-12"}
NOISE = re.compile(r"^(news|weather|movie|film|sports|baseball|football|wrestling|bowling|today|tv guide|.{0,4})$")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("listings")
    ap.add_argument("--top", type=int, default=70)
    args = ap.parse_args()

    conn = psycopg.connect(host=os.environ["DB_HOST"], port=os.environ.get("DB_PORT", "5432"), user=os.environ["DB_USER"],
                           password=os.environ["DB_PASSWORD"], dbname=os.environ.get("DATABASE") or os.environ["DB_NAME"])
    conn.read_only = True
    cur = conn.cursor()
    cur.execute("SELECT show_name FROM shows")
    ours = sorted({norm(n) for (n,) in cur.fetchall() if len(norm(n)) >= 4})
    conn.rollback()

    kids = collections.Counter()      # title -> listings in kids' dayparts
    children_tagged = collections.Counter()
    years = collections.defaultdict(set)
    for line in open(args.listings):
        r = json.loads(line)
        if r["issue"] in DUPLICATE_ISSUES:
            continue
        slot = guide_slot(r["day"], r["time"])
        t = norm(r["title"])
        if slot in KIDS_SLOTS and len(t) >= 5 and not NOISE.match(t):
            kids[t] += 1
            years[t].add(r["year"])
            if tag_class(r["tag"]) == "cartoons/children":
                children_tagged[t] += 1

    def have(title):
        if title in ours:
            return True
        if any(len(o) >= 6 and (o in title or title in o) for o in ours):
            return True
        return bool(difflib.get_close_matches(title, ours, n=1, cutoff=0.86))

    rows = []
    for title, n in kids.most_common(1500):
        if n < 25:
            break
        if not have(title):
            ys = sorted(years[title])
            rows.append((title, n, children_tagged[title], f"{ys[0]}-{ys[-1]}"))
    print(f"{'guide title':<34}{'kids-slot listings':>19}{'tagged cartoon/children':>26}  years seen")
    for title, n, ct, ys in rows[: args.top]:
        print(f"{title.upper():<34}{n:>19}{ct:>26}  {ys}")


if __name__ == "__main__":
    main()
