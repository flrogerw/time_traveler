#!/usr/bin/env python3
"""
Audit + fix for `schedules` rows with a NULL show_id where a matching show actually exists in
`shows` -- i.e. the historical seed row was already correct, it just never got linked to the
real ingested content. Confirmed by title-normalization matching plus manual verification of
each candidate's network/year against the real show's airdate range (see the conversation this
came from for the row-by-row reasoning).

Out of 1,415 distinct unlinked titles, only 9 rows had a confident, verified match:
  - 5 were an exact match once normalized (case/whitespace only): Laramie, Gloria,
    Charles in Charge, My Little Margie, Nanny and the Professor.
  - 2 were a near-exact match: "Mayberry R.F.D." -> Mayberry RFD (punctuation only), "Nancy
    Derringer" -> Yancy Derringer (a data-entry typo -- confirmed by matching network/year: CBS
    1958, exactly Yancy Derringer's real run).
  - 2 were the same show under a different season-year label: "Dragnet 1968"/"Dragnet 1970" ->
    Dragnet 1967 (show_id 120, whose real airdate range is 1967-01-12 to 1970-04-16, covering
    both). "Dragnet 1969" was already correctly linked to the same show_id.

Two near-misses were deliberately NOT linked, despite a high string-similarity score:
  - "The New Bill Cosby Show" vs. "Bill Cosby Show" (show_id 353) -- these are two genuinely
    different real productions (the 1969-71 NBC sitcom vs. the 1972-73 CBS variety show), not a
    naming variant of the same show.
  - "Specials" vs. "Special" (show_id 328) -- "Specials" is a generic placeholder/bucket label
    in the schedule data (alongside things like "Miami Vice / Specials and Movies"), not a
    specific named show.
  - The 6 bare "Dragnet" rows (NBC, 1953-1958) are the original 1950s Dragnet, not the 1967
    revival (show_id 120 only covers 1967-1970) -- correctly left unlinked since the original
    series isn't in `shows` at all.

The other ~1,400 unlinked titles have no plausible match in `shows` -- they're real historical
programs (news, sports, local blocks, unowned shows) that were never ingested into this library,
which is expected and fine; `schedules` intentionally retains them with show_id NULL per
CLAUDE_WEEKEND_SCHEDULING_HANDOFF.md's "retain the historical row, do not discard" rule.

Idempotent: every UPDATE is scoped to `show_id IS NULL`, so re-running is a no-op.
"""
import os

import psycopg
from dotenv import load_dotenv

load_dotenv()

# show_title (exact match) -> show_id
LINKS = {
    "Laramie": 400,
    "Gloria": 398,
    "Charles in Charge": 393,
    "My Little Margie": 406,
    "Nanny and the Professor": 399,
    "Mayberry R.F.D.": 397,
    "Nancy Derringer": 68,   # data-entry typo for Yancy Derringer -- confirmed via CBS/1958 match
    "Dragnet 1968": 120,
    "Dragnet 1970": 120,
}


def get_db_connection():
    return psycopg.connect(
        host=os.environ["DB_HOST"],
        port=os.environ.get("DB_PORT", "5432"),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        dbname=os.environ.get("DATABASE") or os.environ["DB_NAME"],
    )


def main():
    conn = get_db_connection()
    cur = conn.cursor()
    for show_title, show_id in LINKS.items():
        cur.execute(
            "UPDATE schedules SET show_id = %s WHERE show_title = %s AND show_id IS NULL",
            (show_id, show_title),
        )
        print(f"{show_title!r} -> show_id {show_id}: {cur.rowcount} row(s) linked")
    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
