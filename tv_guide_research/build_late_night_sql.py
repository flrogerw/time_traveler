"""Build the late-night migration from the manual slot review. READ-ONLY: writes add_late_night.sql and
rollback_late_night.sql, never touches the DB.

  1. ALTER TABLE show_syndication_eligibility ADD COLUMN late_night (default FALSE)
  2. late_night = TRUE for the syndicated shows picked for late night (slot_late_night_selections.csv)
  3. generic (all-era) fill_rules for every channel that has fill rules: 23:00-23:59:59 and 00:00-06:00, all days,
     category weights proportional to how many picked shows have each mapped_genre
fill_rules matches start_time <= t < end_time on one day, so the window that crosses midnight is two rows.

    python -m tv_guide_research.build_late_night_sql
"""
import collections, csv, os
import psycopg
from dotenv import load_dotenv

load_dotenv()
HERE = os.path.dirname(os.path.abspath(__file__))


def main():
    picked = [int(r["show_id"]) for r in csv.DictReader(open(os.path.join(HERE, "slot_late_night_selections.csv")))]
    conn = psycopg.connect(host=os.environ["DB_HOST"], port=os.environ.get("DB_PORT", "5432"), user=os.environ["DB_USER"],
                           password=os.environ["DB_PASSWORD"], dbname=os.environ.get("DATABASE") or os.environ["DB_NAME"])
    conn.read_only = True
    cur = conn.cursor()
    cur.execute("""SELECT e.show_id, e.mapped_genre FROM show_syndication_eligibility e JOIN shows s USING (show_id)
                   WHERE s.show_is_syndicated AND e.show_id = ANY(%s) ORDER BY e.show_id""", (picked,))
    shows = cur.fetchall()
    cur.execute("SELECT DISTINCT channel_id FROM fill_rules ORDER BY 1")
    channels = [r[0] for r in cur.fetchall()]
    conn.rollback()
    counts = collections.Counter(g for _, g in shows if g)
    total = sum(counts.values())
    weights = {g: round(n / total, 4) for g, n in counts.most_common()}
    rows = [f"({ch}, '{start}', '{end}', '{cat}', {w}, ARRAY[0,1,2,3,4,5,6], NULL, NULL)"
            for ch in channels for start, end in (("23:00", "23:59:59"), ("00:00", "06:00")) for cat, w in weights.items()]
    with open(os.path.join(HERE, "add_late_night.sql"), "w") as f:
        f.write("-- PROPOSAL ONLY: not applied. Built by build_late_night_sql.py\nBEGIN;\n"
                "ALTER TABLE show_syndication_eligibility ADD COLUMN IF NOT EXISTS late_night boolean NOT NULL DEFAULT FALSE;\n"
                f"UPDATE show_syndication_eligibility SET late_night = TRUE WHERE show_id IN ({', '.join(str(i) for i, _ in shows)});\n"
                "INSERT INTO fill_rules (channel_id, start_time, end_time, category, weight, dows, year_start, year_end) VALUES\n  "
                + ",\n  ".join(rows) + ";\nCOMMIT;\n")
    with open(os.path.join(HERE, "rollback_late_night.sql"), "w") as f:
        f.write("BEGIN;\nDELETE FROM fill_rules WHERE year_start IS NULL AND start_time IN ('23:00', '00:00')\n"
                "  AND end_time IN ('23:59:59', '06:00');\nALTER TABLE show_syndication_eligibility DROP COLUMN IF EXISTS late_night;\nCOMMIT;\n")
    print(f"{len(shows)} shows late_night, {len(channels)} channels, {len(rows)} fill_rules rows, weights {weights}")


if __name__ == "__main__":
    main()
