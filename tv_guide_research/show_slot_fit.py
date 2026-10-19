"""Which of four coarse slots each show fits (weekend/weekday x morning/afternoon). READ-ONLY: writes a CSV.

Only shows with shows.show_is_syndicated = TRUE are listed.
A show fits a slot when any of its daypart flags in show_syndication_eligibility is ON. The `*_after_vetoes`
columns apply tier B and C of proposed_flag_vetoes.csv (tier A is excluded as the weakest evidence).
    python -m tv_guide_research.show_slot_fit [--out tv_guide_research/show_slot_fit.csv]
"""
import argparse, csv, os
import psycopg
from dotenv import load_dotenv

load_dotenv()
SLOTS = {
    "weekend_morning": ["saturday_early_morning", "saturday_morning", "sunday_early_morning", "sunday_morning"],
    "weekend_afternoon": ["saturday_midday", "saturday_afternoon", "saturday_early_evening",
                          "sunday_midday", "sunday_afternoon", "sunday_early_evening"],
    "weekday_morning": ["weekday_early_morning", "weekday_morning", "weekday_late_morning"],
    "weekday_afternoon": ["weekday_noon", "weekday_early_afternoon", "weekday_after_school", "weekday_early_evening"],
}
FLAGS = [f for fl in SLOTS.values() for f in fl]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="tv_guide_research/show_slot_fit.csv")
    ap.add_argument("--vetoes", default="tv_guide_research/proposed_flag_vetoes.csv")
    args = ap.parse_args()
    vetoed = {(int(r["show_id"]), r["flag_to_turn_off"]) for r in csv.DictReader(open(args.vetoes)) if r["tier"] in "BC"}
    conn = psycopg.connect(host=os.environ["DB_HOST"], port=os.environ.get("DB_PORT", "5432"), user=os.environ["DB_USER"],
                           password=os.environ["DB_PASSWORD"], dbname=os.environ.get("DATABASE") or os.environ["DB_NAME"])
    conn.read_only = True
    cur = conn.cursor()
    cur.execute(f"""SELECT s.show_id, s.show_name, s.show_genre, e.mapped_genre, e.eligible_year_start, e.eligible_year_end,
                    {', '.join('e.' + f for f in FLAGS)}
                    FROM show_syndication_eligibility e JOIN shows s USING (show_id)
                    WHERE s.show_is_syndicated IS TRUE ORDER BY s.show_name""")
    rows = []
    for r in cur.fetchall():
        show_id, name, genre, mapped, y0, y1 = r[:6]
        on = dict(zip(FLAGS, r[6:]))
        cur_fit = {k: any(on[f] for f in fl) for k, fl in SLOTS.items()}
        new_fit = {k: any(on[f] and (show_id, f) not in vetoed for f in fl) for k, fl in SLOTS.items()}
        rows.append([show_id, name.strip(), genre, mapped, y0, y1]
                    + [("yes" if cur_fit[k] else "") for k in SLOTS]
                    + [("yes" if new_fit[k] else "") for k in SLOTS])
    with open(args.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["show_id", "show_name", "show_genre", "mapped_genre", "eligible_year_start", "eligible_year_end"]
                   + list(SLOTS) + [k + "_after_vetoes" for k in SLOTS])
        w.writerows(rows)
    conn.rollback()
    print(len(rows), "shows ->", args.out)
    for i, k in enumerate(SLOTS):
        print(f"  {k:<18} current {sum(1 for r in rows if r[6+i]):>3}   after vetoes {sum(1 for r in rows if r[10+i]):>3}")


if __name__ == "__main__":
    main()
