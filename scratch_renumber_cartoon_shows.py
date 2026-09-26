#!/usr/bin/env python3
"""
Scratch/one-off script: renumber Dudley Do-Right (51), Pink Panther (73), Wacky Races (91)
so each real segment gets its own sequential episode_number per season, instead of multiple
segments of the same package sharing one number. See conversation/COMMERCIAL_BREAK_AUDIT.md
for context -- no external episode guide exists for these, so numbering is reconstructed
purely from in-DB evidence (the combined package title + which segment a file matches).
"""
import os
import re
import difflib
import json
from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row

load_dotenv(".env")

SHOWS = (51, 73, 91)
PREFIXES = {
    51: ['Dudley_Do_Right_'],
    73: ['The_Pink_Panther_Show_', 'The_Pink_Panther_'],
    91: ['Wacky_Races_'],
}


def norm(s):
    return re.sub(r'[^a-z0-9]+', '', s.lower())


def file_to_segment_guess(show_id, episode_file):
    base = re.sub(r'\.mp4$', '', episode_file, flags=re.I)
    for p in PREFIXES[show_id]:
        if base.startswith(p):
            base = base[len(p):]
            break
    return base.replace('_', ' ')


def match_package(show_id, pkg_rows, segments):
    """One-to-one match: each row gets a distinct segment index (the greedy-per-row approach
    can double-assign the same segment to two rows and leave another segment unmatched)."""
    if len(segments) == 1:
        return [(0, 1.0) for _ in pkg_rows]
    scores = []
    for r in pkg_rows:
        guess = norm(file_to_segment_guess(show_id, r['episode_file']))
        scores.append([difflib.SequenceMatcher(None, guess, norm(seg)).ratio() for seg in segments])
    assigned = [None] * len(pkg_rows)
    used_segs = set()
    pairs = sorted(
        ((scores[i][j], i, j) for i in range(len(pkg_rows)) for j in range(len(segments))),
        reverse=True,
    )
    for score, i, j in pairs:
        if assigned[i] is None and j not in used_segs:
            assigned[i] = (j, score)
            used_segs.add(j)
    return assigned


def main():
    conn = psycopg.connect(
        host=os.environ['DB_HOST'], port=os.environ['DB_PORT'],
        user=os.environ['DB_USER'], password=os.environ['DB_PASSWORD'],
        dbname=os.environ['DB_NAME'],
    )
    cur = conn.cursor(row_factory=dict_row)

    report = {"shows": []}
    updates = []  # (episode_id, new_number)

    for show_id in SHOWS:
        cur.execute("""SELECT episode_id, show_season_number, episode_number, episode_file, episode_title
                       FROM episodes WHERE show_id=%s ORDER BY show_season_number, episode_number, episode_id""",
                    (show_id,))
        rows = cur.fetchall()

        # Group into packages first (season, old_number, title -- title distinguishes a real
        # multi-segment package from an unrelated single-segment row that collided onto the
        # same old episode_number), then do one-to-one segment matching per package.
        packages = {}
        for r in rows:
            key = (r['show_season_number'], r['episode_number'], r['episode_title'])
            packages.setdefault(key, []).append(r)

        enriched = []
        for key, pkg_rows in packages.items():
            segments = [s.strip() for s in key[2].split('/')]
            assigned = match_package(show_id, pkg_rows, segments)
            for r, (idx, score) in zip(pkg_rows, assigned):
                # User-confirmed resolution: episode_id 2365 ("PinkIn.mp4") is the missing
                # "Shooting Of Caribou Lou" segment by elimination, not a duplicate.
                if r['episode_id'] == 2365:
                    idx, score = 1, 1.0
                enriched.append({**r, 'seg_idx': idx, 'seg_count': len(segments), 'segments': segments, 'match_score': score})

        packages = {}
        for r in enriched:
            key = (r['show_season_number'], r['episode_number'], r['episode_title'])
            packages.setdefault(key, []).append(r)

        show_report = {"show_id": show_id, "seasons": {}}

        for season in sorted(set(r['show_season_number'] for r in enriched)):
            season_packages = [
                (key, rows_) for key, rows_ in packages.items() if key[0] == season
            ]
            # order: by old episode_number, multi-segment packages before single-segment
            # collisions at the same old number, then by earliest episode_id as a final
            # deterministic tiebreak (no reliable date evidence exists for these).
            def pkg_sort_key(item):
                key, rows_ = item
                _, old_num, _title = key
                is_single = 1 if rows_[0]['seg_count'] == 1 else 0
                return (old_num, is_single, min(r['episode_id'] for r in rows_))

            season_packages.sort(key=pkg_sort_key)

            counter = 1
            season_log = []
            for key, rows_ in season_packages:
                rows_sorted = sorted(rows_, key=lambda r: r['seg_idx'])
                pkg_entries = []
                for r in rows_sorted:
                    new_num = counter
                    counter += 1
                    updates.append((r['episode_id'], new_num))
                    pkg_entries.append({
                        "episode_id": r['episode_id'],
                        "episode_file": r['episode_file'],
                        "segment": r['segments'][r['seg_idx']],
                        "old_number": str(r['episode_number']),
                        "new_number": new_num,
                        "match_score": round(r['match_score'], 2),
                    })
                season_log.append({"package_title": key[2], "old_number": str(key[1]), "episodes": pkg_entries})
            show_report["seasons"][season] = season_log

        report["shows"].append(show_report)

    with open("renumber_cartoon_shows_report.json", "w") as f:
        json.dump(report, f, indent=2)

    print(f"Planned {len(updates)} renumbering updates. Report: renumber_cartoon_shows_report.json")

    import sys
    if "--apply" in sys.argv:
        for episode_id, new_num in updates:
            cur.execute("UPDATE episodes SET episode_number = %s WHERE episode_id = %s", (new_num, episode_id))
        conn.commit()
        print("Applied.")
    else:
        conn.rollback()
        print("Dry-run only (rolled back). Re-run with --apply to commit.")


if __name__ == "__main__":
    main()
