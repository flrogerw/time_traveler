#!/usr/bin/env python3
"""
Rebuild commercial_breaks_v2 candidates for one show, full coverage (every episode).

PERMANENT SCRIPT — lives at repo root. Do not recreate this in scratchpad; if it needs
changes, edit this file in place. See COMMERCIAL_BREAK_AUDIT.md for the audit methodology
this implements and the per-show log of results.

Usage: python3 rebuild_show.py <show_id> <out_json> [--workers N]

Writes a JSON file: {episode_id: {"file": ..., "path": ..., "start_point":, "end_point":,
"level": <which threshold level found candidates>, "breaks": [[start,end], ...]}}

Threshold levels (per COMMERCIAL_BREAK_AUDIT.md methodology):
  0: blackdetect d=0.8/th=0.1, silencedetect db=50 (defaults)
  1: blackdetect d=0.8/th=0.1, silencedetect db=35
  2: blackdetect d=0.3/th=0.15, silencedetect db=35
  3: blackdetect d=0.15/th=0.25, silencedetect db=30

POLICY (changed 2026-09-14, see COMMERCIAL_BREAK_AUDIT.md "Detection policy change"):
ALWAYS run all 4 levels, every episode, every time — never stop early. Take the union of
every level's silence-confirmed candidates, deduping any whose spans overlap or sit within
2s of each other down to one (keeping the bounds from the strictest/lowest level that found
it). Do NOT use the old "stop at first non-empty level" or "only fall back if 0+1 find
nothing" logic — both under-detect whenever a stricter level finds SOME but not ALL of the
real breaks in an episode.

Break/resume timing comes ONLY from the black segment's own bounds (not silence) —
silence is used only to confirm a black segment is a real ad break
(merge_segments_black_bounded).

Runs episodes in parallel (default 8 workers, --workers to override) since ffmpeg
blackdetect/silencedetect per episode is independent I/O+CPU work. Results are written
incrementally as each episode finishes (order in the output JSON may not match episode_id
order, but every episode_id key is present when done).
"""
import argparse
import sys
import os
import json
import subprocess
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, "/Volumes/ExternalDrive/time_traveler")
os.chdir("/Volumes/ExternalDrive/time_traveler")

from dotenv import load_dotenv
import psycopg2

load_dotenv("/Volumes/ExternalDrive/time_traveler/.env", override=True)

RESULTS_LOCK = threading.Lock()


def db_conn():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME") or os.getenv("DATABASE"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT") or 5432,
    )


def run_ffmpeg_blackdetect(video_path, duration=0.8, threshold=0.1):
    cmd = [
        "ffmpeg", "-i", video_path,
        "-vf", f"blackdetect=d={duration}:pix_th={threshold}",
        "-an", "-f", "null", "-"
    ]
    result = subprocess.run(cmd, stderr=subprocess.PIPE, stdout=subprocess.DEVNULL, text=True)
    black_segments = []
    for line in result.stderr.splitlines():
        m = re.search(r"black_start:(\d+\.?\d*)\s+black_end:(\d+\.?\d*)", line)
        if m:
            black_segments.append((float(m.group(1)), float(m.group(2))))
    return black_segments


def run_ffmpeg_silencedetect(video_path, db=50, duration=0.2):
    cmd = [
        "ffmpeg", "-i", video_path,
        "-af", f"silencedetect=n=-{db}dB:d={duration}",
        "-f", "null", "-"
    ]
    result = subprocess.run(cmd, stderr=subprocess.PIPE, stdout=subprocess.DEVNULL, text=True)
    silence_segments = []
    start = None
    for line in result.stderr.splitlines():
        if "silence_start" in line:
            start = float(line.split("silence_start: ")[1])
        elif "silence_end" in line and start is not None:
            end = float(line.split("silence_end: ")[1].split()[0])
            silence_segments.append((start, end))
            start = None
    return silence_segments


def merge_close_segments(segments, max_gap=1.0):
    if not segments:
        return []
    segments = sorted(segments)
    merged = [segments[0]]
    for cur in segments[1:]:
        last = merged[-1]
        if cur[0] - last[1] <= max_gap:
            merged[-1] = (last[0], max(last[1], cur[1]))
        else:
            merged.append(cur)
    return merged


def merge_segments_black_bounded(black, silence, tolerance=1.0):
    """Keep black segment's own bounds; silence only confirms it's a real break."""
    commercials = []
    for b_start, b_end in black:
        for s_start, s_end in silence:
            latest_start = max(b_start, s_start)
            earliest_end = min(b_end, s_end)
            overlap = earliest_end - latest_start
            if overlap >= 0 or abs(b_end - s_start) <= tolerance or abs(s_end - b_start) <= tolerance:
                commercials.append((b_start, b_end))
                break
    return merge_close_segments(commercials)


def filter_edges(segments, start_point=0.0, end_point=None, epsilon=0.2):
    if not segments:
        return []
    filtered = [s for s in segments if s[0] > start_point + epsilon]
    if end_point is not None:
        filtered = [s for s in filtered if s[1] < end_point - epsilon]
    return filtered


def union_dedupe(tagged_candidates, gap=2.0):
    """
    tagged_candidates: list of (break_tuple, level). Cluster candidates whose spans
    overlap or sit within `gap` seconds of each other (they're almost certainly the
    same underlying break, just measured slightly differently by a looser threshold),
    and keep the bounds from the STRICTEST level in each cluster (lower level number =
    tighter blackdetect/silencedetect params = more trustworthy bounds). Retains
    provenance: which level(s) independently found each logical candidate.
    Returns (list of {"start","end","level","levels_found"}, min_level_used).
    """
    if not tagged_candidates:
        return [], None
    tagged_candidates = sorted(tagged_candidates, key=lambda x: x[0][0])
    clusters = [[tagged_candidates[0]]]
    for cand in tagged_candidates[1:]:
        cluster_end = max(c[0][1] for c in clusters[-1])
        if cand[0][0] - cluster_end <= gap:
            clusters[-1].append(cand)
        else:
            clusters.append([cand])

    result = []
    min_level = min(lvl for _, lvl in tagged_candidates)
    for cluster in clusters:
        best = min(cluster, key=lambda x: x[1])
        levels_found = sorted(set(lvl for _, lvl in cluster))
        result.append({
            "start": best[0][0],
            "end": best[0][1],
            "level": best[1],
            "levels_found": levels_found,
        })
    return result, min_level


def detect(video_path, start_point, end_point):
    """
    Always run every threshold level (0-3) and take the silence-confirmed union,
    not just the first level that finds something. A show can have SOME real breaks
    findable at strict thresholds and OTHER real breaks that only loosened thresholds
    catch (e.g. Land of the Lost, 2026-09-14: level 0/1 found only the near-end break
    in many episodes, but level 2/3 found 2 more mid-episode breaks per episode, each
    independently confirmed by overlapping silence). Stopping at the first non-empty
    level under-detects whenever that level finds SOME but not ALL real breaks.
    """
    black0 = run_ffmpeg_blackdetect(video_path, 0.8, 0.1)
    black2 = run_ffmpeg_blackdetect(video_path, 0.3, 0.15)
    black3 = run_ffmpeg_blackdetect(video_path, 0.15, 0.25)

    sil0 = run_ffmpeg_silencedetect(video_path, 50, 0.2)
    sil1 = run_ffmpeg_silencedetect(video_path, 35, 0.2)
    sil3 = run_ffmpeg_silencedetect(video_path, 30, 0.2)

    lvl0 = filter_edges(merge_segments_black_bounded(black0, sil0), start_point, end_point)
    lvl1 = filter_edges(merge_segments_black_bounded(black0, sil1), start_point, end_point)
    lvl2 = filter_edges(merge_segments_black_bounded(black2, sil1), start_point, end_point)
    lvl3 = filter_edges(merge_segments_black_bounded(black3, sil3), start_point, end_point)

    tagged = [(b, 0) for b in lvl0] + [(b, 1) for b in lvl1] + [(b, 2) for b in lvl2] + [(b, 3) for b in lvl3]
    return union_dedupe(tagged)


def build_file_index(name_prefix):
    """One find call for all files starting with name_prefix, across the whole tree."""
    cmd = ["find", "/Volumes/TTBS/time_traveler", "-iname", f"{name_prefix}*"]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, timeout=300)
    index = {}
    for line in result.stdout.splitlines():
        line = line.strip()
        if line:
            index[os.path.basename(line)] = line
    return index


def process_episode(episode_id, episode_file, start_point, end_point, title, path):
    if not path:
        return episode_id, {"file": episode_file, "path": None, "error": "not found"}

    breaks, level = detect(path, start_point or 0.0, end_point)
    return episode_id, {
        "file": episode_file,
        "path": path,
        "start_point": start_point,
        "end_point": end_point,
        "title": title,
        "level": level,
        "breaks": breaks,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("show_id", type=int)
    parser.add_argument("out_json")
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()

    conn = db_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT episode_id, episode_file, start_point, end_point, episode_title "
        "FROM episodes WHERE show_id=%s ORDER BY episode_id", (args.show_id,)
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    print(f"Show {args.show_id}: {len(rows)} episodes, {args.workers} workers")

    prefix = os.path.commonprefix([r[1] for r in rows]).rstrip("_") or rows[0][1].split("_")[0]
    print(f"Indexing files under prefix '{prefix}*'...")
    file_index = build_file_index(prefix)
    print(f"Indexed {len(file_index)} files")

    results = {}
    total = len(rows)
    done_count = 0

    def save():
        with open(args.out_json, "w") as f:
            json.dump(results, f, indent=2)

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(
                process_episode, episode_id, episode_file, start_point, end_point, title,
                file_index.get(episode_file)
            ): episode_id
            for episode_id, episode_file, start_point, end_point, title in rows
        }
        for future in as_completed(futures):
            episode_id, result = future.result()
            with RESULTS_LOCK:
                results[episode_id] = result
                done_count += 1
                n_breaks = len(result.get("breaks", []))
                print(f"  [{done_count}/{total}] episode {episode_id} ({result.get('title')}): "
                      f"{n_breaks} candidates (level {result.get('level')})"
                      + (" FILE NOT FOUND" if result.get("error") else ""))
                save()

    print(f"Done. Wrote {args.out_json}")


if __name__ == "__main__":
    main()
