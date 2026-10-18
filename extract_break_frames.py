#!/usr/bin/env python3
"""
PERMANENT SCRIPT — repo root, do not recreate in scratchpad. See COMMERCIAL_BREAK_AUDIT.md.

Given a rebuild_show.py candidates JSON, extract one frame at the midpoint of every
candidate break (plus, for any break longer than --long-threshold seconds, extra frames
sampled across its span — a long duration is a red flag worth checking at multiple points,
not just the midpoint). Frames are written to <out_dir>/<episode_id>_<break_index>_<t>.png.

Also extracts --context-count frames (default 5) spaced --context-spacing seconds apart
(default 0.25s) immediately BEFORE break_point and the same number immediately AFTER
resume_point, for EVERY candidate, clamped to the episode's start_point/end_point. Tagged
pre_0..pre_N (pre_0 closest to the break) and post_0..post_N (post_0 closest to the break).
This exists because a black frame's content alone (or lack of it) can't distinguish a real
ad-break splice from an ordinary in-program scene transition (a fade to black between two
scenes) -- both can look identical from inside the black window. A short filmstrip of the
actual scene right before and after (not just one frame each side) lets a reviewer judge
actual motion/narrative continuity across the cut -- 2026-09-25 methodology addition, see
COMMERCIAL_BREAK_AUDIT.md.

Also extracts frames from the gap BETWEEN any two breaks in the same episode that are under
--close-pair-threshold seconds apart (per the charter's close-pair rule: close pairs must be
resolved by viewing the frames between them, never by position/duration/pattern alone).
Gap frames are tagged gap<i>_<j>_s0..sN and reference both break indices in the manifest.

Runs extractions in parallel (default 8 workers).

Usage: python3 extract_break_frames.py <candidates_json> <out_dir> [--workers 8] [--long-threshold 5]

After running, use scratchpad triage (file size outliers) to decide which frames need a
full-resolution Read.
"""
import argparse
import json
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed


def extract_frame(video_path, t, out_path):
    cmd = ["ffmpeg", "-y", "-ss", str(t), "-i", video_path, "-frames:v", "1", out_path]
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return out_path, os.path.getsize(out_path) if os.path.exists(out_path) else 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("candidates_json")
    parser.add_argument("out_dir")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--long-threshold", type=float, default=5.0)
    parser.add_argument("--long-samples", type=int, default=5,
                         help="number of extra sample points across a long break's span")
    parser.add_argument("--close-pair-threshold", type=float, default=5.0,
                         help="max gap (seconds) between two breaks to trigger gap-frame extraction")
    parser.add_argument("--close-pair-samples", type=int, default=3,
                         help="number of frames sampled across a close pair's gap")
    parser.add_argument("--context-count", type=int, default=5,
                         help="number of frames to sample on each side of the break for narrative-continuity context")
    parser.add_argument("--context-spacing", type=float, default=0.25,
                         help="seconds between consecutive context frames")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    data = json.load(open(args.candidates_json))

    def bounds(b):
        # supports both the current dict form {"start","end","level","levels_found"}
        # and the older plain [start, end] tuple form for pre-2026-09-14 JSON files.
        if isinstance(b, dict):
            return b["start"], b["end"]
        return b[0], b[1]

    jobs = []  # (episode_id, break_index, tag, t, video_path)
    for episode_id, ep in data.items():
        path = ep.get("path")
        if not path:
            continue
        ep_start = ep.get("start_point", 0.0)
        ep_end = ep.get("end_point")
        breaks = ep.get("breaks", [])
        for bi, b in enumerate(breaks):
            b_start, b_end = bounds(b)
            mid = (b_start + b_end) / 2
            jobs.append((episode_id, bi, "mid", mid, path))
            if (b_end - b_start) > args.long_threshold:
                n = args.long_samples
                for si in range(n):
                    frac = (si + 1) / (n + 1)
                    t = b_start + frac * (b_end - b_start)
                    jobs.append((episode_id, bi, f"s{si}", t, path))

            for ci in range(args.context_count):
                pre_t = max(ep_start, b_start - (ci + 1) * args.context_spacing)
                jobs.append((episode_id, bi, f"pre_{ci}", pre_t, path))
                post_t = b_end + (ci + 1) * args.context_spacing
                if ep_end is not None:
                    post_t = min(ep_end, post_t)
                jobs.append((episode_id, bi, f"post_{ci}", post_t, path))

        # close-pair gap frames: consecutive breaks (sorted by start) whose gap is under
        # --close-pair-threshold get sampled between the pair, per the charter's close-pair rule.
        ordered = sorted(range(len(breaks)), key=lambda i: bounds(breaks[i])[0])
        for a, b_idx in zip(ordered, ordered[1:]):
            _, end_a = bounds(breaks[a])
            start_b, _ = bounds(breaks[b_idx])
            gap = start_b - end_a
            if 0 <= gap < args.close_pair_threshold:
                pair_tag = f"{a}-{b_idx}"
                n = args.close_pair_samples
                for si in range(n):
                    frac = (si + 1) / (n + 1)
                    t = end_a + frac * gap
                    jobs.append((episode_id, pair_tag, f"gap_s{si}", t, path))

    print(f"{len(jobs)} frame extractions across {len(data)} episodes, {args.workers} workers")

    manifest = []

    def run_job(job):
        episode_id, bi, tag, t, path = job
        fname = f"{episode_id}_{bi}_{tag}_{t:.2f}.png"
        out_path = os.path.join(args.out_dir, fname)
        _, size = extract_frame(path, t, out_path)
        return {"episode_id": episode_id, "break_index": bi, "tag": tag, "t": t,
                "file": fname, "size": size}

    done = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(run_job, j) for j in jobs]
        for fut in as_completed(futures):
            manifest.append(fut.result())
            done += 1
            if done % 50 == 0 or done == len(jobs):
                print(f"  {done}/{len(jobs)}")

    manifest_path = os.path.join(args.out_dir, "manifest.json")
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"Wrote {manifest_path}")


if __name__ == "__main__":
    main()
