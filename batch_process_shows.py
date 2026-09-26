#!/usr/bin/env python3
"""
PERMANENT SCRIPT — repo root, do not recreate in scratchpad. See COMMERCIAL_BREAK_AUDIT.md.

Unattended batch driver: runs rebuild_show.py -> extract_break_frames.py -> verify_breaks_vlm.py
for a list of shows, one after another, with NO human/Claude intervention until the results are
read afterward. Never writes to commercial_breaks_v2 -- only reads from the DB and writes JSON
files to disk, so it's safe to leave running for hours (e.g. overnight) with nobody watching.

Writes each show's outputs to <out_root>/<show_id>/{candidates.json, frames/, verified.json,
verified.json.needs_review.json, batch.log}. Resumable: a show whose verified.json already
exists is skipped unless --force. One show's failure is logged and does not stop the batch.

Usage:
  python3 batch_process_shows.py --out-root batch_runs --show-ids 231 177 266
  python3 batch_process_shows.py --out-root batch_runs --untouched-shows
      # auto-queues every show with zero commercial_breaks_v2 rows and >0 episodes, smallest
      # episode count first, excluding shows already flagged in COMMERCIAL_BREAK_AUDIT.md as
      # either explicitly-skip (e.g. Match Game, told to skip by the user) or confirmed-done
      # with genuinely zero real breaks (the 12 cartoon shows) -- see the two sets below.
  python3 batch_process_shows.py --out-root batch_runs \
      --queue-file show_reprocess_queue.json --categories detection_risk_1_125 detection_risk_126_152
      # reads show_reprocess_queue.json (see COMMERCIAL_BREAK_AUDIT.md's "Full itemized
      # breakdown" entry) and queues the named categories, smallest episode count first.

Crash/resume behavior: <out_root>/batch_summary.json is updated after EVERY show, including an
"in_progress" entry written the moment a show starts (before rebuild_show.py even runs), so if
the process is killed or the machine crashes mid-show, batch_summary.json still shows exactly
which show was in flight, not just the ones that finished. On the next run with the same
arguments, any show already marked "done" is skipped; a show that was "in_progress" or failed is
retried from scratch (rebuild_show.py/extract_break_frames.py/verify_breaks_vlm.py all just
overwrite their own output files, so a partial prior attempt is safely clobbered).

Requires a running Ollama daemon with the model already pulled before starting:
  ollama serve &
  ollama pull qwen3-vl:30b
"""
import argparse
import json
import os
import subprocess
import sys
import time
import urllib.request

REPO_ROOT = "/Volumes/ExternalDrive/time_traveler"

# Shows COMMERCIAL_BREAK_AUDIT.md says to skip / not touch without asking first.
# Match Game (340): user told a prior session to skip it; ask before revisiting.
SKIP_SHOW_IDS = {340}

# The 12 cartoon shows already confirmed DONE with genuinely zero real breaks (not
# "untouched") -- see COMMERCIAL_BREAK_AUDIT.md "12 zero-row shows ... CONFIRMED DONE,
# do not touch." Resolved by show_name lookup on 2026-09-17.
CONFIRMED_ZERO_BREAK_SHOW_IDS = {
    36,  # Atom Ant
    41,  # Popeye the Sailor
    42,  # Felix the Cat
    43,  # Secret Squirrel
    44,  # Ant and the Aardvark
    46,  # Squiddly Diddly
    48,  # Wally Gator
    49,  # Tijuana_Toads
    50,  # Roger Ramjet
    51,  # Dudley Do-Right Show
    62,  # Lippy The Lion & Hardy Har Har
    67,  # Ricochet Rabbit & Droop-a-long
}


def db_conn():
    sys.path.insert(0, REPO_ROOT)
    from dotenv import load_dotenv
    import psycopg2
    load_dotenv(os.path.join(REPO_ROOT, ".env"), override=True)
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME") or os.getenv("DATABASE"),
        user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"), port=os.getenv("DB_PORT") or 5432,
    )


def untouched_shows():
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT s.show_id, s.show_name, count(e.episode_id) AS n_eps
        FROM shows s JOIN episodes e ON e.show_id = s.show_id
        LEFT JOIN commercial_breaks_v2 cb ON cb.media_id = e.episode_id
        GROUP BY s.show_id, s.show_name
        HAVING count(cb.break_id) = 0
        ORDER BY n_eps ASC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [(show_id, name, n) for show_id, name, n in rows
            if show_id not in SKIP_SHOW_IDS and show_id not in CONFIRMED_ZERO_BREAK_SHOW_IDS]


def load_from_queue(queue_file, categories):
    data = json.load(open(queue_file))
    unknown = [c for c in categories if c not in data]
    if unknown:
        print(f"ERROR: unknown categories {unknown}. Available: {list(data.keys())}")
        sys.exit(1)
    combined = []
    for c in categories:
        for entry in data[c]:
            show_id, name, n_eps = entry[0], entry[1], entry[2]
            combined.append((show_id, name, n_eps))
    combined.sort(key=lambda s: (s[2] if s[2] is not None else 0))
    return combined


def show_names(show_ids):
    conn = db_conn()
    cur = conn.cursor()
    out = []
    for sid in show_ids:
        cur.execute("SELECT show_name FROM shows WHERE show_id=%s", (sid,))
        row = cur.fetchone()
        out.append((sid, row[0] if row else "?", None))
    cur.close()
    conn.close()
    return out


def format_duration(seconds):
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def check_ollama(host):
    try:
        urllib.request.urlopen(f"{host}/api/tags", timeout=5)
        return True
    except Exception:
        return False


def run_step(cmd, log_path):
    """Each sub-script's own per-episode chatter (rebuild_show.py/extract_break_frames.py/
    verify_breaks_vlm.py all print per-episode progress) goes to the show's batch.log only, not
    the terminal -- the terminal gets one concise line per show instead (see main()), so a long
    unattended run stays readable rather than flooded with per-episode noise."""
    with open(log_path, "a") as f:
        f.write(f"\n$ {' '.join(cmd)}\n")
        f.flush()
        result = subprocess.run(cmd, cwd=REPO_ROOT, stdout=f, stderr=subprocess.STDOUT)
    return result.returncode == 0


def process_show(show_id, out_root, model, ollama_host, force):
    show_dir = os.path.join(out_root, str(show_id))
    os.makedirs(show_dir, exist_ok=True)
    log_path = os.path.join(show_dir, "batch.log")
    candidates_path = os.path.join(show_dir, "candidates.json")
    frames_dir = os.path.join(show_dir, "frames")
    verified_path = os.path.join(show_dir, "verified.json")

    if os.path.exists(verified_path) and not force:
        return "skipped (already done)"

    py = sys.executable
    if not run_step([py, "rebuild_show.py", str(show_id), candidates_path, "--workers", "8"], log_path):
        return "FAILED at rebuild_show.py -- see batch.log"

    if not run_step([py, "extract_break_frames.py", candidates_path, frames_dir, "--workers", "8"], log_path):
        return "FAILED at extract_break_frames.py -- see batch.log"

    if not run_step([py, "verify_breaks_vlm.py", candidates_path, frames_dir, verified_path,
                      "--model", model, "--ollama-host", ollama_host], log_path):
        return "FAILED at verify_breaks_vlm.py -- see batch.log"

    return "done"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-root", default="batch_runs")
    parser.add_argument("--show-ids", type=int, nargs="*", default=None)
    parser.add_argument("--untouched-shows", action="store_true",
                         help="auto-queue every untouched show, smallest episode count first")
    parser.add_argument("--queue-file", default=None,
                         help="JSON file with named show-list categories (see show_reprocess_queue.json)")
    parser.add_argument("--categories", nargs="*", default=None,
                         help="which categories to queue from --queue-file, e.g. untouched detection_risk_1_125")
    parser.add_argument("--model", default="qwen3-vl:30b")
    parser.add_argument("--ollama-host", default="http://localhost:11434")
    parser.add_argument("--force", action="store_true",
                         help="reprocess a show even if its verified.json already exists")
    args = parser.parse_args()

    if not check_ollama(args.ollama_host):
        print(f"ERROR: Ollama not reachable at {args.ollama_host}. Start it first: ollama serve &")
        sys.exit(1)

    if args.queue_file:
        if not args.categories:
            print("ERROR: --queue-file requires --categories <name ...>")
            sys.exit(1)
        shows = load_from_queue(args.queue_file, args.categories)
        print(f"Queuing {len(shows)} shows from {args.queue_file} categories {args.categories}, "
              f"smallest episode count first.")
    elif args.untouched_shows:
        shows = untouched_shows()
        print(f"Queuing {len(shows)} untouched shows, smallest episode count first.")
    elif args.show_ids:
        shows = show_names(args.show_ids)
    else:
        print("Provide --show-ids <id ...>, --untouched-shows, or --queue-file <file> --categories <name ...>")
        sys.exit(1)

    os.makedirs(args.out_root, exist_ok=True)
    summary_path = os.path.join(args.out_root, "batch_summary.json")
    summary = json.load(open(summary_path)) if os.path.exists(summary_path) else {}

    for i, (show_id, name, n_eps) in enumerate(shows, 1):
        eps_str = f"{n_eps} episodes" if n_eps is not None else "episode count unknown"

        prior = summary.get(str(show_id))
        if prior and prior.get("status") == "done" and not args.force:
            print(f"[{i}/{len(shows)}] show {show_id} ({name}, {eps_str}) -- already done, skipping")
            continue

        print(f"[{i}/{len(shows)}] show {show_id} ({name}, {eps_str})...")

        # Written BEFORE processing starts, so a crash/kill mid-show still leaves an accurate
        # record in batch_summary.json of exactly which show was in flight, not just the ones
        # that finished -- this is what makes the batch resumable after a real crash, not just
        # a clean stop between shows.
        summary[str(show_id)] = {"name": name, "status": "in_progress", "seconds": None}
        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2)

        t0 = time.time()
        status = process_show(show_id, args.out_root, args.model, args.ollama_host, args.force)
        elapsed = time.time() - t0
        print(f"    -> {name} ({eps_str}): {status} in {format_duration(elapsed)} [{i}/{len(shows)}]")
        summary[str(show_id)] = {"name": name, "status": status, "seconds": round(elapsed)}
        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2)

    n_done = sum(1 for v in summary.values() if v["status"] == "done")
    n_failed = sum(1 for v in summary.values() if str(v["status"]).startswith("FAILED"))
    print(f"\nBatch complete. {n_done} done, {n_failed} failed. Summary: {summary_path}")


if __name__ == "__main__":
    main()
