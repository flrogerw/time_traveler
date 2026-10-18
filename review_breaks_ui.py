#!/usr/bin/env python3
"""
PERMANENT SCRIPT -- repo root, do not recreate in scratchpad. See COMMERCIAL_BREAK_AUDIT.md.

Local browser tool for the commercial-break audit's mandatory frame-by-frame review step
(the charter requires every detected break candidate to be individually visually checked
before insertion into `commercial_breaks_v2` -- no sampling, no delegation to a subagent or
cheaper model). Doing this review turn-by-turn in chat is extremely token-heavy; this tool
lets a human do the same review directly in a browser, using the exact same evidence
(candidates.json/verified.json/frames from rebuild_show.py + extract_break_frames.py +
verify_breaks_vlm.py) -- it makes no AI judgment of its own anywhere.

Usage: python3 review_breaks_ui.py [--port 5055]
Then open http://127.0.0.1:5055/ in a browser. Ctrl-C to stop.

Data flow:
  - Reads batch_runs/<show_id>/verified.json + frames/manifest.json (untouched, pipeline output).
  - Writes/updates batch_runs/<show_id>/human_review.json as decisions are made (resumable).
  - On Finalize (only reachable once every candidate has an explicit decision): inserts the
    surviving "keep" breaks into commercial_breaks_v2, appends one plain factual row to
    COMMERCIAL_BREAK_AUDIT.md's per-show table, then deletes batch_runs/<show_id>/ entirely
    (candidates.json, verified.json, frames/, manifest.json, human_review.json) per the
    charter's cleanup rule.

No sampling is possible: there is no skip/bulk-decide affordance anywhere in the UI, and the
finalize endpoint independently re-verifies every candidate has a decision before doing anything.
"""
import argparse
import json
import os
import subprocess
import sys
from datetime import datetime

import psycopg
from dotenv import load_dotenv
from flask import Flask, jsonify, request, send_from_directory, abort

load_dotenv()

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
BATCH_RUNS = os.path.join(REPO_ROOT, "batch_runs")
AUDIT_LOG = os.path.join(REPO_ROOT, "COMMERCIAL_BREAK_AUDIT.md")
BATCH_SUMMARY = os.path.join(BATCH_RUNS, "batch_summary.json")

OPENING_TITLE_WINDOW = 35.0  # seconds from episode start_point
LONG_BREAK_THRESHOLD = 5.0
CLOSE_PAIR_THRESHOLD = 5.0

app = Flask(__name__)


def get_db_connection():
    return psycopg.connect(
        host=os.environ["DB_HOST"],
        port=os.environ.get("DB_PORT", "5432"),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        dbname=os.environ.get("DATABASE") or os.environ["DB_NAME"],
    )


def show_dir(show_id):
    d = os.path.realpath(os.path.join(BATCH_RUNS, str(int(show_id))))
    if not d.startswith(os.path.realpath(BATCH_RUNS) + os.sep):
        abort(400)
    return d


def load_json(path, default=None):
    if not os.path.exists(path):
        return default
    with open(path) as f:
        return json.load(f)


def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


# ---------------------------------------------------------------------------
# Show list
# ---------------------------------------------------------------------------

@app.route("/api/shows")
def api_shows():
    summary = load_json(BATCH_SUMMARY, {})
    done_ids = [int(k) for k, v in summary.items() if v.get("status") == "done"]

    all_episode_ids = []
    show_episode_ids = {}
    for show_id in done_ids:
        verified = load_json(os.path.join(show_dir(show_id), "verified.json"))
        if not verified:
            continue
        eps = [int(e) for e in verified.keys()]
        show_episode_ids[show_id] = eps
        all_episode_ids.extend(eps)

    already_has_rows = set()
    if all_episode_ids:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT media_id FROM commercial_breaks_v2 WHERE media_id = ANY(%s)", (all_episode_ids,))
        already_has_rows = {r[0] for r in cur.fetchall()}
        conn.close()

    shows = []
    for show_id, eps in show_episode_ids.items():
        if any(e in already_has_rows for e in eps):
            continue  # already finalized (or partially finalized) -- not ready for review
        review_state = load_json(os.path.join(show_dir(show_id), "human_review.json"), {})
        verified = load_json(os.path.join(show_dir(show_id), "verified.json"))
        total = sum(len(ep["breaks"]) for ep in verified.values())
        decided = sum(len(v) for v in review_state.values())
        shows.append({
            "show_id": show_id,
            "name": summary[str(show_id)]["name"],
            "episode_count": len(eps),
            "total_candidates": total,
            "decided_candidates": decided,
        })

    shows.sort(key=lambda s: s["episode_count"])
    return jsonify(shows)


# ---------------------------------------------------------------------------
# Show detail / review queue
# ---------------------------------------------------------------------------

def build_frame_index(show_id):
    manifest = load_json(os.path.join(show_dir(show_id), "frames", "manifest.json"), [])
    index = {}
    for row in manifest:
        key = (str(row["episode_id"]), str(row["break_index"]))
        index.setdefault(key, []).append(row)
    return index


def compute_flags(breaks):
    """Given a list of break dicts (with 'start'/'end'), return per-index flags for
    long-break / close-pair (and which partner indices) -- same thresholds as
    extract_break_frames.py, so this matches exactly which frames actually got extracted.

    A candidate can be the shared middle of a close-pair chain (e.g. 16-17 AND 17-18 both
    under the threshold) -- each side has its own separate gap-frame set, so partners is a
    list, not a single value, or the shared middle candidate would silently lose one side's
    gap frames."""
    flags = {i: {"is_long": (b["end"] - b["start"]) > LONG_BREAK_THRESHOLD, "pair_partners": []} for i, b in enumerate(breaks)}
    ordered = sorted(range(len(breaks)), key=lambda i: breaks[i]["start"])
    for a, b_idx in zip(ordered, ordered[1:]):
        gap = breaks[b_idx]["start"] - breaks[a]["end"]
        if 0 <= gap < CLOSE_PAIR_THRESHOLD:
            flags[a]["pair_partners"].append(b_idx)
            flags[b_idx]["pair_partners"].append(a)
    return flags


def ensure_context_frames(show_id):
    """Lazily backfill pre/post context frames for shows whose frames/ directory predates the
    2026-09-25 addition of that extraction step -- runs extract_break_frames.py once (it
    re-extracts everything, including the frames that already existed, which is wasteful but
    simple and correct) the first time such a show is opened, rather than batch-backfilling
    all ~140 batch_runs/ shows upfront (slow, and some may still be actively written to by a
    running batch_process_shows.py pass)."""
    d = show_dir(show_id)
    manifest_path = os.path.join(d, "frames", "manifest.json")
    manifest = load_json(manifest_path, [])
    if any(row["tag"] in ("pre_0", "post_0") for row in manifest):
        return  # already backfilled with the current (multi-frame) context format
    candidates_path = os.path.join(d, "candidates.json")
    if not os.path.exists(candidates_path):
        return
    subprocess.run(
        [sys.executable, os.path.join(REPO_ROOT, "extract_break_frames.py"), candidates_path, os.path.join(d, "frames")],
        check=True,
    )


@app.route("/api/show/<int:show_id>")
def api_show_detail(show_id):
    ensure_context_frames(show_id)
    d = show_dir(show_id)
    verified = load_json(os.path.join(d, "verified.json"))
    if verified is None:
        abort(404)
    frame_index = build_frame_index(show_id)
    review_state = load_json(os.path.join(d, "human_review.json"), {})
    summary = load_json(BATCH_SUMMARY, {})
    name = summary.get(str(show_id), {}).get("name", f"show {show_id}")

    episodes_out = []
    for episode_id in sorted(verified.keys(), key=int):
        ep = verified[episode_id]
        breaks = ep["breaks"]
        flags = compute_flags(breaks)
        ep_decisions = review_state.get(episode_id, {})

        breaks_out = []
        for i, b in enumerate(breaks):
            frames = list(frame_index.get((episode_id, str(i)), []))
            # each close-pair partner's gap_s* frames are filed under their own "a-b" combined
            # key -- a candidate that's the shared middle of a chain (e.g. paired with both its
            # neighbors) has two separate gap-frame sets, both fetched here.
            for partner in flags[i]["pair_partners"]:
                a, bb = sorted([i, partner])
                frames += frame_index.get((episode_id, f"{a}-{bb}"), [])
            is_opening_title = b["start"] <= (ep["start_point"] + OPENING_TITLE_WINDOW)
            breaks_out.append({
                "index": i,
                "start": b["start"],
                "end": b["end"],
                "duration": round(b["end"] - b["start"], 2),
                "vlm_verdict": b.get("vlm_verdict"),
                "vlm_confidence": b.get("vlm_confidence"),
                "vlm_reasoning": b.get("vlm_reasoning"),
                "is_long": flags[i]["is_long"],
                "pair_partners": flags[i]["pair_partners"],
                "is_opening_title": is_opening_title,
                "frames": [{"tag": f["tag"], "url": f"/frames/{show_id}/{f['file']}", "size": f["size"], "t": f["t"]} for f in
                           sorted(frames, key=lambda f: f["t"])],
                "decision": ep_decisions.get(str(i)),
            })

        episodes_out.append({
            "episode_id": episode_id,
            "title": ep.get("title"),
            "file": ep.get("file"),
            "start_point": ep["start_point"],
            "end_point": ep["end_point"],
            "breaks": breaks_out,
        })

    total = sum(len(e["breaks"]) for e in episodes_out)
    decided = sum(1 for e in episodes_out for b in e["breaks"] if b["decision"] is not None)

    return jsonify({
        "show_id": show_id,
        "name": name,
        "episodes": episodes_out,
        "total_candidates": total,
        "decided_candidates": decided,
        "all_decided": decided == total,
    })


@app.route("/frames/<int:show_id>/<path:filename>")
def serve_frame(show_id, filename):
    return send_from_directory(os.path.join(show_dir(show_id), "frames"), filename)


@app.route("/api/show/<int:show_id>/clip/<episode_id>/<int:break_index>")
def api_clip(show_id, episode_id, break_index):
    """On-demand only (never pre-generated) -- a short playable clip from 2s before the break
    to 2s after, for when the still-frame filmstrip isn't enough to tell whether a black frame
    is a real ad-splice or just a scene transition. Cached on disk after first generation."""
    d = show_dir(show_id)
    verified = load_json(os.path.join(d, "verified.json"))
    ep = verified.get(str(episode_id))
    if not ep or break_index >= len(ep["breaks"]):
        abort(404)
    b = ep["breaks"][break_index]

    clip_dir = os.path.join(d, "frames", "clips")
    os.makedirs(clip_dir, exist_ok=True)
    clip_name = f"{episode_id}_{break_index}.mp4"
    clip_path = os.path.join(clip_dir, clip_name)

    if not os.path.exists(clip_path):
        start = max(ep.get("start_point", 0.0), b["start"] - 2.0)
        end = b["end"] + 2.0
        if ep.get("end_point") is not None:
            end = min(ep["end_point"], end)
        duration = max(0.1, end - start)
        subprocess.run(
            ["ffmpeg", "-y", "-ss", str(start), "-i", ep["path"], "-t", str(duration),
             "-c:v", "libx264", "-preset", "veryfast", "-c:a", "aac", clip_path],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )

    return send_from_directory(clip_dir, clip_name)


@app.route("/api/show/<int:show_id>/decision", methods=["POST"])
def api_decision(show_id):
    payload = request.get_json()
    episode_id = str(payload["episode_id"])
    break_index = str(payload["break_index"])
    decision = payload["decision"]
    if decision not in ("keep", "reject"):
        abort(400)

    path = os.path.join(show_dir(show_id), "human_review.json")
    state = load_json(path, {})
    state.setdefault(episode_id, {})[break_index] = decision
    save_json(path, state)
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Finalize
# ---------------------------------------------------------------------------

def build_final_breaks(show_id):
    """Returns (final_breaks_by_episode, warnings) using only 'keep' decisions. Raises
    ValueError if any candidate lacks a decision -- finalize must never run on a partially
    reviewed show."""
    d = show_dir(show_id)
    verified = load_json(os.path.join(d, "verified.json"))
    review_state = load_json(os.path.join(d, "human_review.json"), {})

    final = {}
    warnings = []
    for episode_id, ep in verified.items():
        ep_decisions = review_state.get(episode_id, {})
        breaks = ep["breaks"]
        if len(ep_decisions) != len(breaks):
            raise ValueError(f"episode {episode_id} has {len(breaks)} candidates but only {len(ep_decisions)} decisions")
        kept = sorted(
            [(b["start"], b["end"]) for i, b in enumerate(breaks) if ep_decisions.get(str(i)) == "keep"]
        )
        final[episode_id] = kept

        for i in range(1, len(kept)):
            gap = kept[i][0] - kept[i - 1][1]
            if gap < CLOSE_PAIR_THRESHOLD:
                warnings.append(f"episode {episode_id}: confirmed breaks at {kept[i-1]} and {kept[i]} are only {gap:.1f}s apart")
        if kept:
            run_length = ep["end_point"] - ep["start_point"]
            if kept[-1][0] > ep["start_point"] + 0.9 * run_length and len(kept) <= 2:
                warnings.append(f"episode {episode_id}: all {len(kept)} confirmed break(s) sit in the last 10% of the runtime -- unusual, double-check")
        else:
            warnings.append(f"episode {episode_id}: zero confirmed breaks")

    return final, warnings


@app.route("/api/show/<int:show_id>/finalize_preview")
def api_finalize_preview(show_id):
    try:
        final, warnings = build_final_breaks(show_id)
    except ValueError as e:
        return jsonify({"error": str(e)}), 409

    episode_ids = [int(e) for e in final.keys()]
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT media_id FROM commercial_breaks_v2 WHERE media_id = ANY(%s)", (episode_ids,))
    existing = [r[0] for r in cur.fetchall()]
    conn.close()

    total_confirmed = sum(len(v) for v in final.values())
    return jsonify({
        "final_breaks": final,
        "total_confirmed": total_confirmed,
        "warnings": warnings,
        "already_in_db": existing,
    })


@app.route("/api/show/<int:show_id>/finalize", methods=["POST"])
def api_finalize(show_id):
    try:
        final, _warnings = build_final_breaks(show_id)
    except ValueError as e:
        return jsonify({"error": str(e)}), 409

    episode_ids = [int(e) for e in final.keys()]
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT media_id FROM commercial_breaks_v2 WHERE media_id = ANY(%s)", (episode_ids,))
    existing = [r[0] for r in cur.fetchall()]
    if existing:
        conn.close()
        return jsonify({"error": f"commercial_breaks_v2 already has rows for episode_id(s) {existing} -- refusing to insert"}), 409

    inserted = 0
    for episode_id, breaks in final.items():
        for bp, rp in breaks:
            cur.execute(
                "INSERT INTO commercial_breaks_v2 (media_id, break_point, resume_point) VALUES (%s, %s, %s)",
                (int(episode_id), bp, rp),
            )
            inserted += 1
    conn.commit()
    conn.close()

    d = show_dir(show_id)
    verified = load_json(os.path.join(d, "verified.json"))
    review_state = load_json(os.path.join(d, "human_review.json"), {})
    summary = load_json(BATCH_SUMMARY, {})
    name = summary.get(str(show_id), {}).get("name", f"show {show_id}")

    total = sum(len(ep["breaks"]) for ep in verified.values())
    rejected = total - inserted
    row = f"| {show_id} | {name} | {len(verified)} | yes ({total} candidates reviewed, {inserted} confirmed, {rejected} rejected) | MANUAL |\n"
    with open(AUDIT_LOG, "a") as f:
        f.write(row)

    import shutil
    shutil.rmtree(d)

    return jsonify({"ok": True, "inserted": inserted, "rejected": rejected})


# ---------------------------------------------------------------------------
# Frontend (single page, inline)
# ---------------------------------------------------------------------------

INDEX_HTML = """<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Commercial Break Review</title>
<style>
  body { font-family: -apple-system, sans-serif; background: #1a1a1a; color: #eee; margin: 0; }
  header { padding: 12px 20px; background: #111; border-bottom: 1px solid #333; display: flex; justify-content: space-between; align-items: center; }
  #app { padding: 20px; max-width: 1400px; margin: 0 auto; }
  .show-row { display: flex; justify-content: space-between; padding: 10px 14px; margin-bottom: 6px; background: #242424; border-radius: 6px; cursor: pointer; }
  .show-row:hover { background: #2e2e2e; }
  .badges { min-height: 26px; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 12px; margin-left: 6px; }
  .badge.long { background: #7a4; }
  .badge.pair { background: #a74; }
  .badge.title { background: #a44; }
  .frame-grid { height: 150px; display: flex; flex-wrap: nowrap; overflow-x: auto; overflow-y: hidden; gap: 4px; margin: 10px 0; align-items: flex-end; }
  .frame-grid figure { margin: 0; text-align: center; flex: 0 0 auto; height: 130px; display: flex; flex-direction: column; justify-content: flex-end; }
  .frame-grid img { width: 110px; height: 82px; object-fit: cover; box-sizing: border-box; border: 1px solid #444; display: block; }
  .frame-grid figcaption { height: 14px; font-size: 10px; color: #aaa; margin-top: 2px; }
  .frame-grid figure.context img { border-color: #567; }
  .frame-grid figure.breakframe img { border: 3px solid #d63; }
  .frame-grid figcaption.break-label { color: #f96; font-weight: bold; font-size: 12px; }
  .hint { height: 70px; overflow-y: auto; color: #999; font-size: 13px; margin-top: 8px; white-space: pre-wrap; }
  .buttons { margin-top: 16px; }
  button { font-size: 16px; padding: 10px 22px; margin-right: 10px; border-radius: 6px; border: none; cursor: pointer; }
  .keep { background: #2a7; color: #fff; }
  .reject { background: #a33; color: #fff; }
  .progress { color: #aaa; }
  #candidate-title { font-size: 14px; color: #ccc; }
  .duration { font-size: 20px; font-weight: bold; color: #fff; margin: 4px 0 10px; }
  a.back { color: #6cf; cursor: pointer; }
  table.finalize { width: 100%; border-collapse: collapse; margin-top: 12px; }
  table.finalize td, table.finalize th { border: 1px solid #444; padding: 4px 8px; font-size: 13px; }
  .warning { color: #e94; }
</style>
</head>
<body>
<header><h2 style="margin:0">Commercial Break Review</h2><span id="progress" class="progress"></span></header>
<div id="app">Loading...</div>
<script>
const app = document.getElementById('app');
const progressEl = document.getElementById('progress');
let currentShow = null;
let queue = [];
let qi = 0;

function renderShowList() {
  progressEl.textContent = '';
  fetch('/api/shows').then(r => r.json()).then(shows => {
    if (shows.length === 0) { app.innerHTML = '<p>No shows ready for review.</p>'; return; }
    app.innerHTML = shows.map(s => `
      <div class="show-row" onclick="openShow(${s.show_id})">
        <span>${s.name} (${s.episode_count} episodes)</span>
        <span>${s.decided_candidates}/${s.total_candidates} decided</span>
      </div>`).join('');
  });
}

function openShow(showId) {
  fetch('/api/show/' + showId).then(r => r.json()).then(data => {
    currentShow = data;
    queue = [];
    for (const ep of data.episodes) {
      for (const b of ep.breaks) {
        queue.push({ep, b});
      }
    }
    qi = queue.findIndex(q => q.b.decision === null);
    if (qi === -1) qi = 0;
    renderCandidate();
  });
}

function renderCandidate() {
  const decided = queue.filter(q => q.b.decision !== null).length;
  progressEl.textContent = `${decided}/${queue.length} decided`;
  if (queue.length === 0) { app.innerHTML = '<p>No candidates.</p>'; return; }
  const {ep, b} = queue[qi];
  let badges = '';
  if (b.is_long) badges += '<span class="badge long">long break &gt;5s -- check span frames</span>';
  if (b.pair_partners.length) badges += `<span class="badge pair">close pair with #${b.pair_partners.join(', #')} -- check gap frames</span>`;
  if (b.is_opening_title) badges += '<span class="badge title">near opening titles -- check for title-sequence flash</span>';

  const frames = b.frames.map(f => {
    const isPre = f.tag.startsWith('pre_');
    const isPost = f.tag.startsWith('post_');
    const isBreakFrame = f.tag === 'mid' || f.tag.startsWith('s') || f.tag.startsWith('gap_s');
    let label;
    if (isPre) label = `-${(b.start - f.t).toFixed(2)}s`;
    else if (isPost) label = `+${(f.t - b.end).toFixed(2)}s`;
    else if (f.tag === 'mid') label = `BREAK ${b.duration}s`;
    else label = f.tag;
    const figClass = isBreakFrame ? 'breakframe' : (isPre || isPost ? 'context' : '');
    const capClass = f.tag === 'mid' ? 'break-label' : '';
    return `<figure class="${figClass}"><img src="${f.url}" title="${f.size}b"><figcaption class="${capClass}">${label}</figcaption></figure>`;
  }).join('');
  const hint = b.vlm_verdict ? `VLM (hint only, not authoritative): ${b.vlm_verdict} / ${b.vlm_confidence}\\n${b.vlm_reasoning || ''}` : 'VLM: no verdict (malformed response) -- decide from frames alone.';

  app.innerHTML = `
    <a class="back" onclick="renderShowList()">&larr; back to shows</a>
    <h3>${currentShow.name} -- ${ep.title || ep.episode_id} <span id="candidate-title">(candidate ${qi+1}/${queue.length})</span></h3>
    <div class="duration">Duration: ${b.duration}s &nbsp; (${b.start.toFixed(2)}s &rarr; ${b.end.toFixed(2)}s)</div>
    <div class="badges">${badges}</div>
    <div class="frame-grid">${frames}</div>
    <div class="hint">${hint}</div>
    <div id="clip-area"></div>
    <div class="buttons">
      <button class="keep" onclick="decide('keep')">Keep (K)</button>
      <button class="reject" onclick="decide('reject')">Reject (R)</button>
      <button onclick="nav(-1)" ${qi===0?'disabled':''}>&larr; prev</button>
      <button onclick="nav(1)" ${qi===queue.length-1?'disabled':''}>next &rarr;</button>
      <button onclick="loadClip()">Play clip (-2s/+2s)</button>
    </div>
    ${decided === queue.length ? '<div style="margin-top:20px"><button onclick="showFinalize()">Finalize show...</button></div>' : ''}
  `;
}

function loadClip() {
  const {ep, b} = queue[qi];
  document.getElementById('clip-area').innerHTML =
    `<video controls autoplay src="/api/show/${currentShow.show_id}/clip/${ep.episode_id}/${b.index}" style="max-width:480px;display:block;margin:8px 0"></video>`;
}

function decide(decision) {
  const {ep, b} = queue[qi];
  fetch(`/api/show/${currentShow.show_id}/decision`, {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({episode_id: ep.episode_id, break_index: b.index, decision})
  }).then(() => {
    b.decision = decision;
    if (qi < queue.length - 1) qi++;
    renderCandidate();
  });
}

function nav(delta) {
  qi = Math.max(0, Math.min(queue.length - 1, qi + delta));
  renderCandidate();
}

document.addEventListener('keydown', (e) => {
  if (!currentShow) return;
  if (e.key === 'k' || e.key === 'K') decide('keep');
  if (e.key === 'r' || e.key === 'R') decide('reject');
  if (e.key === 'ArrowLeft') nav(-1);
  if (e.key === 'ArrowRight') nav(1);
});

function showFinalize() {
  fetch(`/api/show/${currentShow.show_id}/finalize_preview`).then(r => r.json()).then(data => {
    if (data.error) { app.innerHTML = `<p class="warning">${data.error}</p>`; return; }
    let rows = '';
    for (const [epId, breaks] of Object.entries(data.final_breaks)) {
      for (const [bp, rp] of breaks) {
        rows += `<tr><td>${epId}</td><td>${bp.toFixed(2)}</td><td>${rp.toFixed(2)}</td></tr>`;
      }
    }
    const warnings = data.warnings.map(w => `<div class="warning">${w}</div>`).join('');
    const dbWarning = data.already_in_db.length ? `<div class="warning">already has rows for episode_id(s): ${data.already_in_db.join(', ')} -- will refuse to insert</div>` : '';
    app.innerHTML = `
      <a class="back" onclick="renderCandidate()">&larr; back to review</a>
      <h3>Finalize ${currentShow.name} -- ${data.total_confirmed} breaks will be inserted</h3>
      ${warnings}${dbWarning}
      <table class="finalize"><tr><th>episode_id</th><th>break_point</th><th>resume_point</th></tr>${rows}</table>
      <div class="buttons"><button class="keep" onclick="doFinalize()">Confirm insert + finalize</button></div>
    `;
  });
}

function doFinalize() {
  fetch(`/api/show/${currentShow.show_id}/finalize`, {method: 'POST'}).then(r => r.json()).then(data => {
    if (data.error) { alert(data.error); return; }
    alert(`Inserted ${data.inserted}, rejected ${data.rejected}. Show finalized and batch_runs cleaned up.`);
    currentShow = null;
    renderShowList();
  });
}

renderShowList();
</script>
</body>
</html>
"""


@app.route("/")
def index():
    return INDEX_HTML


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=5055)
    args = parser.parse_args()
    app.run(host="127.0.0.1", port=args.port, debug=False)


if __name__ == "__main__":
    main()
