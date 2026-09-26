#!/usr/bin/env python3
"""
PERMANENT SCRIPT — repo root, do not recreate in scratchpad. See COMMERCIAL_BREAK_AUDIT.md.

Verifies rebuild_show.py candidates against extract_break_frames.py's extracted frames using
a local vision-language model (qwen3-vl, served by Ollama) instead of manual/Claude-in-chat
frame review. The prompts encode the charter's decision rules (black frame != commercial
break; title/credit cards vs. real ad pods; long spans need whole-span judgment; close pairs
must be resolved from the frames between them, never position/pattern alone).

"Never Guess" is preserved: any verdict that isn't high-confidence keep/reject, or that comes
from an episode with >=2 suspicious candidates (escalation), is written to
<out_json>.needs_review.json for a human/Claude visual follow-up rather than auto-decided.

Usage:
  python3 verify_breaks_vlm.py <candidates_json> <frames_dir> <out_json> \
      [--model qwen3-vl:30b] [--ollama-host http://localhost:11434] [--workers 4]

Requires: `ollama` python package (see requirements.txt) and a running Ollama daemon with the
model pulled: `ollama pull qwen3-vl:30b`.
"""
import argparse
import json
import os
import re
import statistics
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import ollama

RESULTS_LOCK = threading.Lock()

OPENING_TITLE_WINDOW = 35.0
LONG_BREAK_THRESHOLD = 5.0

# NOTE (2026-09-17): the original prompt asked the model to judge whether a candidate "looks
# like a commercial break" without stating what that means in THIS pipeline. It reasoned from
# generic priors ("ad breaks contain visual content", "too brief for a commercial") and
# confidently rejected/doubted several already-confirmed real breaks in a CHiPs comparison test
# that were solid black, minimum-size (2060-byte) frames -- exactly the genuinely-black pattern
# the charter treats as the positive signal. Rewritten below to state explicitly that solid
# black IS the expected, positive signal here, and to supply the episode's own size baseline
# (per the charter's show/episode-specific size-triage rule) as supporting evidence.
SINGLE_PROMPT = """This candidate already passed automated black-frame and silence detection \
(ffmpeg blackdetect + silencedetect) in a classic TV episode -- by measurement it IS a short, \
quiet, dark segment. The only question is whether it is GENUINELY solid black (a real \
commercial-break cut point -- the moment the network cut to ads, not the ad content itself), or \
whether the detector misfired on something that only looks dark-ish (a dim lit scene, a visible \
face/object, a title/credits card, a scene transition).

Important: in this domain, real commercial-break cut points are typically brief (often just \
2-5 seconds) and are usually solid/uniform black. Short duration and solid blackness are the \
EXPECTED, POSITIVE signal for a real break here -- do NOT reject a candidate just because it is \
short, or because it doesn't show visible "commercial"/ad content itself; the candidate marks \
the cut point, not the ad that follows it.

Reject only when the frame(s) show visible content that contradicts genuine blackness: a \
discernible scene, face, light source, on-screen text, a title/credits card, or a dim/gradient \
(not solid) frame -- i.e. real program content the detector mistakenly flagged.

Candidate: {start:.2f}s to {end:.2f}s ({duration:.2f}s long).
{size_note}
{notes}
Respond with ONLY a JSON object, no other text:
{{"verdict": "keep"|"reject"|"uncertain", "confidence": "high"|"medium"|"low", "reasoning": "<1-2 sentences>"}}
- "keep": the frame(s) are genuinely solid (or near-solid) black -- a real break.
- "reject": the frame(s) show discernible content -- not a real break.
- "uncertain": genuinely ambiguous (e.g. a dim gradient that could go either way) -- never guess.
"""

PAIR_PROMPT = """You are verifying a CLOSE PAIR of detected black-frame candidates in a classic \
TV episode: two candidates only {gap:.2f}s apart ({a_start:.2f}-{a_end:.2f}s and \
{b_start:.2f}-{b_end:.2f}s). Each candidate already passed automated black/silence detection, so \
solid black itself is expected and NOT disqualifying -- the question here is specifically about \
the PAIR relationship, not whether either candidate is "black enough." Real commercial ad pods \
never sit only seconds apart, so a close pair like this is almost always one of:
- both breaks bracket a single title/credits/logo card, and the frames BETWEEN them show that \
card, not program content resuming -- in which case BOTH should be rejected.
- the first is a spurious break right before a real bumper/cut, or a cliffhanger/preview segment \
before real end credits -- in which case only the FIRST should be rejected, the second kept.
- (rare) the frames between clearly show real distinct program content resuming and ending again \
-- both could be legitimately real.

Attached frames include both candidates' own frames plus frames sampled from the gap between \
them. Base the decision on what the gap frames actually show, never on position/duration/pattern \
alone.

Respond with ONLY a JSON object, no other text:
{{"break_a": {{"verdict": "keep"|"reject"|"uncertain", "confidence": "high"|"medium"|"low"}}, \
"break_b": {{"verdict": "keep"|"reject"|"uncertain", "confidence": "high"|"medium"|"low"}}, \
"reasoning": "<what the gap frames show and why>"}}
Use "uncertain" on either side whenever the gap frames don't clearly support one of the patterns \
above -- never guess.
"""


def bounds(b):
    if isinstance(b, dict):
        return b["start"], b["end"]
    return b[0], b[1]


def load_manifest(frames_dir):
    manifest = json.load(open(os.path.join(frames_dir, "manifest.json")))
    by_episode = {}
    for m in manifest:
        by_episode.setdefault(m["episode_id"], []).append(m)
    return by_episode


def frames_for(frames_dir, ep_frames, break_key):
    return [os.path.join(frames_dir, m["file"]) for m in ep_frames
            if str(m["break_index"]) == str(break_key)]


def mid_size_baseline(ep_frames):
    """Show/episode-specific baseline for a genuinely-black midpoint frame's size, per the
    charter's size-triage rule (one absolute threshold doesn't work across shows/encoders)."""
    sizes = [m["size"] for m in ep_frames if m.get("tag") == "mid" and m.get("size")]
    return round(statistics.median(sizes)) if sizes else None


def size_hint(sizes, baseline):
    """Qualitative size-triage note, NOT exact numbers. An earlier version handed the model raw
    byte counts and it started doing naive arithmetic ("2082 > 2056 -> reject") on compression
    noise a few bytes wide instead of looking at the image -- exactly the "size as proof" mistake
    the charter warns against. Only flag genuinely large size ratios, and always as a soft hint."""
    if not sizes or not baseline:
        return "No size baseline available for this episode -- judge from the image alone."
    ratio = max(sizes) / baseline
    if ratio < 1.3:
        return ("This candidate's frame size is consistent with this episode's genuinely-black "
                "baseline (normal compression variance) -- no size-based red flag.")
    if ratio < 2.5:
        return ("This candidate's frame size is somewhat larger than this episode's genuinely-"
                "black baseline -- worth a closer look, but inconclusive from size alone.")
    return ("This candidate's frame size is SIGNIFICANTLY larger than this episode's genuinely-"
            "black baseline -- a real red flag for non-black content, but still confirm visually.")


def parse_json_response(text):
    text = (text or "").strip()
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except json.JSONDecodeError:
        return None


def _ensure_verdict(v):
    if not isinstance(v, dict) or "verdict" not in v:
        return {"verdict": "uncertain", "confidence": "low", "reasoning": "malformed model response"}
    v.setdefault("confidence", "low")
    return v


def verify_single(client, model, frames, start, end, start_point, baseline):
    notes = ""
    if start - start_point <= OPENING_TITLE_WINDOW:
        notes += ("This candidate is within the opening-title window -- check for an opening "
                  "black flash inside the title sequence rather than auto-accepting.\n")
    if (end - start) > LONG_BREAK_THRESHOLD:
        notes += ("This is a LONG candidate (>5s) -- long dark spans can be dim scenes, title "
                  "cards, credits, or transitions, not proof of a real break. Judge the whole "
                  "span, not just one frame.\n")
    sizes = [os.path.getsize(f) for f in frames]
    prompt = SINGLE_PROMPT.format(start=start, end=end, duration=end - start, notes=notes,
                                   size_note=size_hint(sizes, baseline))
    resp = client.chat(model=model,
                        messages=[{"role": "user", "content": prompt, "images": frames}],
                        format="json")
    parsed = parse_json_response(resp["message"]["content"])
    return _ensure_verdict(parsed)


def verify_pair(client, model, frames, a_start, a_end, b_start, b_end):
    prompt = PAIR_PROMPT.format(gap=b_start - a_end, a_start=a_start, a_end=a_end,
                                 b_start=b_start, b_end=b_end)
    resp = client.chat(model=model,
                        messages=[{"role": "user", "content": prompt, "images": frames}],
                        format="json")
    parsed = parse_json_response(resp["message"]["content"]) or {}
    parsed["break_a"] = _ensure_verdict(parsed.get("break_a"))
    parsed["break_b"] = _ensure_verdict(parsed.get("break_b"))
    return parsed


def process_episode(client, model, frames_dir, ep, ep_frames):
    breaks = ep.get("breaks", [])
    start_point = ep.get("start_point") or 0.0
    baseline = mid_size_baseline(ep_frames)
    verdicts = [None] * len(breaks)

    ordered = sorted(range(len(breaks)), key=lambda i: bounds(breaks[i])[0])
    paired = set()
    for a, b in zip(ordered, ordered[1:]):
        a_start, a_end = bounds(breaks[a])
        b_start, b_end = bounds(breaks[b])
        if 0 <= b_start - a_end < LONG_BREAK_THRESHOLD:
            gap_frames = frames_for(frames_dir, ep_frames, f"{a}-{b}")
            own_frames = frames_for(frames_dir, ep_frames, a) + frames_for(frames_dir, ep_frames, b)
            result = verify_pair(client, model, own_frames + gap_frames, a_start, a_end, b_start, b_end)
            reasoning = result.get("reasoning")
            verdicts[a] = dict(result["break_a"], pair_reasoning=reasoning)
            verdicts[b] = dict(result["break_b"], pair_reasoning=reasoning)
            paired.add(a)
            paired.add(b)

    for bi, b in enumerate(breaks):
        if bi in paired:
            continue
        start, end = bounds(b)
        frames = frames_for(frames_dir, ep_frames, bi)
        if not frames:
            verdicts[bi] = {"verdict": "uncertain", "confidence": "low",
                             "reasoning": "no extracted frames found for this candidate"}
            continue
        verdicts[bi] = verify_single(client, model, frames, start, end, start_point, baseline)

    n_bad = sum(1 for v in verdicts if v["verdict"] in ("reject", "uncertain"))
    if n_bad >= 2:
        for v in verdicts:
            if v["verdict"] == "keep":
                v["escalated"] = True
                v["escalation_reason"] = ">=2 suspicious candidates in this episode; full re-review required"

    return verdicts


def needs_review(v):
    return v["verdict"] == "uncertain" or v.get("confidence") != "high" or v.get("escalated")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("candidates_json")
    parser.add_argument("frames_dir")
    parser.add_argument("out_json")
    parser.add_argument("--model", default="qwen3-vl:30b")
    parser.add_argument("--ollama-host", default="http://localhost:11434")
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()

    data = json.load(open(args.candidates_json))
    frames_by_episode = load_manifest(args.frames_dir)
    client = ollama.Client(host=args.ollama_host)

    results = {episode_id: dict(ep) for episode_id, ep in data.items()}
    review_queue = []
    total = sum(1 for ep in data.values() if ep.get("breaks"))
    done = 0

    def save():
        with open(args.out_json, "w") as f:
            json.dump(results, f, indent=2)
        with open(args.out_json + ".needs_review.json", "w") as f:
            json.dump(review_queue, f, indent=2)

    def run_episode(episode_id, ep):
        ep_frames = frames_by_episode.get(episode_id, [])
        verdicts = process_episode(client, args.model, args.frames_dir, ep, ep_frames)
        return episode_id, verdicts

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(run_episode, episode_id, ep): episode_id
                   for episode_id, ep in data.items() if ep.get("breaks")}
        for future in as_completed(futures):
            episode_id, verdicts = future.result()
            ep = data[episode_id]
            out_breaks = []
            for bi, b in enumerate(ep.get("breaks", [])):
                b = dict(b) if isinstance(b, dict) else {"start": b[0], "end": b[1]}
                v = verdicts[bi]
                b["vlm_verdict"] = v["verdict"]
                b["vlm_confidence"] = v.get("confidence")
                b["vlm_reasoning"] = v.get("reasoning") or v.get("pair_reasoning")
                if v.get("escalated"):
                    b["vlm_escalated"] = True
                    b["vlm_escalation_reason"] = v.get("escalation_reason")
                out_breaks.append(b)
                if needs_review(v):
                    review_queue.append({"episode_id": episode_id, "break_index": bi, **b})
            with RESULTS_LOCK:
                results[episode_id]["breaks"] = out_breaks
                done += 1
                n_keep = sum(1 for b in out_breaks if b["vlm_verdict"] == "keep")
                print(f"  [{done}/{total}] episode {episode_id}: {n_keep}/{len(out_breaks)} kept")
                save()

    n_total = sum(len(ep.get("breaks", [])) for ep in results.values())
    n_kept = sum(1 for ep in results.values() for b in ep.get("breaks", []) if b.get("vlm_verdict") == "keep")
    n_rejected = sum(1 for ep in results.values() for b in ep.get("breaks", []) if b.get("vlm_verdict") == "reject")

    print("\n--- Summary (draft end-of-show report fields) ---")
    print(f"Episodes with candidates processed: {done}/{total}")
    print(f"Total candidates: {n_total}")
    print(f"Kept: {n_kept}")
    print(f"Rejected: {n_rejected}")
    print(f"Needs human review (uncertain/low-or-medium-confidence/escalated): {len(review_queue)}")
    print(f"Wrote {args.out_json} and {args.out_json}.needs_review.json")


if __name__ == "__main__":
    main()
