# Commercial Break Audit

## CHARTER (2026-09-14) — authoritative operating rules, supersedes informal practice below

This section is the standing charter for this project. Everything below it in this file is
history/detail; when anything below conflicts with this charter, the charter wins. This was
given verbatim by the user on 2026-09-14 and must never be silently changed — any amendment
requires the same "explain, propose, ask if ambiguous/policy-level, then record" process this
charter itself describes.

**Role:** TV Commercial-Break Detection, Verification, and Scheduling Expert for this repo.
Job: identify every commercial break in every episode, verify those decisions against actual
video frames, maintain the detection/verification tooling, and produce an accurate final
commercial-break schedule. This is an evidence-driven forensic task, not pattern-matching or
statistical classification.

**Priorities, in order:** Accuracy > Complete coverage > Evidence-based decisions >
Reproducibility > Auditability > (only then) Speed. Never silently guess when evidence is
ambiguous.

**Core operating principle — Never Guess.** If a candidate break's evidence is ambiguous,
conflicting, unusual, or insufficient: stop at that decision point, explain exactly what's
ambiguous, show the relevant evidence/frames/timestamps, state the competing interpretations,
and ask the user to decide. Do not silently pick the "probably correct" interpretation.
Continue with independent work that doesn't depend on the answer whenever practical. Any new
methodology/threshold/policy/interpretation/correction the user gives must be applied AND
recorded in this file in the same turn — never "I'll remember this" or "I'll document it
later." Do not rely on conversation memory for methodology.

**Detection (`rebuild_show.py`) — mandatory threshold levels.** Every episode runs all four
levels, always, never stopped early because an earlier level found "enough":
- Level 0: blackdetect d=0.8/th=0.1, silencedetect db=50 (defaults)
- Level 1: blackdetect d=0.8/th=0.1, silencedetect db=35
- Level 2: blackdetect d=0.3/th=0.15, silencedetect db=35
- Level 3: blackdetect d=0.15/th=0.25, silencedetect db=30
Do not optimize any level away; do not decide a show "obviously doesn't need" a level.

**Candidate construction.** Take the union of every level's silence-confirmed candidates.
Deduplicate candidates within 2 seconds of each other into one logical candidate, preserving
the bounds from the STRICTEST level that found it, and retaining provenance (which level(s)
detected it) in the output wherever practical — no arbitrary averaging/recomputing of bounds.

**Black-bounded timing rule.** `merge_segments_black_bounded` is authoritative: break/resume
timing comes only from the black segment's own bounds, NEVER from silence timing (silence only
*confirms* a candidate is real). This specifically prevents resume-overshoot and
break-undershoot. Never reintroduce silence-derived timing anywhere in the pipeline.

**Edge filtering.** `filter_edges` discards candidates sitting directly at `start_point`/
`end_point` — an episode boundary must never masquerade as a break. Handle floating-point/
encoding timing noise consistently with the repo's established edge semantics.

**Parallel processing.** 8 workers, always. Do not reduce to serial "for convenience." If a
technical constraint prevents 8 workers, report the constraint explicitly rather than silently
changing the methodology.

**Verification — 100% coverage, never sampling.** Every surviving candidate in every episode
is frame-checked. No statistical sample, no "representative subset," no "these look similar
enough to skip."
- *Midpoint frame first, but not visual-only*: extract the midpoint frame, compare its file
  size against a SHOW-SPECIFIC baseline (genuinely black frames compress very uniformly; one
  absolute threshold does not work across shows/encoders/resolutions) to triage outliers.
- *Outliers*: any size outlier must be viewed at full resolution — size alone is triage, never
  final proof.
- *Long breaks (>5s)*: automatically suspicious. Pull additional sample points across the
  entire span, not just the midpoint — long dark intervals can be dim scenes, title cards,
  credits, transitions, or genuinely dark content, not proof of a real break.
- *Opening-title black flashes*: any candidate within ~30-35s of `start_point` must be
  specifically checked for a black flash inside the opening title sequence — never
  auto-accepted; inspect the surrounding context.
- *Escalation*: if more than one candidate in the same episode looks bad/suspicious/
  inconsistent, escalate to checking 100% of that episode's remaining breaks, not just the
  flagged ones.
- *Close-pair rule*: any two breaks in the same episode under ~5s apart require pulling and
  viewing the frames BETWEEN them — never resolved by position, expected patterns, episode
  templates, historical assumptions, "this usually happens here," or duration alone. Known
  outcomes: delete both (bracketing a title/credit card), delete first only (spurious break
  before a real bumper/cut, OR a cliffhanger/preview segment before real end credits —
  confirmed on Batman). A pattern seen before is useful context, never evidence that another
  episode matches it — inspect the actual frames every time.
- *Contextual verification*: a black frame ≠ a commercial break. Black frames can be scene
  transitions, title cards, credits, opens/closes, bumpers, network transitions, intentionally
  dark material, or encoding artifacts. A real break also need not look visually identical
  across episodes. Use surrounding frames/context; the goal is real break boundaries, not
  merely black intervals.

**Existing data is not automatically correct.** Treat existing `commercial_breaks_v2` rows as
claims to verify, not ground truth. Fix mislabeled/mistimed/false-positive/missing/duplicated/
incorrect rows immediately, without waiting for approval — but call out in the report exactly
what was wrong, what changed, why, and what evidence supported it.

**Audit log discipline.** This file is the authoritative methodological record. Any
methodology/threshold/verification rule/policy decision/exception/interpretation affecting this
project is written here in the SAME TURN it's decided — never deferred. Preserve enough detail
for another engineer to understand what was used, why, what changed, when, what failure mode
prompted it, what was decided manually, and what evidence supported it. Never rewrite history
to obscure prior decisions — record changes and their reasons, don't erase the old entry.

**Permanent repository scripts.** `rebuild_show.py` and `extract_break_frames.py` live
permanently at the repo root. Never only in scratchpad/temp/notebook/ephemeral workspace. Keep
them reproducible and runnable by another engineer; don't leave the authoritative
implementation only in ad-hoc command history.

**Finalization requirements — a show is not done just because candidates were detected and
verified.** Before declaring done: run the close-pair scan against the FINAL surviving set;
run a relative-position sanity check against the FINAL surviving set (unusual position =
investigate, not automatic deletion); reconcile all four levels' union, dedup, black-bounded
timing, edge filtering, frame verification, long-break sampling, opening-title checks,
close-pair checks, escalated-episode checks, and corrections to existing rows — the final
surviving set must be traceable back to the evidence.

**No silent methodology changes**, ever, to: thresholds, merge windows, dedup rules, frame
sampling rules, black/silence semantics, edge handling, worker count, verification
requirements, or confidence criteria. If a change seems necessary: explain the problem, propose
the change, ask if genuinely ambiguous/policy-level, then (once decided) record it here and
apply consistently. No undocumented "small improvements."

**Evidence hierarchy** (prefer top to bottom when evidence conflicts): (1) direct inspection of
actual video frames, (2) multiple surrounding frames showing the transition/context, (3)
black-segment boundaries, (4) independent agreement between detection levels, (5) audio
silence, (6) relative-position/structural patterns, (7) existing database rows, (8) assumptions
about how TV episodes normally behave. Patterns and existing data are clues, never substitutes
for video evidence.

**Confidence and escalation.** Routine decisions with clear evidence: proceed. Ask the user
when: frames genuinely support two plausible interpretations; a break could reasonably be
either commercial or program content; the boundary can't be reliably determined; a methodology
change is required; two existing rules conflict; evidence is corrupted/unavailable; the answer
would require inventing information; or an unusual case doesn't fit established policy. When
asking, be precise — not "I'm not sure," but: episode/candidate timestamps, duration, which
detection levels found it, relevant frame evidence, surrounding context, competing
interpretations, a recommended interpretation if one exists, and exactly what decision is
needed.

**End-of-show report must contain:** episodes processed/completed/failed; total candidates
before and after dedup; total final surviving breaks; candidates rejected; candidates requiring
escalation; existing rows corrected; unusual episodes; methodology/policy changes; confirmation
all 4 levels ran on every episode; confirmation every final candidate was frame-checked;
confirmation long breaks got additional sampling; confirmation close pairs were inspected;
confirmation the final close-pair scan and final relative-position sanity check were run;
anything still requiring a user decision.

**Definition of Done** (a show may only be called DONE when ALL are true): all episodes
processed; all 4 threshold levels ran on every episode; candidates unioned across levels;
within-2s candidates deduped correctly with strictest-level bounds preserved; break/resume
timing from black-segment bounds only; episode-edge candidates filtered; 8 workers used where
technically possible; every candidate frame-checked; show-specific size-baseline triage
performed; all size outliers viewed at full resolution; every >5s break got additional
span sampling; candidates within ~30-35s of start_point checked for opening-title flashes;
episodes with multiple suspicious breaks got full remaining-break verification; every close
pair under ~5s inspected via intervening frames; incorrect existing rows corrected and reported;
all methodology/policy decisions written to this file; `rebuild_show.py` and
`extract_break_frames.py` exist at repo root; final close-pair scan run; final relative-position
sanity check run; final results evidence-traceable; no unresolved ambiguity remains unless
explicitly presented to the user.

**100% compliance verification note (2026-09-14):** When the charter above was first given,
Batman (153) and Family Affair (154) had already been marked done using a spot-check-and-
extrapolate shortcut on their large-size-outlier frames (viewing 4-5 representative frames per
show across the size/episode range, then bulk-excluding the rest of that band on the strength of
the size pattern) instead of individually viewing every one. That is non-compliant with "every
surviving candidate... is frame-checked... no representative subset." Per user instruction, both
shows were corrected: all 121 Batman and all 74 Family Affair large-size-outlier frames were
individually viewed at full resolution. Result: every single one was confirmed real content,
matching the original spot-check's conclusion exactly — zero additional corrections were needed
to either show's final row count. This is recorded so the shortcut is not reintroduced elsewhere:
size triage may be used to prioritize what to look at, never to excuse not looking at something.

**Automated frame verification via local VLM (2026-09-17):** The per-candidate frame-check
decision (previously made by Claude visually inspecting each extracted frame in chat) can now
be delegated to a local vision-language model — qwen2.5vl:7b served by Ollama — via the new
permanent script `verify_breaks_vlm.py`, to cut the cost of auditing hundreds of episodes.
This does NOT relax "Never Guess": the model must emit a confidence level per verdict
(high/medium/low) alongside keep/reject/uncertain, and anything not high-confidence is
written to `<out.json>.needs_review.json` for an actual human/Claude visual follow-up rather
than being silently decided. The escalation rule (>=2 suspicious candidates in an episode ->
escalate the whole episode) is implemented by force-flagging every "keep" verdict in such an
episode for review too, regardless of its own confidence. Close pairs (<5s apart) are decided
by a combined prompt over both candidates' frames plus new gap-sample frames extracted by
`extract_break_frames.py` between them (added in this same change), never independently.
Before trusting this on any not-yet-verified show, run it against a show that's already been
fully manually verified and compare its verdicts against that show's final, human-confirmed
`commercial_breaks_v2` rows to get a concrete agreement-rate baseline.

**Default model changed to qwen3-vl:30b (2026-09-17):** After a smoke test on this machine
(Apple M5 Pro, 48GB unified memory), `qwen2.5vl:7b` was replaced as `verify_breaks_vlm.py`'s
default with `qwen3-vl:30b` (a newer-generation, ~20GB MoE model — Ollama reports it loading
100% on-GPU via Metal). Reason: on an identical black-frame test prompt, `qwen2.5vl:7b`
hallucinated unrelated visual detail (a "white circle" on a plain black test image) in a
free-text description and gave a differently-reasoned verdict than `qwen3-vl:30b` on the same
synthetic candidate; `qwen3-vl:30b`'s reasoning was more directly tied to the actual prompt
criteria (break duration vs. typical transition length). This is one anecdotal comparison, not
a benchmark — the real validation is still the agreement-rate test above, which has not yet
been run against a fully human-verified show. `qwen2.5vl:7b` was subsequently removed from this
machine (`ollama rm qwen2.5vl:7b`) to free disk space; re-pull it with `ollama pull
qwen2.5vl:7b` if a faster/lighter-weight run is ever needed again.

**Real agreement-rate test run + two prompt bugs found and fixed (2026-09-17):** Ran
`verify_breaks_vlm.py` (qwen3-vl:30b) against show 124 (CHiPs, 10 episodes, already DONE, 49
known-correct rows in `commercial_breaks_v2`) as the validation the entries above called for.
Re-running `rebuild_show.py` found 65 raw candidates this time (vs. 50 in the original 2026-09-14
pass — expected, detection methodology has evolved since then). Three iterations:
1. **Original prompt: catastrophic failure.** Only 3/65 kept. Cross-checked episode 4650 against
   the DB directly: 5 of its 7 candidates are confirmed-real breaks, all a uniform, minimum-size
   (2060-byte) solid-black frame — and the model confidently (`high` confidence) rejected 3 of
   those 5, calling solid black frames "ambiguous" or reasoning that "commercial breaks contain
   visual content" / "are longer segments." Root cause: the prompt never stated that a candidate
   here IS the black/silent cut point itself (not the ad content), so the model reasoned from a
   generic, wrong prior about what "commercials" look like.
2. **Fix 1 — reframed the task, told the model solid black + short duration is the EXPECTED
   positive signal here, not a red flag.** Also added each episode's own genuinely-black
   midpoint-frame size as a size-triage hint (per the charter's show-specific-baseline rule).
   Result: 46/65 kept, 91.2% accuracy on high-confidence verdicts, 0 false-keeps, but 3 confident
   false-**rejects** remained.
3. **Fix 2 — found a second bug from fix 1 itself: giving the model exact byte counts caused it
   to reject solid-black frames over compression noise a few bytes wide** (e.g. "2082 bytes
   exceeds baseline 2056 bytes → reject", on a frame confirmed solid black by direct inspection).
   The charter already warns size is triage only, never proof — handing over raw numbers invited
   naive arithmetic instead of visual judgment. Replaced with a qualitative `size_hint()` bucket
   (consistent-with-baseline / somewhat-larger-worth-a-look / significantly-larger-red-flag,
   ratio-based, no raw bytes in the prompt). Result: 54/65 kept, 93.8% accuracy on high-confidence
   verdicts, **0 false-rejects**, 3 confident false-**keeps** remained, 17/65 (26%) correctly
   routed to `needs_review.json`.

**Residual, non-prompt-fixable failure mode identified:** the 3 remaining false-keeps (episode
4668, candidates at 951.4s/2245.3s/2759.9s) were individually viewed at full resolution — all 3
are genuinely dim, low-contrast frames with a faint but discernible shape/silhouette, not truly
solid black. The model reported "no discernible content" and missed them at `high` confidence.
This is the same dim-night-scene-misread-as-black pattern already documented elsewhere in this
file for other crime/detective shows (Rockford Files, Cannon, Kojak) — CHiPs is a police show
with night-driving scenes, so it is not surprising it reproduces here. Two of the three had a
size ratio (1.51x, 1.52x baseline) that should have triggered extra scrutiny but the model still
called them confidently clean; the third (1.09x) had no size signal at all. This looks like a
genuine visual-perception limit of the model on subtle low-contrast content, not something more
prompt wording will reliably fix.

**Standing rule going forward (superseded below, 2026-09-17 — kept for history):** because this
specific failure mode (dim scene passed as a confident "keep") is real and repeatable, do not
blindly bulk-insert every high-confidence "keep" verdict from `verify_breaks_vlm.py` for a
night-driving/dim-lighting-heavy show (crime dramas, detective shows, anything with frequent
low-key/night scenes) without at least a light spot-check of the "keep" set, not just the
`needs_review.json` queue — the same caution the charter already applies to human-driven audits
of this show genre. Shows with little dim/night content are lower-risk for this particular
failure mode.

**Standing rule corrected/broadened (2026-09-17) — genre hypothesis refuted:** ran the same
spot-check on show 231 (Quark, 7 episodes, sci-fi comedy — no prior DB rows, so this was a real
first-pass run, not a re-validation). Total 48 candidates, 29 kept, of which only 9 were
unflagged high-confidence keeps (the rest were escalated/lower-confidence and already in
`needs_review.json`). Randomly sampled 6 of the 29 kept candidates and viewed them at full
resolution: 1 (episode 11550, candidate idx4) is a clear false positive — visibly discernible
dark shapes/objects in the frame, not solid black; 1 more (episode 11548, idx0) is borderline,
with a faint visible shape. Both were in the small "trusted" bucket of 9, not already flagged.
Neither had a size ratio the `size_hint()` triage would catch (1.16x and 1.08x of baseline,
both "consistent with baseline").

This refutes the CHiPs-based hypothesis that this failure mode is specific to crime/detective
shows with night-driving scenes. Quark is a spaceship-set sci-fi comedy with no crime/detective
content, yet reproduces the same failure at a comparable or worse rate. The real pattern is
broader: **any show with plausible dark-environment content** (night scenes, space/interior
shots, dim rooms — not just detective-show night driving) can produce dim-but-not-solid-black
frames the model misreads as clean black at high confidence, and file size alone will often not
catch it.

**Corrected standing rule:** do not restrict the "spot-check the keep pile" precaution to
crime/detective shows. Apply it to any show, unless/until a specific show's content is known to
be uniformly bright (e.g. a daytime game show, a brightly-lit sitcom set) — and even then, treat
that as a lower-risk assumption, not a guarantee, since it hasn't been directly tested. When the
"trusted" (unflagged, high-confidence keep) bucket for a show is small (roughly a dozen or
fewer), it is cheaper to just review all of them directly rather than spend effort deciding
whether to spot-check a sample.

**Policy correction (2026-09-17) — user directive: accuracy is the default, not cost; never
just a sample.** Full manual review was done of ALL 48 Quark candidates (not a sample): the 9
unflagged high-confidence keeps, all 24 items already in `needs_review.json`, AND (per this
directive) all 15 unflagged high-confidence rejects that would otherwise never have been looked
at. Result: 18 real breaks (keep), 30 false positives (reject). The model's own verdicts were
right on 46/48 (95.8%) individually, but the 2 wrong ones were both in the "trusted, unflagged
keep" bucket that nothing would have caught automatically — while all 15 unflagged rejects and
everything already flagged were correct. Given this, the earlier framing of "spot-check keeps
for risky shows, trust the rest" is superseded:

**Current policy: `verify_breaks_vlm.py`'s verdicts are triage/pre-sort only, not a decision.**
No candidate — keep OR reject, any confidence level, any show — goes into `commercial_breaks_v2`
without an actual human/Claude visual review of every candidate, the same 100%-coverage
discipline the charter has always required of manual audits. The VLM's value is speeding up that
review (surfacing likely-correct vs. likely-wrong groupings, pre-sorting what to look at first)
and providing a first-pass estimate, not replacing the review itself. This does not eliminate
the cost-reduction goal this tool was built for — reviewing 48 pre-sorted, pre-reasoned
candidates is still much faster than the old fully-manual process — but it stops short of
letting the model insert anything unreviewed.

**2026-09-18 addendum — cheap-model delegation tested, rejected.** Ran a validation test (same
method as the qwen3-vl agreement-rate test above): a Haiku agent independently reviewed all 106
frame candidates for show 224 (Tabitha), already hand-verified by Claude/Sonnet. Result: 84%
agreement (89/106). 14 of 17 disagreements were Haiku false-KEEPs (the dangerous direction —
would insert a bad break), and 13 of those 14 were the exact same failure mode: a recurring
~44s photo-album title-sequence transition frame that only resolves correctly by pulling extra
context frames from the source video (before/after the candidate) — Haiku, working only from
the single provided mid-frame image, had no way to catch it. Conclusion: do not delegate
frame-review verdicts to a cheaper model as a cost-saving measure — its accuracy is fine on
easy/isolated frames but has a systematic blind spot on exactly the subtle cases (recurring
title/bumper patterns, dim-scene misreads) that this audit exists to catch. The local VLM
(qwen3-vl:30b) triage-only policy above remains the only approved automation shortcut.

**2026-09-23 finding — a show marked `yes` in the table below can still need real new work; don't
trust that column alone.** Started Police Squad (`show_id 84`) assuming it needed full review
since it wasn't obviously in `batch_runs/batch_summary.json`'s `done` list under a name I
recognized — but it was already marked `| 84 | Police Squad | 6 | yes |` in the table below.
Reviewed anyway (2 of its 6 episodes, 2724/2726, already had all confirmed-keep breaks in
`commercial_breaks_v2` — inserting again would have duplicated rows, caught before committing),
and found genuinely new breaks that weren't in the DB yet for 2 more episodes (2725, 2727). Root
cause: `show_id 84` falls in CLAUDE.md's `detection_risk_1_125` bucket — the `yes` reflects an
*older*, since-superseded detection pass, not the newer, more thorough `batch_runs/84` run (dated
2026-09-17) sitting unreviewed. **The right signal to check first is `show_reprocess_queue.json`'s
classification** (`detection_risk_1_125`/`126_152` = old under-detecting pass, needs redo;
`done_153_plus` = fine), not just this table's `yes`/`no` column or whether a show appears in
`batch_summary.json`. Regardless of classification, always check `commercial_breaks_v2` directly
per episode_id before assuming a show needs full review from scratch or none at all.

**2026-09-23 pipeline bug found — `rebuild_show.py` assigned one episode's detection data to a
different episode.** While reviewing Police Squad, `episode_id 2729`'s (`Testimony of Evil`)
candidates in `batch_runs/84/candidates.json` turned out to be byte-for-byte identical (same
timestamps, and confirmed via matching frame images) to `episode_id 2727`'s (`Rendezvous at Big
Gulch`) — i.e. 2729's "candidates" actually describe 2727's video, not its own. Checked the other
4 episodes in this same batch run (2724, 2725, 2726, 2728) — all have distinct, independently
plausible timestamps and confirmed-distinct frame content, so this looks isolated to this one
episode/run rather than a systemic bug, but the root cause (something in `rebuild_show.py`'s
per-episode file-to-candidate mapping) hasn't been found yet. **Do not review or insert anything
from a `candidates.json` entry without spot-checking that its frames actually show plausible/
distinct content for that specific episode first** — this is a new, narrower version of "never
guess": don't trust the candidate-to-episode mapping itself as a given. Show 84 episode 2729
needs `rebuild_show.py` + `extract_break_frames.py` re-run for just that episode before it can be
reviewed. Flagging rather than guessing which of its 11 "candidates" (if any) might coincidentally
still be valid — none are trustworthy until regenerated from the correct source file.

**2026-09-23 audit — 22 `yes`-marked shows have zero rows in `commercial_breaks_v2`.** Cross-
checked every show marked `yes` in the table below against actual `commercial_breaks_v2` content
(not just the table's own say-so — the Police Squad finding above shows why). Two of the 22
(School House Rock, French Chef) were explicitly marked done without review this session per
user decision (their break counts were negligible/not worth reviewing — user call, not a gap).
Of the remaining 20: **16 have data only in the legacy `commercial_breaks` table** (Atom Ant,
Popeye the Sailor, Felix the Cat, Secret Squirrel, Ant and the Aardvark, Squiddly Diddly, Wally
Gator, Tijuana Toads, Roger Ramjet, Lippy the Lion, Ricochet Rabbit, New Adventures of Superman,
Pink Panther Show, Bullwinkle Show, Wacky Races, ThunderCats) — finalized before
`commercial_breaks_v2` and the current frame-check charter existed, never migrated/re-verified.
**4 have zero rows in both tables**: Dudley Do-Right Show (51), Bill Nye the Science Guy (92),
Cosmos (242), Star Cops (373). Per user: Bill Nye and Cosmos are PBS (non-commercial broadcaster)
— zero real breaks is expected/correct for those, not a gap. Dudley Do-Right Show isn't PBS, but
per user, short cartoon-anthology shows also legitimately have no commercial breaks in this
collection (matches the pattern already established for Bucket B cartoon-package shows earlier
this file) — also expected/correct, not a gap. Star Cops (373) initially looked like the one
confirmed real gap, but per user it's BBC/UK — same "genuinely commercial-free" explanation as
the PBS/cartoon shows, confirmed by review (37 of 46 candidates checked, all false positives,
zero kept) before user flagged the reason and review was stopped. **All 22 originally-flagged
`yes`-with-zero-rows shows are now accounted for** — 16 legacy-only (need v2 migration someday
if ever prioritized), 6 genuinely commercial-free (PBS/cartoon/UK), 2 explicit user skip
decisions (School House Rock, French Chef).

**2026-09-25 finding — Manimal (`show_id 266`) had a dramatically higher VLM false-"keep" rate
than any show reviewed so far, concentrated almost entirely in the pilot/TV-movie episode.**
Full frame-by-frame review of all 128 raw candidates across all 8 episodes (untouched show,
first pass): 45 confirmed real breaks inserted into `commercial_breaks_v2`, 83 rejected. The
regular 7 half-hour episodes behaved normally (30-70% keep rate, consistent with other shows in
this audit). The 39-candidate pilot/TV-movie episode (`episode_id 13091`, ~71.6min runtime) was
a severe outlier: only **2 of 39** candidates were genuine breaks (both within the final ~40s of
the episode), and the VLM's own "keep" verdicts were wrong on the large majority of them —
repeatedly calling clearly-visible content (faces, a car, a building facade, a staircase, a
bedroom scene, panther close-ups) "solid black, no discernible content" at *high* confidence.
Two candidates (episode 13091's #26, and episode 13095's #8) had self-contradictory VLM output:
the model's own `vlm_reasoning` text described visible program content ("two people
interacting", "a close-up of a character") while still emitting a `keep` verdict — a distinct
bug from the already-documented dim-scene-misread pattern, since here the model's stated
observation and its verdict directly disagreed with each other.

Given the pilot's very low final break count is concentrated entirely near the episode's true
end, this matches the already-documented "cleanly re-edited print, most ad-break duration/
evidence removed" pattern (see the Shaft finding above) rather than a detection gap — not
escalating further, but flagging since a 71-minute single episode with only 2 breaks is
positionally unusual per the charter's relative-position check and should be remembered as
precedent rather than assumed to be a review error if seen again.

**Practical implication for remaining `batch_runs/` output:** do not treat any VLM "keep"
verdict on this show/print type as reliable pre-sort without full visual confirmation (already
required for every candidate regardless — this is not a policy change, just a reminder that this
show's automated pre-sort is unusually unreliable, so budget full review time accordingly rather
than expecting the trusted-bucket shortcuts that worked on cleaner-print shows).

**Session-to-session pointer:** Police Squad (`show_id 84`) re-reviewed against the newer
`batch_runs/84` detection pass as of 2026-09-23 — episodes 2724, 2725, 2726, 2727, 2728 fully
reviewed/finalized (new confirmed breaks found and inserted for 2725 and 2727 beyond what the
old `yes`-marked pass had). Episode 2729 still blocked on a `rebuild_show.py` +
`extract_break_frames.py` re-run (see the pipeline-bug finding above) before it can be reviewed
— table entry below left as `no` until that's done. Star Cops (373) resolved (see finding above,
legitimately commercial-free, marked `yes`). Otherwise: pick another `detection_risk_1_125`/`126_152` show from
`show_reprocess_queue.json` (the actual signal for "needs the newer detection pass," not just
table `no`/absence from `batch_summary.json`), or a genuinely `untouched`-classified show
cross-checked against `commercial_breaks_v2` directly (see the finding two entries up about not
trusting classification files/tables alone).

**2026-09-21 data-quality fix — 3 Thriller episodes were mis-filed under Gunsmoke's `show_id`
(144).** Found while testing unrelated scheduler changes. `episode_id`s 11870/11871/11872
(`Thriller_The_Innocent_Bystanders.mp4` S2E28, `Thriller_The_Storm.mp4` S2E18,
`Thriller_Waxworks.mp4` S2E16 — all correctly numbered/dated for Thriller's real season 2, all
free slots) had `show_id = 144` (Gunsmoke) instead of the existing `show_id = 211` (Thriller).
Reassigned `show_id` to 211; no numbering collisions resulted. Gunsmoke's row count in the table
below drops from 118 to 115 (still fully done — those 3 were never actually Gunsmoke episodes).
These 3 episodes already carry `commercial_breaks`/`commercial_breaks_v2` rows from whatever pass
audited them as "Gunsmoke" — that break data is keyed by `episode_id`, not `show_id`, so it
moved with them and did not need to be redone. Thriller (211, 55 episodes total) is otherwise
**untouched** — added to the table below as not-done; only those 3 episodes have any break data.
`show_reprocess_queue.json` does not list show 211 at all and is stale for this and any other
changes made this session — regenerate it before trusting it for batch runs.

**2026-09-21 data-quality fix — Charlie's Angels "Angels in Paradise" two-parter shared one
episode_number.** `episode_id 11769` ("Angels in Paradise Part 2", `episode_is_multipart = 2`)
had `episode_number = 1.0`, colliding with Part 1 (`11768`, `episode_is_multipart = 1`, also
`episode_number = 1.0`). This broke `Episodes._handle_multipart_continuation()`'s lookup, which
matches on `episode_number = <current> + 1` — Part 2 could never be found as Part 1's
continuation. `episode_number 2.0` was free for `show_id 108` season 2 (the rest of the season
was already numbered 3.0, 4.0, 5.0... as if Part 2 occupied slot 2), so corrected Part 2's
`episode_number` to `2.0`. No other data touched (`commercial_breaks`/`commercial_breaks_v2` for
both parts are keyed by `episode_id`, unaffected); no collision resulted.

**2026-09-21 data-quality fix (partial) — French Chef (`show_id 338`) episode_number collisions,
true numbering left unresolved.** 30 of French Chef's 36 episodes use zero-padded filenames
(`French_Chef_S0#E##.mp4`) and appear to follow one internally-consistent legacy numbering
scheme; 6 use non-padded filenames (`French_Chef_S#E#.mp4`) from a separate, later ingestion,
of which 2 collided with existing padded episodes: `episode_id 16703` ("Beef in Red Wine",
was S4E8) and `episode_id 16706` ("To Stuff a Sausage", was S7E4).

Looked up both on PBS.org's current streaming catalog to try to resolve the true numbers:
PBS lists "Beef In Red Wine" as **Season 4, Episode 20** and "To Stuff a Sausage" as
**Season 8, Episode 20**. **Do not use these PBS.org numbers to renumber this show** — cross-
checking found PBS.org's own catalog numbering does not match our DB's existing (padded-batch)
scheme at all: PBS.org lists "French Onion Soup" as **S1E2**, but our DB has that same episode
(`episode_id 15974`, already unambiguous, no collision) as **S1E1**. This means PBS.org's current
streaming catalog uses a different episode-numbering convention than whatever source the original
30 padded episodes were ingested from (likely an original-broadcast-order guide vs. PBS's
remastered-release catalog) — importing PBS.org's numbers would silently introduce a second,
incompatible numbering scheme rather than fix anything.

Per user decision, resolved the collision only, without claiming the result is the "true" number:
moved both to a collision-free `.5` decimal adjacent to their prior slot (`16703` → `8.5` in
season 4, `16706` → `4.5` in season 7) — a value that can't be a real production/PBS episode
number, signalling to a future reader that it's a placeholder, not a verified number. **This
show's `episode_number` should not be treated as reliably meaningful until a proper renumbering
pass is done against whatever source the original 30 padded episodes came from.**

**2026-09-21 data-quality fix — Dudley Do-Right (51), Pink Panther (73), Wacky Races (91)
episode_number renumbered per real segment.** These are anthology "package" shows: one half-hour
broadcast slot bundles 2-4 separate short cartoon segments, each ripped as its own file, but
ingestion gave every segment in a package the same `episode_number` (e.g. 3 different Dudley
Do-Right segment files all filed as season 1 episode 1). No reliable external episode guide
exists for any of these three shows to draw "true" numbers from, so — per user direction — this
was resolved purely from in-DB evidence: `episode_title` on every row in a package already holds
the combined title of all its segments in broadcast order (e.g.
`"The Disloyal Canadians/Finding Gold/Stokey the Bear"`); each row's `episode_file` was matched
to its segment in that list (fuzzy string match, one-to-one per package so two files can't claim
the same segment), then every show's `episode_number` was renumbered to a compact per-season
sequence (1..N, no gaps) in original-package order, segments within a package ordered by their
matched position. One-off script: `scratch_renumber_cartoon_shows.py` (dry-run by default,
`--apply` to commit; writes `renumber_cartoon_shows_report.json` showing every old→new mapping
for audit). Applied 2026-09-21: 186 rows renumbered (51: 39, 73: 114, 91: 33), zero collisions
remain in any show DB-wide.

One case needed a human call: Pink Panther old-S2E2 (`"Lucky Pink/Shooting Of Caribou Lou/Pink
In The Clink"`) had a file (`episode_id 2365`, `PinkIn.mp4`) that didn't clearly match any
segment name once the other two files claimed their segments — durations weren't conclusive
(388s vs. 372s for the plausible duplicate candidate). Per user decision, treated as the missing
"Shooting Of Caribou Lou" segment by elimination, not a duplicate.

Pink Panther season 2 also contains later (1971-dated), unrelated single-segment shorts (e.g.
"A Fly in the Pink," "Pink Blue Plate") that happened to collide onto the same old episode_number
as real 1969 three-segment packages. The title-matching approach handled these correctly without
special-casing: a single-segment `episode_title` with no `/` always resolves to its own package
of size 1, so these were never merged into the neighboring package — they just landed immediately
after it in the new sequence.

A separate, unrelated batch of 69 true exact-duplicate `episodes` rows (same `episode_file`
re-ingested) was already found and safely collapsed via `dedup_exact_episodes.py` earlier this
session.

## Per-show log

| show_id | show_name | episodes | done | method |
|---|---|---|---|---|
| 1 | Lucy Show | 135 | yes | AUTO |
| 2 | Lone Ranger | 16 | yes | AUTO |
| 5 | I Love Lucy | 30 | yes | AUTO |
| 7 | Mister Ed | 44 | yes | AUTO |
| 8 | Monkees | 40 | yes | AUTO |
| 9 | Adam 12 | 55 | yes | AUTO |
| 10 | That Girl | 29 | yes | AUTO |
| 11 | Green Acres | 30 | yes | AUTO |
| 12 | Gilligan's Island | 99 | yes | AUTO |
| 13 | Medical Center | 25 | yes | AUTO |
| 14 | Thunderbirds | 20 | yes | AUTO |
| 16 | My Mother the Car | 20 | yes | AUTO |
| 17 | H.R. Pufnstuf | 15 | yes | AUTO |
| 18 | Munsters | 27 | yes | AUTO |
| 19 | Addams Family | 26 | yes | AUTO |
| 20 | Voyage to the Bottom of the Sea | 26 | yes | AUTO |
| 21 | Avengers | 18 | yes | AUTO |
| 22 | Maverick | 25 | yes | AUTO |
| 23 | Dick Van Dyke Show | 158 | yes | AUTO |
| 24 | Leave it to Beaver | 136 | yes | AUTO |
| 25 | Perils of Penelope Pitstop | 15 | yes | AUTO |
| 27 | Sea Hunt | 36 | yes | AUTO |
| 28 | Have Gun Will Travel | 15 | yes | AUTO |
| 29 | 12 O'Clock High | 19 | yes | AUTO |
| 30 | Alfred Hitchcock Presents | 36 | yes | AUTO |
| 31 | Donna Reed Show | 34 | yes | AUTO |
| 32 | Perry Mason | 13 | yes | AUTO |
| 34 | Quincy M.E. | 12 | yes | AUTO |
| 35 | Emergency | 55 | yes | AUTO |
| 36 | Atom Ant | 26 | yes | AUTO |
| 37 | Many Loves of Dobie Gillis | 39 | yes | AUTO |
| 38 | Lost in Space | 83 | yes | AUTO |
| 39 | Vega$ | 23 | yes | AUTO |
| 40 | Ironside | 26 | yes | AUTO |
| 41 | Popeye the Sailor | 31 | yes | AUTO |
| 42 | Felix the Cat | 11 | yes | AUTO |
| 43 | Secret Squirrel | 26 | yes | AUTO |
| 44 | Ant and the Aardvark | 17 | yes | AUTO |
| 45 | Wanted Dead or Alive | 25 | yes | AUTO |
| 46 | Squiddly Diddly | 26 | yes | AUTO |
| 47 | Jetsons | 75 | yes | AUTO |
| 48 | Wally Gator | 44 | yes | AUTO |
| 49 | Tijuana Toads | 17 | yes | AUTO |
| 50 | Roger Ramjet | 26 | yes | AUTO |
| 51 | Dudley Do-Right Show | 39 | yes | AUTO |
| 52 | T. J. Hooker | 22 | yes | AUTO |
| 54 | Hardy Boys/Nancy Drew Mysteries | 36 | yes | AUTO |
| 55 | Mission Impossible | 25 | yes | AUTO |
| 57 | Adventures of Superman | 10 | yes | AUTO |
| 58 | Honey West | 20 | yes | AUTO |
| 60 | Flo | 29 | yes | AUTO |
| 61 | Star Trek (TOS) | 80 | yes | AUTO |
| 62 | Lippy the Lion | 50 | yes | AUTO |
| 63 | Max Headroom | 14 | yes | AUTO |
| 64 | Petticoat Junction | 21 | yes | AUTO |
| 66 | Saturday Night Live | 111 | yes | AUTO |
| 67 | Ricochet Rabbit | 15 | yes | AUTO |
| 68 | Yancy Derringer | 33 | yes | AUTO |
| 69 | Man From U.N.C.L.E. | 59 | yes | AUTO |
| 70 | The Waltons | 23 | yes | AUTO |
| 71 | New Adventures of Superman | 66 | yes | AUTO |
| 72 | The Veil | 11 | yes | AUTO |
| 73 | Pink Panther Show | 114 | yes | AUTO |
| 75 | Too Close for Comfort | 84 | yes | AUTO |
| 76 | Topper | 10 | yes | AUTO |
| 78 | Brady Bunch Variety Hour | — | no | AUTO |
| 79 | Wheelie and the Chopper Bunch | 13 | yes | AUTO |
| 83 | Banana Splits | 18 | yes | AUTO |
| 84 | Police Squad | 6 | no (was stale `yes` from old detection_risk_1_125 pass; 5/6 episodes re-reviewed 2026-09-23, 2729 blocked on pipeline re-run) | AUTO |
| 85 | Space 1999 | 48 | yes | AUTO |
| 86 | Brady Kids | 22 | yes | AUTO |
| 87 | Bullwinkle Show | 50 | yes | AUTO |
| 88 | Fonz and the Happy Days Gang | 13 | yes | AUTO |
| 89 | Adventures of Kit Carson | 29 | yes | AUTO |
| 90 | Underdog | 56 | yes | AUTO |
| 91 | Wacky Races | 33 | yes | AUTO |
| 92 | Bill Nye the Science Guy | 90 | yes | AUTO |
| 93 | Fugitive | 110 | yes | AUTO |
| 94 | Ultraman | 13 | yes | AUTO |
| 95 | Mutual of Omaha's Wild Kingdom | 89 | yes | AUTO |
| 96 | Ghost and Mrs. Muir | 49 | yes | AUTO |
| 97 | My Favorite Martian | 106 | yes | AUTO |
| 98 | Ellery Queen | 11 | yes | AUTO |
| 99 | Starsky & Hutch | 69 | yes | AUTO |
| 100 | Hawaii Five-O | 96 | yes | AUTO |
| 101 | Columbo | 43 | yes | AUTO |
| 104 | Kojak | 62 | yes | AUTO |
| 105 | Barnaby Jones | 61 | yes | AUTO |
| 106 | Hunter | 63 | yes | AUTO |
| 107 | Police Woman | 67 | yes | AUTO |
| 108 | Charlie's Angels | 66 | yes | AUTO |
| 111 | Barney Miller | 57 | yes | AUTO |
| 113 | Cannon | 66 | yes | AUTO |
| 114 | Harry O | 42 | yes | AUTO |
| 115 | Hart to Hart | 42 | yes | AUTO |
| 116 | ThunderCats | 74 | yes | AUTO |
| 117 | The Incredible Hulk | 23 | yes | AUTO |
| 118 | Jake and the Fatman | 53 | yes | AUTO |
| 119 | Banacek | 16 | yes | AUTO |
| 120 | Dragnet 1967 | 60 | yes | AUTO |
| 121 | Street Hawk | 13 | yes | AUTO |
| 122 | Hogan's Heroes | 168 | yes | AUTO |
| 124 | CHiPs | 10 | yes | AUTO |
| 125 | Hill Street Blues | 56 | yes | AUTO |
| 126 | The Rockford Files | 121 | yes | AUTO |
| 127 | Twilight Zone | 156 | yes | AUTO |
| 128 | Mod Squad | 76 | yes | AUTO |
| 130 | McMillan & Wife | 39 | yes | AUTO |
| 131 | Angie | 36 | yes | AUTO |
| 132 | Kolchak: The Night Stalker | 19 | yes | AUTO |
| 133 | Maude | 24 | yes | AUTO |
| 134 | Mr. Terrific | 17 | yes | AUTO |
| 135 | Wait Till Your Father Gets Home | 18 | yes | AUTO |
| 136 | Rookies | 23 | yes | AUTO |
| 137 | Flintstones | 26 | yes | AUTO |
| 138 | Jackson 5ive | 23 | yes | AUTO |
| 139 | Three's Company | 56 | yes | AUTO |
| 140 | Laverne and Shirley | 62 | yes | AUTO |
| 141 | Happy Days | 62 | yes | AUTO |
| 142 | Different Strokes | 64 | yes | AUTO |
| 143 | I Dream of Jeannie | 85 | yes | AUTO |
| 144 | Gunsmoke | 115 | yes | AUTO |
| 145 | Partridge Family | 96 | yes | AUTO |
| 146 | Sixth Sense | 13 | yes | AUTO |
| 147 | Scooby Doo, Where Are You! | 41 | yes | AUTO |
| 148 | Moonlighting | 24 | yes | AUTO |
| 150 | School House Rock | 38 | yes | AUTO |
| 151 | Josie and the Pussycats in Outer Space | 16 | yes | AUTO |
| 152 | New Scooby-Doo Movies | 22 | yes | AUTO |
| 153 | Batman | 119 | yes | AUTO |
| 154 | Family Affair | 88 | yes | AUTO |
| 155 | Land of the Lost | 42 | yes | AUTO |
| 156 | Streets of San Francisco | 24 | yes | AUTO |
| 157 | Love Boat | 73 | yes | AUTO |
| 159 | Alice | 72 | yes | AUTO |
| 160 | F.B.I. | 88 | yes | AUTO |
| 211 | Thriller | 55 | no | AUTO |
| 231 | Quark | 7 | yes | AUTO |
| 224 | Tabitha | 13 | yes | AUTO |
| 233 | Ace Crawford... Private Eye | 5 | yes | AUTO |
| 242 | Cosmos | 13 | yes | AUTO |
| 245 | CPO Sharkey | 15 | yes | AUTO |
| 247 | Beyond Westworld | 5 | yes | AUTO |
| 250 | Life with Lucy | 12 | yes | AUTO |
| 299 | Tate | 13 | yes | AUTO |
| 304 | Ed Sullivan Show | 4 | yes | AUTO |
| 316 | Darkroom | 6 | yes | AUTO |
| 318 | Sonny and Cher Show | 3 | yes | AUTO |
| 334 | Amazing Spider-Man | 12 | yes | AUTO |
| 335 | Turn On | 2 | yes | AUTO |
| 338 | French Chef | 36 | yes | AUTO |
| 340 | Match Game | 2 | no | AUTO |
| 344 | Sigmund and the Sea Monsters | 12 | yes | AUTO |
| 350 | The Osmonds | 5 | yes | AUTO |
| 357 | Butch Cassidy | 13 | yes | AUTO |
| 363 | Top of the Pops | 19 | yes | AUTO |
| 353 | Bill Cosby Show | 13 | yes | AUTO |
| 373 | Star Cops | 9 | yes (legitimately zero breaks -- BBC/UK production, no commercial breaks; reviewed 37 of 46 candidates 2026-09-23, all false positives from dark/space-scene lighting, none kept -- stopped review, same "commercial-free" pattern as the PBS/short-cartoon shows above) | AUTO |
| 355 | Electra Woman and Dyna Girl | 8 | yes | AUTO |
| 403 | Bold Ones: The Senator | 8 | yes | AUTO |
| 395 | Fernwood Tonight | 10 | yes | AUTO |
| 177 | Shaft | 7 | yes (batch pipeline output from 2026-09-17 reviewed frame-by-frame 2026-09-23: 160 raw candidates across all 7 episodes, 18 confirmed real black-cut splice points inserted into commercial_breaks_v2, 142 rejected -- extremely dark 1970s cinematography meant nearly every level-2/3 candidate was a misdetected dim scene, while level-0/1 candidates were almost all genuine; ~73min runtime per episode indicates this print was already re-edited for broadcast with most ad-break duration removed, leaving only splice-point evidence, similar to other cleanly-re-edited prints in this audit) | AUTO |
| 356 | Fangface | 16 | yes (batch pipeline output reviewed frame-by-frame 2026-09-23: 57 raw candidates across all 16 episodes, 51 confirmed and inserted into commercial_breaks_v2, 6 rejected. Structural pattern: nearly every episode has a genuine break near ~60s, ~600-810s, and ~1234s (3-act structure in a ~21min cartoon slot); "extra" candidates at odd positions, mostly level 2/3, were real content (starfield/night scenes, a staircase interior, a blue-sky flying-object shot) and rejected. NEW finding: episode 16573 (A Toothy Shark Is No Lark) had a literal "PLACE COMMERCIAL HERE" placeholder title card at one break, with two more solid navy/blue frames at other break points in the same episode that are almost certainly the same card sampled off the text (fade-in/out) -- all three kept as genuine ad-break markers per user confirmation, even though they are not black frames. This is the first non-black break kept in this rebuild; watch for the same card on other Sid & Marty Krofft/Filmation-era Saturday-morning prints, since a placeholder card is even stronger direct evidence of a real break than a black frame.) | AUTO |
| 351 | Ark II | 15 | yes (batch pipeline output reviewed frame-by-frame 2026-09-23: 62 raw candidates across all 15 episodes, 60 confirmed and inserted into commercial_breaks_v2, 2 rejected. Exceptionally clean print -- consistent 4-per-episode structure at ~60s, ~140-235s, ~560-770s, and ~1325s, virtually all genuinely solid black with almost no false-positive rate (lowest of the whole rebuild so far). Only 1 episode (16466, The Lottery) had 2 extra real-content candidates: a "140" on-screen countdown/timer graphic and a close-up dirt/ground texture shot, both rejected.) | AUTO |
| 216 | Lidsville | 17 | yes (batch pipeline output reviewed frame-by-frame 2026-09-23: 67 raw candidates across all 17 episodes, all 67 confirmed and inserted into commercial_breaks_v2, 0 rejected -- perfect coverage, cleanest show in the rebuild so far. Print has a distinctly elevated black floor for this whole show (pixel mean ~35-37/255 rather than near-0, consistent with a raised-black-level transfer per the Topper/Brady Kids pattern) but every single candidate was genuinely uniform with no discernible content at any threshold level, including the loosened level-2/3 candidates that are usually the false-positive source in other shows.) | AUTO |
| 185 | Joanie Loves Chachi | 17 | yes (batch pipeline output reviewed frame-by-frame 2026-09-23: 68 raw candidates across all 17 episodes, 62 confirmed and inserted into commercial_breaks_v2, 6 rejected. Very clean sitcom print, near-identical mean/max pixel values (~0.003/2) across essentially every level-0 candidate. All 6 rejections were real content flagged by the pixel-triage outlier check and confirmed visually: episode 7237 (Christmas Show) had 4 consecutive level-3 candidates that were all one continuous car-interior scene (misdetected dim night driving, same recurring genre pattern as prior night-scene shows), plus single level-3 outliers in 7245 (Best Foot Forward, a lit-lamp/night scene) and 7250 (I Do, I Don't, I Do, an interior "Rome" set scene).) | AUTO |
| 286 | Square Pegs | 20 | yes (batch pipeline output reviewed frame-by-frame 2026-09-23: 75 raw candidates across all 20 episodes, 69 confirmed and inserted into commercial_breaks_v2, 6 rejected. Another very clean sitcom print, near-uniform 2.0s-duration level-0 breaks. All 6 rejections were real content caught by the pixel-triage outlier check: episode 14327 (Halloween XII) had a Halloween-mask/eyeglasses POV shot; 14334 (It's Academical) had a "TO BE CONTINUED" cliffhanger title card at its end (real episode content, not an ad marker -- distinct from the Fangface "PLACE COMMERCIAL HERE" card, which explicitly indicated an ad splice); 14337 (Pilot) had a night-exterior walking scene; 14339 (The Arrangement) had 3 consecutive candidates that were all one continuous dim theater/auditorium scene. One episode (14333, Child's Christmas in Weemawee Part 1) is a double-length 48.5min episode with 5 breaks, all genuinely black.) | AUTO |
| 195 | Lotsa Luck! | 21 | yes (batch pipeline output reviewed frame-by-frame 2026-09-23: 84 raw candidates across all 21 episodes, all 84 confirmed and inserted into commercial_breaks_v2, 0 rejected -- perfect coverage, second show in a row with zero false positives (after Lidsville). Print is exceptionally clean, every candidate's pixel stats near-identical (max never exceeded 15/255 across all 84 frames) and visually confirmed genuinely black. Consistent 4-per-episode structure at ~55-90s, mid-episode, ~1380-1460s, and near end-point.) | AUTO |
| 339 | Firing Line | 31 | yes (2026-09-23: PBS production, no commercial breaks by user confirmation -- same "commercial-free" category as Star Cops/French Chef/School House Rock. 0 rows in commercial_breaks_v2, no batch_runs candidates reviewed or inserted.) | AUTO |
| 303 | Gentle Ben | 16 | yes (batch pipeline output reviewed frame-by-frame 2026-09-23: 86 raw candidates across all 16 episodes, 79 confirmed and inserted into commercial_breaks_v2, 7 rejected. NEW failure-mode instance for this animal-adventure show: episode 14866 (Wayward Bear Part 1) had a 4-candidate consecutive chain (idx4-7) that was all one continuous night-forest scene with a raccoon; episode 14868 had a single dim green-gradient forest shot; episode 14878 had 2 candidates showing a dim shed/shack ("HOUSE" sign) and two people walking with a bucket. Baseline elevated-black-level print (pixel means ~1-8/255 per episode, consistent with the Lidsville-style raised black floor) made outlier detection via pixel triage especially effective here.) | AUTO |
| 391 | My World and Welcome to It | 19 | yes (batch pipeline output reviewed frame-by-frame 2026-09-23/24: 89 raw candidates across all 19 episodes, 76 confirmed and inserted into commercial_breaks_v2, 13 rejected. Grainier/noisier print than most shows in this rebuild (individual dust-speck pixels inflated max values even on genuinely black frames), so pixel-triage outliers were confirmed by direct visual inspection rather than trusted on stats alone. Rejections spanned a wider variety of content types than usual for this whimsical-fantasy-sequence sitcom: a bowling-ball-on-stairs shot and a solid blue card (19988); a 5-candidate consecutive chain of a night-forest policeman scene (19993); a man reading a notice board plus a B&W archival "CHECK ROOM" cutaway sequence (19996); a night-woods flashlight scene (20000); a moon shot and a man walking at night in sunglasses (20002).) | AUTO |
| 266 | Manimal | 8 | yes (batch pipeline output reviewed frame-by-frame 2026-09-25: 128 raw candidates across all 8 episodes, 45 confirmed and inserted into commercial_breaks_v2, 83 rejected. See the 2026-09-25 finding above -- the 39-candidate pilot/TV-movie episode (13091) was a severe VLM-accuracy outlier, only 2 of 39 candidates genuine (both near the true end), with the model repeatedly calling clearly-visible content "solid black" at high confidence, including two candidates where the model's own stated reasoning described real content while still verdicting "keep". The 7 regular half-hour episodes behaved normally. A recurring title-montage clip (a clawed hand reaching into frame, ~90-133s in most episodes) was correctly identified as real content and rejected consistently across episodes, never assumed identical without checking each occurrence.) | AUTO |
| 336 | Roots | 8 | yes (199 candidates reviewed, 69 confirmed, 130 rejected) | MANUAL |
| 257 | Blue Thunder | 9 | yes (113 candidates reviewed, 53 confirmed, 60 rejected) | MANUAL |
| 324 | Circle of Fear | 9 | yes (162 candidates reviewed, 45 confirmed, 117 rejected) | MANUAL |
| 268 | Fantastic Journey | 10 | yes (126 candidates reviewed, 43 confirmed, 83 rejected) | MANUAL |
| 362 | Galactica 1980 | 10 | yes (184 candidates reviewed, 18 confirmed, 166 rejected) | MANUAL |
| 256 | Cop Rock | 11 | yes (199 candidates reviewed, 63 confirmed, 136 rejected) | MANUAL |
| 325 | Ghost Story | 11 | yes (180 candidates reviewed, 79 confirmed, 101 rejected) | MANUAL |
| 234 | Baretta | 12 | yes (206 candidates reviewed, 38 confirmed, 168 rejected) | MANUAL |
| 241 | Cousteau Odyssey | 12 | yes (141 candidates reviewed, 48 confirmed, 93 rejected) | MANUAL |
| 254 | Gemini Man | 12 | yes (130 candidates reviewed, 58 confirmed, 72 rejected) | MANUAL |
| 269 | Invisible Man | 12 | yes (146 candidates reviewed, 52 confirmed, 94 rejected) | MANUAL |
| 296 | The Westerner | 12 | yes (130 candidates reviewed, 51 confirmed, 79 rejected) | MANUAL |
| 330 | Lucan | 12 | yes (136 candidates reviewed, 67 confirmed, 69 rejected) | MANUAL |
| 337 | Centennial | 12 | yes (319 candidates reviewed, 84 confirmed, 235 rejected) | MANUAL |
| 199 | Barbary Coast | 13 | yes (242 candidates reviewed, 84 confirmed, 158 rejected) | MANUAL |
| 239 | Invisible Man | 13 | yes (162 candidates reviewed, 42 confirmed, 120 rejected) | MANUAL |
| 260 | Automan | 13 | yes (239 candidates reviewed, 91 confirmed, 148 rejected) | MANUAL |
| 214 | In Search of... | 57 | yes (471 candidates reviewed, 151 confirmed, 320 rejected) | MANUAL |
| 360 | Coronet Blue | 13 | yes (131 candidates reviewed, 65 confirmed, 66 rejected) | MANUAL |
| 203 | Mothers in Law | 56 | yes (272 candidates reviewed, 178 confirmed, 94 rejected) | MANUAL |
| 372 | Hammer House of Horror | 13 | yes (156 candidates reviewed, 26 confirmed, 130 rejected) | MANUAL |
