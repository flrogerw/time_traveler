# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Time Traveler simulates a retro (1950s-1980s) broadcast TV network: ingests real classic TV episodes,
movies, specials, and commercials into PostgreSQL, generates period-accurate daily schedules per
simulated channel, produces `.m3u` playlists with era-matched commercial breaks, and renders a
printable, duplex-imposed PDF TV Guide booklet. See `README.md` for full details (architecture
diagrams, DB schema, env vars, troubleshooting) — it is kept up to date and should be your first
reference for anything not covered here. `DVD_RIPPING.md` documents the separate DVD-to-`.mp4`
ingestion workflow (`dvdbackup` + `HandBrakeCLI`, plus MakeMKV as a fallback for damaged/oddly
authored discs) used to source new episode files before they enter this pipeline.

## Commands

```bash
# Setup
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install                          # for movies_parser.py scraping
python -c "import nltk; nltk.download('punkt_tab')"   # for utils/printable_tv_guide.py

# Generate an automatic schedule + playlist for one channel
python -m playlists.create_playlist --hostname TV-ABC-7 --year 1971 \
  --sim-date 2026-10-22 --start 19:00:00 --duration 05:00:00

# Experiment with scheduling WITHOUT touching the DB or the production .m3u (rolls back, temp output)
python dryrun_create_playlist.py --hostname TV-SYN-5 --year 1976 --sim-date 2026-10-24 ...   # same args as create_playlist
python dryrun_fill_ladder.py [--penalty N --adjacent --seeds N]   # read-only pool-exhaustion sim

# Generate a playlist from a manually curated session
python -m playlists.create_playlist_manual --session_name christmas-2025 --year 1971

# Render a printable TV Guide PDF (must run with utils/ as the working directory)
cd utils
python printable_tv_guide.py --source current --dow 3 --year 1971 --filename tv_guide.pdf
python printable_tv_guide.py --source manual --session christmas-2025 --dow 1 --year 1971 \
  --filename tv_guide_christmas.pdf

# Celery background video-processing subsystem (separate, not wired into the main pipeline)
cd my_celery && celery -A celery_app worker -Q video_queue   # worker: run from inside my_celery/
python -m my_celery.celery_ingestion                          # ingestion loop: run from repo root

# Commercial-break audit: local browser review tool (see "Commercial Break Audit" below)
python3 review_breaks_ui.py [--port 5055]                     # then open http://127.0.0.1:5055/

# Run the test suite (scheduler core only -- see tests/ below; 60 tests)
python -m pytest tests/
```

There is no linter or CI configured in this repository. A `pytest` suite under `tests/` covers
the scheduler core (`playlists/classes/`) against the real dev DB — see below. Everything else
(ingestion, `utils/` video tooling, the commercial-break audit scripts) has no test coverage;
verify those by running the relevant script against a live database.

## Tests

`tests/` covers `playlists/classes/` (episode selection, umbrella/rotation logic) against the
**real dev database** (`.env`'s connection) — not a separate test DB or mocks, since that logic
is deeply SQL-driven and mocking it would test very little of the real behavior. Nothing a test
does is ever persisted: `tests/conftest.py`'s `db_conn` fixture patches `conn.commit()` to a
no-op for the duration of each test (several methods under test, e.g.
`Episodes.get_next_episode()` and `Shows.get_replacement_show()`, call `commit()` internally as
part of normal operation — a plain "rollback at teardown" fixture would not catch those), then
restores the real `commit` and rolls the transaction back at teardown. Tests that write use
`TEST_REPLICATION_YEAR = -1` (from `conftest.py`) so they're trivially distinguishable from real
schedule data even if isolation were ever broken.

Data-integrity tests (e.g. `test_no_episode_number_collisions`) assert invariants the scheduler
depends on but nothing in the schema enforces — run these after any ingestion or manual DB
migration, not just after code changes. `episode_number` collisions come in two distinct flavors
that need different fixes: true exact duplicates (identical `episode_file`, from re-ingestion —
safe to auto-collapse with `dedup_exact_episodes.py <show_id>`, dry-run by default, `--apply` to
commit) versus genuinely different episodes/segments colliding on the same number (anthology/
package shows with multiple segments per package, mis-filed episodes, conflicting numbering
conventions between ingestion passes) — those need per-show manual investigation, never a
blanket script. Don't assume a numbering collision is a duplicate without checking whether the
colliding rows' `episode_file`s actually match.

## Architecture

Two independent scheduling pipelines share one PostgreSQL schema and a common set of DB-backed
data-access classes in `playlists/classes/`, but populate different template tables:

- **Automatic** (`playlists/create_playlist.py`): `Shows`/`Episodes`/`Movies`/`Specials` classes pick
  content and write to `schedule_template` + `broadcast_log`.
- **Manual/curated** (`playlists/create_playlist_manual.py`): writes to
  `schedule_templates_manual` + `schedule_sessions` for hand-built schedules (e.g. holidays).
- Both feed `Playlists.get_playlist` (`.m3u` output) and `utils/printable_tv_guide.py` (PDF output).

**Key design point:** episode selection is deliberately DB-driven and stateful —
`Episodes.get_next_episode()` always queries `schedule_template` joined against `broadcast_log` to
find the lowest unaired `(season, episode)` for a show on a given channel, rather than tracking state
in application memory. This lets the pipeline be re-run for any date without losing continuity.
Sequential non-repeating playback, multi-part episode continuity (via `schedule_overrides`), and
holiday programming (blended into normal slots, not replacing a whole day) all depend on this
DB-first model — don't reintroduce in-memory state tracking.

**Umbrella/rotation shows:** some `schedule_template` slots hold an anthology "umbrella" show_id
(e.g. `NBC Mystery Movie`) rather than one ordinary series. `sub_shows` maps that umbrella's
`show_id` to the real component series that rotated through it (e.g. Columbo/McCloud/Banacek/
McMillan & Wife). `Episodes.get_next_episode()` resolves this via a `LATERAL` join (falls back to
the show's own id when it isn't in `sub_shows`, so ordinary shows are unaffected) and orders
candidates by each episode's real `episode_airdate` — reconstructing the actual historical rotation
from ingested broadcast dates instead of a hand-maintained synthetic rotation map. It records the
real component's `show_id` in `broadcast_log`, not the umbrella's. `Shows.get_replacement_show()`
mirrors this: it walks back from the component `show_id` to the umbrella's `schedule_template` row
and only rotates the slot out once every component series is exhausted. See `sub_shows` in the
README schema table.

Other major pieces:
- **Ingestion** (`imdb_scrapper_season_loop.py`, `movies_parser.py`, `specials_parse.py`): scrape
  metadata into PostgreSQL. These three hardcode DB credentials directly in source (no dotenv) —
  a known issue, be careful not to leak them further and prefer fixing rather than propagating.
- **`utils/`**: standalone ffmpeg/OpenCV-based video/audio tooling (commercial-break detection,
  B&W detection, letterbox removal, loudness normalization) — invoked per-file, no concurrency.
- **`my_celery/`**: a separate Celery subsystem for background video processing (Redis-backed,
  broker hardcoded to a LAN IP in `celery_app.py`). Not invoked by any other part of the codebase —
  treat as standalone. Import styles differ by entry point: `celery_tasks.py` uses unqualified
  imports (run from inside `my_celery/`), `celery_ingestion.py` uses package-qualified imports
  (run via `python -m my_celery.celery_ingestion` from repo root).
- **`advertising/`**: static print assets (images/logos/fonts) consumed by `printable_tv_guide.py`;
  paths there are relative to `utils/`.

Scripts under `playlists/` use absolute package imports (`from playlists.classes.X import X`) — run
them as modules (`python -m ...`) from the repository root, not as direct file paths.

**Syndicated scheduling (era-aware, weekday + weekend):** for `syndicated` channels,
`create_playlist.py` resolves each slot via `playlists/classes/SyndicatedScheduler.py`, falling back to the
network path's genre-bias engine (`Shows.get_genre_bias_for_slot`/`get_available_show_id`). Resolution order:
`syndicated_fixed_slots` -> weighted category from `fill_rules` (era-scoped via `year_start`/`year_end`; NULL/NULL =
all-era fallback; a category with no eligible show is dropped and re-picked) -> eligible show from
`show_syndication_eligibility` (`mapped_genre`, `eligible_year_start/end`, per-daypart booleans; one `mapped_genre`
per show for weekday and weekend) AND `shows.show_is_syndicated` -> filler ladder -> genre-bias fallback.
- Dayparts: weekday 7 blocks 06-19 (`weekday_*`), Sat/Sun 5 blocks 07-19 (`saturday_*`/`sunday_*`).
  `setup_syndicated_scheduling.py` / `setup_weekend_scheduling.py` are the re-runnable schema/data imports; re-running the
  first deletes every era-aware `fill_rules` row.
- A weekday `SYN` slot is one Mon-Fri strip (`Schedules.insert_schedule_template` copies it to all five days); a show is
  licensed to one slot per replication year.
- **Filler** (`resolve_filler`): when a slot's categories have no unused eligible show -> adjacent daypart, related
  category, rerun on same channel, rerun from another channel, back-to-back as last resort; `use_filler=False` restores
  old behavior. `dryrun_fill_ladder.py` mirrors it in memory -- keep the two in sync.
- **Duration-aware grid** (`--duration-aware`, opt-in; writes `time_slot_schedules`): slot length = smallest
  `show_duration` option >=30 min that holds the show's typical episode (`EPISODE_FIT_TOLERANCE` 1.05). Episode pick
  prefers one that fits the slot; `get_true_duration` reads `episodes` (+ `commercial_breaks`), not `episode_durations`.
- **Shorts packing** (`Episodes._pack_short_episodes`): episodes <60% of slot are packed (content <=80% of slot, max 12);
  `Playlists._build_pack_pods` gives one ad budget with pods between shorts and cuts the shorts' original ad breaks.
- **Interstitials** (School House Rock): `Episodes.get_next_interstitial`, only on channels where
  `channel_airs_interstitials` (ABC network/syndicated), play between programs with no ads, their time comes out of the
  preceding program's ad budget. Inert unless the show's `mapped_genre='interstitial'` (set for show 150, 1973-85,
  Sat early-morning/morning).
- **Ad audience** (`Commercials.get_commercials(audience=...)`): before noon, children's programming and Saturday-morning
  slots only get `CHILDREN_ALLOWED_TYPES` (+ hygiene from `CHILDREN_HYGIENE_SPONSORS`); afternoon/primetime unrestricted.
- `patch_guide_calibration.py` is inactive: its default steps (early-syndication years, SHR row) are already reflected or
  were reverted; guide-derived flags/weights (`--guide-rules`) were rejected. Do not run it with `--apply`.

**Mid-season schedule accuracy:** `schedules` (historical network schedule seeds) matches on real
`effective_start_date`/`effective_end_date` ranges, not just the legacy `air_date` year int —
`migrate_schedule_date_ranges.py` added these columns (defaulting to Jan 1-Dec 31 of `air_date`
for every existing row) and narrowed specific rows to real researched mid-season switch dates.
`Shows.get_scheduled_shows_id(sim_date, dow)` takes an actual date now, not a bare year.
`Schedules.sync_schedule_template_show_id()` lets a mid-season switch actually take effect on a
*later* run for the same (channel, dow, replication_year) — without it, `schedule_template`'s
"first run wins" caching would lock a slot to whichever show first templated it, permanently.
Network-only by design; never call it from the syndicated branch — its `schedule_data` is a
weighted random pick meant to stay stable across runs (sequential-episode continuity), not a
deterministic date-keyed answer, and force-resyncing it would re-roll a slot's show every run.
Split-season shows: `Shows.get_scheduled_shows_id` orders widest range first (the narrowest researched range wins an overlap,
because `process_scheduled_shows` keeps the last row per time). `tv_guide_research/build_season_range_sql.py` turns
`proposed_schedules_season_ranges.csv` into `season_ranges_applicable.csv` (560 rows, APPLIED 2026-10-18) + `apply_season_ranges.sql` /
`rollback_season_ranges.sql` and `season_ranges_held_back.csv` (33 rows: bad airdate data or an already-researched range).
Redundant `schedules` row 4454 was deleted (2026-10-18).
`tv_guide_research/resolve_held_back.py` (read-only) re-derives 10 of the 33 held rows (`apply_resolved_held.sql`) and proposes 262 summer
extensions to Aug 31 for shows with a later season on the same network (`apply_summer_extensions.sql`); both have rollbacks and are
NOT applied (apply resolved first). 23 rows stay held (`season_ranges_held_back_remaining.csv`).

## TV Guide research (the basis for daypart/schedule decisions)

`TV_GUIDE_RESEARCH.md` holds what 299 real TV Guide issues (1965-70 Northern California, plus 1973-88 spot
issues; 524k parsed listings) show about what aired when -- e.g. Saturday 7-12 is ~76% children's programming and
essentially never crime series or adult sitcoms. Schedule rules (daypart flags, fill weights, content limits) must be
derived from that evidence, not from genre guesses; read it before changing them. Tooling is in `tv_guide_research/`
(`parse_guide.py` rebuilds listings from the archive.org OCR word coordinates; `derive_flags.py` derives daypart
eligibility per show and diffs it against `show_syndication_eligibility`, read-only; `missing_kids_shows.py` ranks
kids' shows the guides show a lot that the library lacks). Rebuild the data with `download_guides.py` +
`run_parse_all.py` (the ~130 MB JSONL is not committed).

**Never write to the database for this work** -- deliver proposed changes as files (CSV/SQL/JSON) and dry-run only; "apply"
or "fix it" is not permission to write. `TV_GUIDE_RESEARCH.md` also records what the DB already holds
(`shows.show_is_syndicated`, the mostly-primetime `schedules` table, `commercial_type`), the per-show syndication research
(`tv_guide_research/data/syndication_research.csv`, including the user's own pasted verdicts for the 119 shows the web
research could not resolve), and the user's standing rules (School House Rock is an interstitial, ad audience rules,
guides-and-existing-structure only). The pre-session DB state is in `tv_guide_research/original_db_state/`.

## Commercial Break Audit

A separate, ongoing effort audits `commercial_breaks_v2` (detected ad-break timestamps per
episode) show by show. Full methodology, standing rules, and per-show progress log live in
`COMMERCIAL_BREAK_AUDIT.md` at the repo root — read its "CHARTER" section first before doing
any work on this; it is the authoritative rule set (detection thresholds, 100%-coverage
verification requirement, close-pair handling, reporting format, Definition of Done) and
supersedes any older/informal notes further down that file.

Permanent scripts (repo root, never scratchpad):

- `rebuild_show.py <show_id> <out.json> --workers 8` — runs blackdetect/silencedetect across
  all 4 threshold levels for every episode of a show, unions and dedupes candidates, retains
  detection provenance.
- `extract_break_frames.py <candidates.json> <out_dir> --workers 8` — extracts a frame at each
  candidate's midpoint, extra span samples for breaks >5s, gap samples for close pairs <5s apart,
  and (for every candidate) a 5-frame, 0.25s-spaced context filmstrip immediately before/after the
  break (`pre_0..pre_4`/`post_0..post_4`, clamped to the episode's start/end) — a single black
  frame's content can't distinguish a real ad-splice from an ordinary in-program scene transition,
  so reviewers need the actual motion/continuity across the cut, not just the break itself.
- `verify_breaks_vlm.py <candidates.json> <frames_dir> <out.json> --model qwen3-vl:30b` — runs
  a local vision-language model (served by Ollama; `ollama pull qwen3-vl:30b` + a running daemon
  required, ~20GB) over the extracted frames to pre-sort/triage candidates as keep/reject/
  uncertain with a confidence level. **This is triage only, not a decision** — every candidate,
  regardless of verdict or confidence, still needs an actual visual review before anything is
  inserted into `commercial_breaks_v2` (see `COMMERCIAL_BREAK_AUDIT.md`'s 2026-09-17 policy
  correction for why: a real test found wrong verdicts even in the "high-confidence, unflagged"
  bucket that no automatic flag caught). `<out.json>.needs_review.json` collects the
  lower-confidence/escalated subset as a starting point, but full coverage means reviewing
  everything, not just that file.
- `batch_process_shows.py` — unattended batch driver: runs the three scripts above end-to-end
  per show with no human/Claude intervention, safe to leave running for hours/days (never
  writes to the DB, resumable via `<out_root>/batch_summary.json`). Use `--queue-file
  show_reprocess_queue.json --categories <name ...>` to run a named subset (see below), or
  `--untouched-shows` / `--show-ids <id ...>` directly.
- `review_breaks_ui.py` — local Flask browser tool (`python3 review_breaks_ui.py`, then
  http://127.0.0.1:5055/) for doing the mandatory 100%-coverage frame review directly, without
  spending chat tokens per frame. Lists shows whose batch pipeline is done and that still have
  zero `commercial_breaks_v2` rows; shows the context filmstrip per candidate with the VLM
  verdict as a de-emphasized hint only, plus an on-demand "play clip" button (2s before break to
  2s after, ffmpeg-cut and cached under `frames/clips/` on first request — never pre-generated)
  for when the still-frame filmstrip alone isn't enough to judge a real ad-splice vs. an
  in-program scene transition. No skip/bulk-decide affordance anywhere — Finalize is only
  reachable once every candidate has an explicit keep/reject decision, and the preview screen
  flags (non-blocking) unusual patterns in the final surviving set — e.g. all confirmed breaks
  sitting in the last 10% of an episode's runtime — worth a manual second look before confirming,
  not proof of a mistake. On confirm it inserts into `commercial_breaks_v2`, appends a row (with
  a `MANUAL` tag) to this file's per-show table, and deletes `batch_runs/<show_id>/`. To discard
  in-progress review decisions for a show and start over, delete
  `batch_runs/<show_id>/human_review.json`.

`show_reprocess_queue.json` (repo root) is a DB-derived snapshot classifying every show into
`untouched` (never processed), `detection_risk_1_125` / `detection_risk_126_152` (processed
under an older, since-superseded detection policy — see `COMMERCIAL_BREAK_AUDIT.md` for why
this under-detects), `done_153_plus` (no known issue), and two explicit skip lists. Regenerate
it if the classification might have changed (e.g. after inserting new rows for a show) rather
than trusting a stale copy.

Every candidate must be individually frame-checked before insertion — no statistical sampling,
regardless of file size, model confidence, or apparent baseline, and never delegated to a
cheaper/faster model as a cost-cutting shortcut (tested and rejected — see
`COMMERCIAL_BREAK_AUDIT.md`'s 2026-09-18 addendum). That file's per-show log (id, name, episode
count, done y/n, and a `method` column — `AUTO` = reviewed by Claude in chat, `MANUAL` = reviewed
directly by the user via `review_breaks_ui.py`) is the source of truth for progress;
`batch_summary.json` under the batch output root shows which shows have cleared the automated
pipeline and are ready to finalize (visually reviewed + inserted) next.

**Don't trust a single source for "is this show done" — check `commercial_breaks_v2` directly.**
The per-show table's `yes`, `show_reprocess_queue.json`'s classification, and
`batch_summary.json`'s `done` status can all be stale or simply wrong (a show can be marked `yes`
from an older pass while the newer `batch_runs/<id>` detection output sits unreviewed; a show can
have zero rows in `commercial_breaks_v2` for a perfectly legitimate reason — PBS/BBC and other
non-commercial broadcasters, or short cartoon segments, genuinely have no breaks — rather than
being an unfinished gap). When in doubt, query `commercial_breaks_v2` for that show's episodes
directly rather than trusting any one file's bookkeeping. Legacy detections may also exist only in
the older `commercial_breaks` table (pre-dating `commercial_breaks_v2` and this charter) — those
still need frame-by-frame re-verification before counting as done, not just a copy over.

Once a show is fully finalized (reviewed, inserted, logged), delete its `batch_runs/<show_id>/`
directory — there's no ongoing value in keeping candidate/frame artifacts around after the real
decisions are recorded in the DB and the log, and they're large (tens of MB to low GB per show).
`review_breaks_ui.py` does this automatically on finalize; do the same by hand for any show
reviewed directly in chat.

## Environment Variables

Set in `.env` (loaded via `python-dotenv`), reconstructed from source — no `.env.example` exists:

| Variable | Purpose |
|---|---|
| `DATABASE` / `DB_NAME` | PostgreSQL database name — both must be set to the same value; different scripts read different ones. |
| `DB_USER` / `DB_PASSWORD` / `DB_HOST` / `DB_PORT` | PostgreSQL connection (port defaults to `5432`, hardcoded in most call sites). |
| `LOCAL_PATH` | Base path (network share) for video/commercial/image assets referenced by generated playlists. |
| `SHARPEN_RATIO` | VLC `sharpen-sigma` value written into generated `.m3u` entries. |
| `DIR_ROOT` | Base directory for `commercial_break_playlist.py`. |
| `DIR_ROOT_PI` | Base directory `my_celery/celery_ingestion.py` reads source video from (ingestion host path, e.g. a Raspberry Pi). |
