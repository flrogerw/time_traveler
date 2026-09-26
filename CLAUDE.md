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

# Run the test suite (scheduler core only -- see tests/ below)
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
  candidate's midpoint, extra span samples for breaks >5s, and gap samples for close pairs <5s
  apart (all needed for visual verification per the charter's rules).
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

`show_reprocess_queue.json` (repo root) is a DB-derived snapshot classifying every show into
`untouched` (never processed), `detection_risk_1_125` / `detection_risk_126_152` (processed
under an older, since-superseded detection policy — see `COMMERCIAL_BREAK_AUDIT.md` for why
this under-detects), `done_153_plus` (no known issue), and two explicit skip lists. Regenerate
it if the classification might have changed (e.g. after inserting new rows for a show) rather
than trusting a stale copy.

Every candidate must be individually frame-checked before insertion — no statistical sampling,
regardless of file size, model confidence, or apparent baseline, and never delegated to a
cheaper/faster model as a cost-cutting shortcut (tested and rejected — see
`COMMERCIAL_BREAK_AUDIT.md`'s 2026-09-18 addendum). That file's per-show log (single line per
show: id, name, episode count, done y/n) is the source of truth for progress; `batch_summary.json`
under the batch output root shows which shows have cleared the automated pipeline and are ready
to finalize (visually reviewed + inserted) next.

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
