# Ripping DVDs (Mayberry RFD, and similar)

How to rip a TV-episode DVD on this Mac (Apple Silicon) into one clean MP4
per episode, named `S0#E##.mp4`.

## Why HandBrake alone doesn't work

Both the Mac App Store build of HandBrake and the direct `.dmg` download
from handbrake.fr are sandboxed (`com.apple.security.app-sandbox = true`).
Their sandbox entitlements only grant read access to `/usr/local/` — not
`/opt/homebrew/`, which is where Homebrew installs `libdvdcss` on Apple
Silicon. So even with `libdvdcss` correctly installed via Homebrew, the
GUI app can't reach it, can't decrypt the disc, and produces a pixelated/
corrupted preview and output (garbage macroblocks on the moving parts of
the frame — the classic fingerprint of CSS-encrypted MPEG-2 being decoded
as if it were plain video).

Confirming which build you have doesn't matter — this affects both.

## One-time setup

```
brew install libdvdcss
brew install dvdbackup      # rips the raw decrypted VIDEO_TS structure
brew install handbrake      # installs HandBrakeCLI, unsandboxed, links
                             # directly against Homebrew's libdvdread/
                             # libdvdnav/libdvdcss — no symlink workaround
                             # needed for the CLI (only the GUI needs one)
brew install lsdvd          # inspect a disc's title/chapter structure
```

If you also want the HandBrake GUI to work (not required for this
workflow, since HandBrakeCLI does the actual extraction), symlink
`libdvdcss` into the one path its sandbox is allowed to read:

```
sudo mkdir -p /usr/local/lib
sudo ln -sf /opt/homebrew/Cellar/libdvdcss/1.6.0/lib/libdvdcss.2.dylib /usr/local/lib/libdvdcss.2.dylib
```

## Per-disc workflow

1. **Insert the disc**, find its device:
   ```
   diskutil list | grep -A2 "external, physical"
   ```
   Note the `/dev/diskN` identifier (use `/dev/rdiskN`, the *raw* device,
   for the actual rip — `dvdbackup` needs raw device access on macOS;
   the buffered `/dev/diskN` form fails with a seek error).

2. **Rip the whole disc structure** (decrypts via `libdvdcss` as it goes —
   this is the slow step, several minutes per disc):
   ```
   mkdir -p /Volumes/TTBS/dvdrip_tmp
   dvdbackup -M -i /dev/rdiskN -o /Volumes/TTBS/dvdrip_tmp -v -p -r a
   ```
   Always use `-M` (mirror the whole disc) — `-t` (single title) does not
   produce `.IFO`/`.BUP` files, so HandBrakeCLI can't read the result at all
   ("scan: unrecognized file type"). Always include `-v -p -r a`: `-v -p`
   (verbose + progress) gives real visibility into which part is slow, which
   is the only way to tell a genuinely stalled/bad read from a normal slow
   one; `-r a` (abort on error) fails loudly and immediately the moment a
   sector is truly unreadable, instead of the default `-r m` (skip multiple
   blocks) silently producing a truncated file with no error at all — the
   latter cost an entire troubleshooting session (2026-09-22/23, Wonder
   Woman S2 disc) chasing what looked like disc damage in two small
   titlesets that were actually fine all along; the visible failure was
   HandBrakeCLI's own transient demux hiccup on one title, unrelated to the
   rip. Trade-off: `-r a` aborts the *whole* rip on any error, even for
   titlesets that already copied fine — re-running from scratch is the
   correct response, not switching back to `-t` or the default `-r m`.

   Success looks like a series of `libdvdread: Get key for ...` lines
   followed by `Found N VTS's`, then it copies. A working CSS key
   retrieval here is the real confirmation `libdvdcss` is functioning —
   if this fails, that's the actual encryption/library problem, not a
   HandBrake problem.

3. **Inspect the title structure** before extracting anything:
   ```
   lsdvd -c "$(ls -d /Volumes/TTBS/dvdrip_tmp/*)"
   ```
   For every Mayberry RFD disc seen so far, the layout has been
   consistent:
   - **Title 01**: all of that disc's episodes concatenated back-to-back
     into one long title (36 chapters on a 7-episode disc, 26 on a
     5-episode disc) — ignore this one entirely, it's redundant with the
     titles below.
   - **Titles 02 through N**: each individual episode as its own title,
     ~24:30–25:50 long, 6 chapters each. This is what you actually want.
   - **The last two titles** (e.g. Title 09/10 on a 7-episode disc): a
     ~2:11 clip and an ~12-second clip — a promo/bumper, not a real
     episode. Skipped so far; ask if these are ever wanted.

4. **Extract each real episode title** with `HandBrakeCLI` (this is the
   *Homebrew* CLI build, unsandboxed, not the GUI app — it reads the
   decrypted `VIDEO_TS` folder from step 2 directly and understands
   title/chapter structure, which raw `ffmpeg` on its own does not):
   ```
   HandBrakeCLI -i "/Volumes/TTBS/dvdrip_tmp/<Disc Folder Name>" -t <title#> \
     -o "/Volumes/ExternalDrive/RFD/S<season>/S0<season>E<episode>.mp4" \
     -e x264 -q 20 -B 160
   ```
   Loop over each disc's real episode titles (skipping Title 01 and the
   trailing short clips), incrementing the episode number to continue
   numbering across discs within the same season.

   **If HandBrakeCLI fails consistently on one specific title, at the same
   point, against a `VIDEO_TS` folder already confirmed complete** (a clean
   `dvdbackup -M ... -r a` run with no errors, and VOB part sizes identical
   across repeated rips) — this is not a bad disc/rip, it's HandBrake's
   default `libdvdnav` reader choking on that title's internal DVD
   structure. This showed up on the Wonder Woman Season 2 disc 1's combined
   73-minute two-part episode: all of that disc's real titles (the "play
   all" concat plus each individual episode) share one titleset with
   overlapping, non-contiguous PGC/cell references — a space-saving
   authoring trick that `libdvdnav` doesn't always handle cleanly, failing
   with `dvdnav: Read Error, Expected NAV packet but none found` a few
   minutes before the true end, silently muxing a truncated (but
   valid-looking, no reported error) file. Fix: add `--no-dvdnav` to force
   HandBrake's older `libdvdread`-based reader, which tolerates the
   malformed navigation packets (visible as a wall of
   `demux_dvd_ps: not a PS packet` lines) and completes correctly:
   ```
   HandBrakeCLI -i "/Volumes/TTBS/dvdrip_tmp/<Disc Folder Name>" -t <title#> --no-dvdnav \
     -o "..." -e x264 -q 20 -B 160
   ```
   Only needed for titles that actually fail this way — the normal command
   works fine for the simple, non-overlapping title structure every other
   disc in this project has had so far.

5. **Spot-check for pixelation** before trusting a batch — pull one frame
   and look at it:
   ```
   ffmpeg -y -ss 300 -i output.mp4 -frames:v 1 -update 1 -q:v 2 check.jpg
   ```
   A clean frame confirms the decrypt/extract worked; leftover pixelation
   means something upstream (disc damage, or `libdvdcss` genuinely not
   loading) needs troubleshooting before continuing the batch.

## Naming / numbering convention

- Files land in `/Volumes/ExternalDrive/RFD/S<season>/`, one folder per
  season.
- Named `S0<season>E<episode>.mp4`, zero-padded to 2 digits
  (`S01E01.mp4` ... `S01E26.mp4`, `S02E01.mp4` ...).
- Episode numbering is **continuous across every disc in a season**, not
  reset per-disc — e.g. Season 1 spanned 4 discs with 7, 7, 7, and 5
  episodes respectively, numbered `S01E01` through `S01E26` straight
  through without restarting at each new disc.

## Season 1 disc → episode mapping (for reference)

| Disc | Real episode titles | Episodes |
|------|---------------------|----------|
| D1   | Titles 02–08 (7)    | S01E01–S01E07 |
| D2   | Titles 02–08 (7)    | S01E08–S01E14 |
| D3   | Titles 02–08 (7)    | S01E15–S01E21 |
| D4   | Titles 02–06 (5)    | S01E22–S01E26 |

Season 2 follows the same process, starting at `S02E01`.

## Season 2 disc → episode mapping (for reference)

| Disc | Real episode titles | Episodes |
|------|---------------------|----------|
| D1   | Titles 02–07 (6)    | S02E01–S02E06 |
| D2   | Titles 02–08 (7)    | S02E07–S02E13 |
| D3   | Titles 02–08 (7)    | S02E14–S02E20 |
| D4   | Titles 02–07 (6)    | S02E21–S02E26 |

Season 2 complete at 26 episodes, same as Season 1. Season 3 follows the
same process (`S3/` folder), starting at `S03E01` — check each new disc's
title count with `lsdvd` first rather than assuming a constant count.

## Folder naming gotcha

The actual convention used on disk is `S1`, `S2`, `S3`... (**not** `S01`,
`S02` as this doc originally assumed) — always check for an existing
season folder before creating a new one, and match its naming.

---

# Ripping McCloud (a second show sharing this workflow)

McCloud uses the same `dvdbackup` + `libdvdcss` + `HandBrakeCLI` + `lsdvd`
workflow as Mayberry RFD above, with these differences:

- **Feature-length format**: episodes run ~70–95 minutes (McCloud aired as
  a rotating 90-minute TV movie in NBC's Mystery Movie wheel), not ~25
  minutes. Discs typically hold 1–3 episodes, not 6–7.
- **Files land in** `/Volumes/ExternalDrive/McCloud/S<season>/`, named
  `S0<season>E<episode>.mp4` — same convention as RFD.
- **Title 01 is always a short (~19s) promo/bumper** — same as RFD,
  skipped/not saved (see below).
- **Titles 02+ are the real episodes** — usually 2–3 per disc, but check
  `lsdvd` per disc; don't assume a constant count (one disc had only 1
  episode, another had 3).

## Do not assume every extra title on a disc is the same show

One McCloud disc's "extra" title turned out to be a different series from
the same NBC Mystery Movie rotation (McMillan & Wife) bundled onto the same
disc, not a bonus McCloud episode. Before batch-extracting every non-bumper
title on a disc, spot-check its content (sample a few frames a few minutes
in) if anything about it looks off — unusual chapter count/pacing compared
to sibling titles, or noticeably different runtime. Look for the lead
actor (Dennis Weaver, sheepskin coat, cowboy hat) and NYC setting to
confirm it's actually McCloud; a fedora/mustache/tweed-coat character in a
tuxedo-party or noir-detective scene is a signal to check the closing
credits before assuming — but note the closing credits themselves aren't
always reliable at a glance either, since a McCloud episode can span very
different scenes (tuxedo party, poolside, desert car chase) within one
episode. When genuinely uncertain, check the tail-end studio card and
guest-cast credits over an identifiable location shot (e.g. NYC skyline)
before concluding it's a different show.

## Universal bumper: check front AND tail of every file, flag anomalies

Standing rule for every McCloud episode: check both the very start and the
very end of the extracted file for the Universal/MCA studio bumper, and
flag anything that doesn't match the established pattern for that
season/disc.

- **Seasons 1–2 discs**: the modern "www.universalstudios.com" Universal
  logo bumper (added for the DVD release, not period-accurate to the
  1970s broadcast) is spliced onto the front of the episode content. It
  fades to black by ~22s, with actual episode content starting a bit
  after — extract with `HandBrakeCLI`, then trim with:
  ```
  ffmpeg -y -ss 22.2 -i input.mp4 -c:v libx264 -crf 18 -preset medium -c:a copy output.mp4
  ```
  (22.2s was the cut point that worked consistently — verify per-file if
  the disc's cadence differs, by sampling frames every ~1s around 20–24s
  to confirm you're cutting on solid black, not mid-fade.) No modern
  bumper has been found at the tail on any file checked so far.
- **Seasons 3–7 discs**: episode files do **not** have the modern bumper
  spliced onto the front at all — content starts within ~15–22s with no
  logo, so **no front trim is needed**. Instead, the **tail end** carries a
  vintage-era Universal/MCA syndication card (several variants seen: "FROM
  UNIVERSAL CITY, CALIFORNIA — UNIVERSAL STUDIOS — MCA-TV EXCLUSIVE
  DISTRIBUTOR", "FROM UNIVERSAL — AN MCA COMPANY", "AND UNIVERSAL — AN MCA
  COMPANY" for co-production episodes). These are all fine as-is — don't
  trim them, they're part of the original broadcast material. One episode
  (S07E06) had no studio card at all at the tail, ending on plain black
  after the legal disclaimer — flagged but not concerning, no action
  needed. The final episode of the series (S07E07) ends on what looks
  like a cast farewell/dedication card instead of the studio logo — also
  expected, not an error.
- **Per the user's instruction, the standalone bumper title (Title 01) is
  no longer extracted or saved** as a separate file — only checked as part
  of inspecting the front/tail of the real episode files.

## McCloud season → episode counts (for reference)

| Season | Episodes | Discs |
|--------|----------|-------|
| S1 | 4 (E01–E04) | D1–D2 |
| S2 | 7 (E01–E07) | D1–D4 |
| S3 | 5 (E01–E05) | D1–D3 |
| S4 | 5 (E01–E05) | D1–D3 |
| S5 | 9 (E01–E09) | D1–D5 |
| S6 | 7 (E01–E07) | D1–D4 |
| S7 | 7+ (E01–E07 so far) | D1–D3 so far |

## Disc read errors: clean and retry before troubleshooting further

A disc that fails partway through `dvdbackup -M` with repeated
`Error reading ... read error returned` messages (ending in `Mirror of DVD
failed`) may just need a physical clean (soft cloth, center-to-edge wipes)
rather than pointing to a drive or software problem — a clean + retry has
fixed this every time it's come up so far. Confirm the retry actually
completed (exit code 0, `dvdbackup` log free of `Error reading` lines)
before trusting the rip.

If a clean + retry still fails at the same spot, don't assume the whole
disc/side is a total loss — `dvdbackup -M -r a` still keeps whatever it
successfully copied before aborting, and if a title's content falls
entirely within that already-copied portion, it can often still be
recovered even though a normal HandBrakeCLI DVD-navigation extraction
won't work on the incomplete `VIDEO_TS` folder (`libdvdnav` needs *every*
titleset referenced in the disc's VMG to be present and will refuse to
scan *any* title, even ones whose actual video data is intact, if even one
other titleset failed to copy). Recovery method (worked on the Wonder
Woman Season 2 disc 1's other side, 2026-09-23):
1. Use `lsdvd -c` against the partial `VIDEO_TS` folder — the `.IFO`/`.BUP`
   metadata is usually written before the actual VOB data, so title/
   chapter structure (including which chapters/cells belong to which
   title) is often still readable even from an incomplete rip.
2. Work out which title's cells fall within what was actually copied.
   When one title is the "play all" concatenation of the others (the usual
   pattern — see the RFD workflow above), its chapter list tells you
   exactly where each real title's content starts/ends within the shared
   VOB stream, in seconds from the start.
3. Concatenate the fully-copied `VTS_XX_N.VOB` parts directly (skip
   `VTS_XX_0.VOB`, that's just the menu) — raw MPEG-PS VOB files split at
   arbitrary byte boundaries concatenate cleanly with plain `cat`:
   ```
   cat VTS_01_1.VOB VTS_01_2.VOB VTS_01_3.VOB VTS_01_4.VOB > title_raw.vob
   ```
4. Feed the concatenated file to HandBrakeCLI as a **plain file**, not a
   `VIDEO_TS` folder — this bypasses DVD navigation entirely, so the
   missing titlesets don't matter:
   ```
   HandBrakeCLI -i title_raw.vob --stop-at duration:<seconds+margin> \
     -o output.mp4 -e x264 -q 20 -B 160
   ```
   Use the target title's own duration (from step 1/2) plus a few seconds
   of margin for `--stop-at`; check the *actual* output duration against
   what was requested afterward — an exact match (no premature cutoff)
   means the needed footage was fully within the copied data.
5. Spot-check frames across the whole file, not just the middle — pull one
   every few hundred seconds through to near the end. A tiny/near-black
   final frame near the requested cutoff is usually just a normal
   end-of-episode fade, not corruption; garbled/pixelated macroblocks are
   the real tell of a bad recovery.

This does not always fully recover the target title — if the read error
happened before covering that title's full needed range, the result will
be truncated (missing the last however-many minutes), which may or may
not be worth keeping. Confirm actual vs. requested duration before
deciding.

## Verify the destination drive before trusting a batch write

`/Volumes/ExternalDrive` is a **mount point name**, not a fixed physical
disk — if the drive is disconnected/reconnected (or a different disk
happens to mount under the same name), macOS can silently swap which
physical volume answers to that path. This has actually happened
mid-session: previously-ripped `RFD/` and `McCloud/` folders vanished
entirely because a different drive ended up mounted at `/Volumes/
ExternalDrive`, and `HandBrakeCLI` then failed with `avio_open2 failed`
trying to write to a path whose parent directory no longer existed.
Before trusting a long ripping session's output, especially after any gap
or reconnect, sanity-check that the expected folders and used-space total
are actually present (`df -h /Volumes/ExternalDrive`, `ls`) rather than
assuming the mount point still points at the same disk.
