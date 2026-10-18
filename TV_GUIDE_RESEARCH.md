# TV Guide research — what real daytime schedules looked like

**Standing rule:** schedules are built from this evidence (real TV Guide listings), not from guesses about genre.
When a scheduling rule is questioned, check it against these numbers first.

## Sources and coverage

| Source (archive.org item) | Issues | Years | Notes |
|---|---|---|---|
| `vintage-tv-guides` | 267 | 1965-01-16 … 1970 (weekly) | Northern California edition, ~15 stations (Bay Area + Sacramento + San Jose) |
| `tv-guide-collection_202108` | 32 files → 25 distinct | 1973-09, 1974-06, 1977-05, 1980 (17), 1982 (9), 1983-07, 1988-07 | mixed markets; 7 files are duplicates under two names (dropped in analysis) |

All 299 files were parsed in full: **524,302 listings**, every issue with all 7 days detected.
Not used: the 2000 and 2015–2021 issues (outside the 1950s–80s scope).

## Method (`tv_guide_research/`)

1. `download_guides.py` — fetches each issue's `_djvu.xml` (OCR with word coordinates, ~4 MB/issue).
2. `parse_guide.py` — rebuilds listings from word coordinates: day header (big weekday word), two page columns
   (split where the right column's rows begin), bold time markers at the column margin (`9:30`, `10AM`, `9 AM`),
   channel icons (counted, not identified), CAPS title with optional `-Genre` tag, `(C)` colour mark, description,
   `(NN min.)` length. Start times come from the nearest marker above a title in the same column, because in some
   layouts (1969) the OCR emits the time column apart from the titles. Pixel cutoffs scale with scan width (780–3200 px).
3. `run_parse_all.py` — parses everything in parallel into one JSONL (`listings_all.jsonl`, ~130 MB; regenerate, do not commit).
4. `analyze_genre_by_daypart.py` — where each kind of show (by the guide's own tag) actually aired.

Checked against page images (read by eye): 1965-01-16 Saturday morning, 1969-07-12 Saturday morning,
1980-04-26 Saturday morning — titles, tags, shared-channel icon counts and times match.

## Limits (be honest about these)

- OCR drops some bold time markers (e.g. `10:30`, `11AM` missing on a 1980 page), so a listing can inherit the
  previous marker's time. Block-level (2–3 hour) results hold; minute-level times do not.
- Roughly 15% of titles are lost or merged by OCR noise ("MicHTY mouse"); weekly repetition makes up for it in aggregate.
- Station identity is not captured (icons are counted). 1 icon ≈ independent/local, 3+ ≈ network feed.
- 1970s coverage is thin (3 issues); 1971–72 and 1978–79 have none. 1988 is a single noisy issue.
- Listings, not durations: only long programs print `(NN min.)`.

## Findings

### Where each kind of show aired (1965–70, % of that genre's listings, guide's own tags)

| genre | SAT 7–12 | SAT 12–19 | SUN 7–12 | SUN 12–19 | WKD 6–10 | WKD 10–16 | WKD 16–19 |
|---|---|---|---|---|---|---|---|
| cartoons/children | 14 | 3 | 6 | 1 | 26 | 14 | 36 |
| western | 4 | 15 | 6 | 14 | 5 | 12 | 46 |
| comedy | 1 | 4 | 2 | 6 | 6 | 41 | 39 |
| adventure/sci-fi | 5 | 22 | 4 | 14 | 2 | 10 | 43 |
| drama | 3 | 10 | 5 | 16 | 10 | 41 | 14 |
| crime/mystery/suspense | 2 | 18 | 4 | 11 | 3 | 27 | 35 |
| variety/music | 1 | 7 | 5 | 8 | 8 | 36 | 35 |
| game/quiz | 0 | 1 | 2 | 4 | 22 | 66 | 5 |
| serial (soap) | 0 | 0 | 0 | 0 | 1 | 95 | 4 |
| religion | 3 | 0 | 65 | 23 | 9 | 1 | 0 |
| education | 4 | 0 | 4 | 1 | 56 | 25 | 10 |

### What fills each slot (1965–70, share of tagged listings)

- **Saturday 7–12: 68% cartoons/children**, adventure 7, drama 8 (kids' live-action like Fury, plus movies),
  western 4, education 5, comedy 3, **crime/mystery/suspense 1** (30 of the 46 are `MOVIE—Mystery`, none is a crime series).
- Sunday 7–12: religion 47%, cartoons/children 19%; Sunday afternoon: drama, movies, public affairs, game shows.
- Weekday 6–10: education 22%, cartoons/children 38%; 10–16: soaps 21%, game 20%, comedy 17%, drama 14%;
  16–19: cartoons/children 31%, comedy 22%, variety 13%, adventure/western 19%.

### Top titles (all stations)

- **Saturday 7–9:** Cap'n Delta, Super 6, Brother Buzz, Jack's Place, Davey and Goliath, Super Heroes, Captain Kangaroo, Casper, Lone Ranger.
  **9–12:** Underdog, Flintstones, Beatles, Secret Squirrel, Casper, Herculoids, George of the Jungle, Frankenstein Jr., Fury, Jetsons,
  Top Cat, Journey to the Center of the Earth, Tennessee Tuxedo, Porky Pig, Archie, Mighty Mouse, Birdman.
  **12–19:** American Bandstand, bowling, wrestling, roller derby, horse racing, golf, Film Feature movies, westerns (Rifleman, Sky King,
  Death Valley Days, Have Gun), Perry Mason, Checkmate, Lost in Space, Time Tunnel, Voyage to the Bottom of the Sea.
- **Weekday 16–18:** Mike Douglas, Merv Griffin, Superman, Traventure Theatre, Flintstones, Cap'n Delta, Popeye, Leave It to Beaver,
  Cisco Kid, Dennis the Menace, Munsters, Cartoon Fun House, Patty Duke (cartoons + kid-friendly reruns).
- **1980–83 (21 issues):** Saturday 7–12 still all kids (Weekend Special, Popeye, Fat Albert, Kidsworld, Daffy Duck, Jetsons, Shazam!,
  Big Blue Marble, Scooby and Scrappy-Doo, Plastic Man). Weekday 16–19 is sitcom reruns (Happy Days, Brady Bunch, M*A*S*H,
  Andy Griffith, All in the Family, Mary Tyler Moore, Beverly Hillbillies, Gilligan's Island) plus PBS kids' shows.
  Weekday 6–10: PTL Club, Donahue, Today, Captain Kangaroo, Romper Room, Flintstones, Tom and Jerry, Sesame Street.

### Implications for scheduling

1. **Weekend flags must not be applied by genre alone.** The guides show crime/mystery series and adult sitcoms essentially never
   airing Saturday 7–12; sitcom reruns are a weekday-afternoon format.
2. Real stations also ran education, farm reports, religion, public affairs, sports and Spanish-language programs; the library has
   none of those, so the filler ladder substitutes the closest entertainment genres.
3. Weekday after-school shifts from cartoons (1965–70) to sitcom reruns (1980s); fill weights should follow the era.
4. Interstitials (School House Rock) are not shows: they air between programs on Saturday mornings.

---

## What is already in the database (check before building anything)

- **`shows.show_is_syndicated`** (boolean, 302 true / 52 false) exists and was pointed out as the data that says whether a show
  was syndicated. The syndicated scheduler does **not** use it today (`SyndicatedScheduler` picks from
  `show_syndication_eligibility` regardless), so the 52 false shows can still be picked for syndicated channels.
- **`schedules`** holds 4,215 real network lineups (CBS 1,457 / ABC 1,402 / NBC 1,321 / FOX 35), 1950-1990, with `show_id` set on
  only 36% of rows. About 3,900 rows start 19:00-22:00; only ~230 start 08:00-11:00 and the afternoons are almost empty (e.g. the
  whole 1976 ABC Saturday is Scooby-Doo 10:30 plus primetime). Network channels anchor on it via `Shows.get_scheduled_shows_id`
  and fill the rest. There are **no independent stations** in it. Real daytime/weekend lineups exist nowhere in the DB: the
  guides are the only source.
- **`commercials.commercial_type`** (food 122, hygiene 83, toy 57, smoking 47, cleaning 30, business 29, cereal 24, beer 18, psa 17,
  fastfood 15, apparel 14, auto 14, candy 12, medical 12, pets 7, camera 6, ...). Ratings exist only for `movies.movie_rating` and
  `specials.specials_rating`; series have none. The playlist reads breaks from the legacy `commercial_breaks`, not the audited
  `commercial_breaks_v2`.

## Syndication research (every show, 2026-10-18)

Goal: for each of the 351 real shows (not the `Local`/`Movie`/`Special` placeholders), was it ever syndicated and in what year.
Two passes of web research (20 researcher agents) plus the user's own pasted answers.

| | count |
|---|---|
| researched: syndicated yes (incl. "both") | 223 |
| researched: not syndicated | 9 (Centennial, French Chef [PBS/APT], Max Headroom, Moonlighting, My World and Welcome to It, New CBS Tuesday Night Movies, The Veil, Turn On, Sanford [cable-only BET]) |
| researched: unknown (web tools rate-limited, Wikipedia usually lacks rerun dates) | 119 |
| with a start year | 102 (40 high / 51 medium / 11 low confidence) |

Researched years are only as good as their source; unknown means **unresearched/inconclusive, not "no"**.

**User-provided verdicts for the 119 unknowns** (pasted by the user, 2026-10-18; raw text in
`tv_guide_research/data/user_syndication_verdicts_paste.txt`, parsed into `data/syndication_research.csv`):
33 syndicated ("hits", 32 with a start year, e.g. Adam-12 1975, Amen 1991, Bob Newhart 1978, Cannon 1976, Daktari 1969,
Hill Street Blues 1987, Kung Fu 1975, Laverne & Shirley 1981, Six Million Dollar Man 1978), 16 limited/delayed/specialty
(Cosmos, Cousteau Odyssey, Flip Wilson, Roots, The Prisoner, Thriller, ...), 70 "no" (mostly one-season shows and UK series).
These are **not independently verified** (many "years" are simply the year the network run ended); 29 of 30 start years are
within 2 years of the DB's `eligible_year_start` (Donna Reed 1966 vs 1963). One conflict with research: Sonny and Cher Comedy
Hour (researched yes 1981, paste says no).

`data/syndication_research.csv` is the merged file (research columns + `db_*` columns + `user_provided_*` columns). The user's
Desktop copy is `~/Desktop/syndication_research.csv`.

## Standing rules from the user (do not repeat these mistakes)

1. **No writes to the database.** Proposed data/schedule changes are delivered as files (CSV/SQL/JSON) for review; "apply",
   "fix it", "do it" are not permission to write. Use `dryrun_create_playlist.py` (rolls back, temp .m3u) instead of
   `create_playlist` for experiments. During this work flags, `fill_rules`, eligibility years and test schedule rows were
   written without permission and had to be restored (originals kept in `tv_guide_research/original_db_state/`; the
   eligibility copy there already contains an earlier edit to the School House Rock row, whose true original was
   `mapped_genre='kids'`, NULL years, Saturday midday/afternoon/early-evening and all-Sunday flags on).
2. **Use the guides' logic and the user's existing structure.** Genre-share/threshold rules inferred from the guides were
   rejected (they put Gunsmoke and Mannix on Saturday morning; some "evidence" was OCR noise from lost time markers). Those
   steps remain only behind `patch_guide_calibration.py --guide-rules` and are not approved.
3. **School House Rock is an interstitial, not a show:** it airs only between programs on Saturday mornings (1973-1985, ABC and
   syndicated channels), never in a slot of its own. The code supports it (`Episodes.get_interstitial_show_id` /
   `get_next_interstitial`, `channel_airs_interstitials`) but it is inert until the DB row says `mapped_genre='interstitial'`.
4. **Ad placement:** before noon, children's programming (and anything in a Saturday-morning slot) only gets kid-appropriate ads
   (`Commercials.CHILDREN_ALLOWED_TYPES`); afternoon and primetime take any ad. Packed cartoon shorts get one commercial budget
   with pods between the shorts; interstitials get no ads of their own.
5. **Filler logic:** a real lineup anchors the day; where the library lacks the show, `SyndicatedScheduler.resolve_filler`
   substitutes accuracy-first (see CLAUDE.md). Shows are built around their real lengths (`--duration-aware`, opt-in).
