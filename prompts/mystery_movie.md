NBC Mystery Movie Rotation Logic

# NBC Mystery Movie — Rotation-Aware Scheduling Logic

 The collection may contain episodes from **The NBC Mystery Movie**, which was an umbrella anthology program containing multiple rotating component series.

 Do **not** treat every file as though it belongs to one ordinary weekly series.

 The scheduling/detection system must be **rotation-aware**.

 ## 1\. Identify the component series first

 For every episode, determine which underlying series the episode belongs to before applying relative-position or scheduling logic.

 Known component series include, among others:

 - Columbo
- McCloud
- McMillan & Wife
- Banacek
- Other component series that appeared in the NBC Mystery Movie rotation

 Do not assume the component series solely from filename if other evidence is available.

 If the identity is uncertain, flag it for verification rather than guessing.

---

 ## 2\. Treat the umbrella program and component program as separate layers

 There are two different concepts:

 ### Layer A — NBC Mystery Movie

 This is the umbrella/network scheduling structure.

 It determines things such as:

 - the night/time-slot structure
- which component series occupied a given slot
- the surrounding network presentation
- umbrella branding/opening/closing material
- potentially recurring network transitions or bumpers

 ### Layer B — Component series

 This is the actual program being broadcast during that week's Mystery Movie slot.

 For example:

 `NBC Mystery Movie → Columbo → specific Columbo episode`

 or:

 `NBC Mystery Movie → McCloud → specific McCloud episode`

 Commercial-break analysis must understand both layers.

---

 # 3\. Build a rotation map

 Create or maintain a structured rotation map containing, at minimum:

 - broadcast date
- episode
- component series
- season/episode where known
- Mystery Movie slot
- source filename
- episode runtime
- detected break positions
- verified break positions

 Do not infer a rotation solely from one episode.

 Build the map from the collection and available metadata.

 If the collection contains broadcast dates, use those as the primary scheduling evidence.

---

 # 4\. Use rotation information as CONTEXT, not as proof

 The rotation map may be used to predict where breaks are likely to occur.

 It must **never be used as proof that a break exists**.

 For example, if Columbo episodes from the same broadcast era normally have breaks around certain relative positions, that information can be used as a sanity check.

 But:

 > "A break normally occurs here"

 does NOT mean:

 > "There is a break here."

 The actual video evidence always wins.

---

 # 5\. Do not compare different component series as though they have identical structures

 Do not create one universal "Mystery Movie break template."

 A Columbo episode may have a different:

 - runtime
- act structure
- opening
- closing
- title sequence
- number of breaks
- break spacing
- placement of credits
- network presentation

 than a McCloud, McMillan & Wife, or Banacek episode.

 Relative-position analysis should therefore be performed primarily against **episodes of the same component series and comparable broadcast era**.

---

 # 6\. Account for changing rotations over time

 The Mystery Movie lineup changed over its run.

 Do not assume that a rotation pattern from one year applies unchanged to another year.

 The scheduling model should therefore be time-aware.

 Prefer comparisons in this order:

 1. Same component series
2. Same season/production era
3. Same broadcast era
4. Same Mystery Movie scheduling configuration
5. Broader historical patterns

 The farther down this list the evidence comes from, the weaker it should be treated.

---

 # 7\. Distinguish episode content from umbrella/network material

 When examining a transition, determine whether it belongs to:

 - the actual component episode
- the NBC Mystery Movie umbrella presentation
- a network bumper
- a commercial break
- a station/network identification
- a promo
- opening material
- closing material

 Do not automatically classify every transition between these elements as a commercial break.

 The objective is to reconstruct the **actual commercial-break locations in the source broadcast**, according to the project's definition of a commercial break.

---

 # 8\. Use neighboring episodes intelligently

 When multiple episodes from the same component series are available, use them as a structural reference.

 For example:

 - Compare act/break spacing.
- Compare opening-title behavior.
- Compare credit placement.
- Compare recurring bumpers.
- Compare unusually short or long black intervals.

 However, neighboring episodes are **contextual evidence only**.

 A structural pattern can identify something worth investigating; it cannot override what the actual frames show.

---

 # 9\. Detect anomalies rather than forcing conformity

 If one episode has a break structure that differs substantially from other episodes of the same series:

 **Do not automatically "correct" it to match the template.**

 Instead:

 1. Flag the anomaly.
2. Inspect the actual video.
3. Determine whether the difference is legitimate.
4. Check whether the source is a different broadcast/edit/version.
5. Only change the result if the video evidence supports the change.

 An unusual episode is not necessarily a bad episode.

---

 # 10\. Broadcast-version awareness

 The same underlying episode may exist in multiple versions:

 - original NBC broadcast
- syndicated version
- edited version
- later rerun
- DVD/home-video version
- different recording of the same broadcast
- recording containing network-specific material

 Do not assume two files representing the same episode have identical break structures.

 If two versions disagree, treat that as potentially meaningful.

 Record the discrepancy rather than silently forcing them to match.

---

 # 11\. Rotation-aware relative-position sanity checking

 When performing the final relative-position sanity check:

 **Never compare a Columbo episode against a McCloud episode simply because both belong to NBC Mystery Movie.**

 Instead:

 `NBC Mystery Movie`\
 → `component series`\
 → `era/version`\
 → `episode`

 The strongest comparison is between episodes at the deepest level for which reliable comparable data exists.

---

 # 12\. Unknown or newly discovered component series

 The system must not be hard-coded so tightly that an unknown component series causes failure.

 If an episode appears to belong to a component series not currently represented in the rotation map:

 1. Identify it.
2. Record it.
3. Add it to the rotation metadata.
4. Do not assume its break structure matches an existing series.
5. Establish its own structural baseline as enough episodes become available.

 If identification remains uncertain, ask me.

---

 # 13\. Rotation metadata belongs in the audit trail

 Any discovered or corrected information about the NBC Mystery Movie rotation must be recorded in:

 `COMMERCIAL_BREAK_AUDIT.md`

 Record:

 - the component series
- relevant date/era
- evidence used
- any uncertainty
- any resulting scheduling/detection rule

 Do not allow rotation knowledge to exist only implicitly in the code.

---

 # 14\. Important principle

 The rotation model is a **prior**, not ground truth.

 Think of the logic as:

 **Rotation information → predicts what to investigate**

 **Video evidence → determines what actually happened**

 Never reverse those two.

 If the schedule says one thing and the video says another, **the video wins**, and the discrepancy should be documented.

 ### OneFor this particular project, I'd thing I'd strongly recommend

 For this particular project, I'd have Claude build a **separate `mystery_movie_rotation` data file** rather than burying the rotation rules inside `rebuild_show.py`.

 Something conceptually like:

```
NBC Mystery Movie
│
├── 1971–72
│   ├── Columbo
│   ├── McMillan & Wife
│   └── McCloud
│
├── 1972–73
│   ├── Columbo
│   ├── McCloud
│   ├── McMillan & Wife
│   └── Banacek
│
└── later seasons
    └── ...
```

 Then your pipeline can reason:

 **episode → broadcast date → Mystery Movie era → component series → comparable episodes → expected structure**

 rather than:

 **episode → generic TV show → expected breaks**

 That distinction could be **very important** for your collection, especially if you're trying to reconstruct the original NBC broadcast rather than simply find commercial breaks in modern copies.

 And there's an even more interesting possibility: if you have a **large collection of Mystery Movie recordings with dates/filenames**, Claude could use the collection itself to **reverse-engineer the rotation and establish per-series/per-era break-position baselines**, while still requiring the actual frames to make the final decision.