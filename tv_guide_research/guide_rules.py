"""
Scheduling rules derived from the TV Guide evidence (guide_evidence.json, built by build_evidence.py).
Pure functions -- no DB access -- so they are testable and the patch script can re-run them anywhere.

  derive_show_flags()   which of the 17 daypart flags a show should have
  derive_block_weights() the fill_rules category weights for a (day group, block, era)

See TV_GUIDE_RESEARCH.md for the evidence behind the thresholds.
"""
import collections
import math

MIN_SHARE = 0.04          # a genre must be >= 4% of a slot's tagged series listings to be plausible there
STRONG_SHARE = 0.20       # ... and >= 20% to be a major part of it
DOMINATED_SHARE = 0.45    # a slot whose top genre is >= 45% is dominated (Saturday 7-12 is ~76% children's):
                          # there only strongly represented genres, or a show with direct evidence, qualify
WELL_SEEN, WELL_SEEN_SHARE, WELL_SEEN_MIN = 8, 0.08, 3   # decided by the show's own listings alone
MANY_HITS, MANY_HITS_SHARE = 6, 0.03
SOME_SEEN, SOME_SEEN_SHARE = 3, 0.25
MIN_WEIGHT = 0.02         # fill categories under 2% of a block's mix are dropped

FLAG_SLOT = {
    "saturday_early_morning": "SAT 07-12", "saturday_morning": "SAT 07-12",
    "saturday_midday": "SAT 12-19", "saturday_afternoon": "SAT 12-19", "saturday_early_evening": "SAT 12-19",
    "sunday_early_morning": "SUN 07-12", "sunday_morning": "SUN 07-12",
    "sunday_midday": "SUN 12-19", "sunday_afternoon": "SUN 12-19", "sunday_early_evening": "SUN 12-19",
    "weekday_early_morning": "WKD 06-10", "weekday_morning": "WKD 06-10",
    "weekday_late_morning": "WKD 10-16", "weekday_noon": "WKD 10-16", "weekday_early_afternoon": "WKD 10-16",
    "weekday_after_school": "WKD 16-19", "weekday_early_evening": "WKD 16-19",
}
# fill_rules block (day group, start hour) -> flag column
BLOCK_FLAG = {
    ("WKD", 6): "weekday_early_morning", ("WKD", 8): "weekday_morning", ("WKD", 10): "weekday_late_morning",
    ("WKD", 12): "weekday_noon", ("WKD", 14): "weekday_early_afternoon", ("WKD", 16): "weekday_after_school",
    ("WKD", 18): "weekday_early_evening",
    ("SAT", 7): "saturday_early_morning", ("SAT", 9): "saturday_morning", ("SAT", 12): "saturday_midday",
    ("SAT", 14): "saturday_afternoon", ("SAT", 17): "saturday_early_evening",
    ("SUN", 7): "sunday_early_morning", ("SUN", 9): "sunday_morning", ("SUN", 12): "sunday_midday",
    ("SUN", 14): "sunday_afternoon", ("SUN", 17): "sunday_early_evening",
}
TOKEN_CLASS = {
    "cartoon": "cartoons/children", "children": "cartoons/children", "kids": "cartoons/children",
    "educational": "education", "comedy": "comedy", "sitcom": "comedy", "parody": "variety/music",
    "satire": "variety/music", "variety": "variety/music", "music": "variety/music", "musical": "variety/music",
    "talk": "variety/music", "drama": "drama", "romance": "drama", "war": "adventure/sci-fi",
    "action": "adventure/sci-fi", "adventure": "adventure/sci-fi", "scifi": "adventure/sci-fi",
    "scfi": "adventure/sci-fi", "fantasy": "adventure/sci-fi", "crime": "crime/mystery/suspense",
    "mystery": "crime/mystery/suspense", "thriller": "crime/mystery/suspense", "horror": "crime/mystery/suspense",
    "western": "western", "game": "game/quiz", "family": "FAMILY",
}
# fill_rules eras -> the guide eras that describe them (thin eras blend with their neighbours)
ERA_SOURCES = {
    1960: ["1965-66"],
    1965: ["1965-66", "1967-68", "1969-70"],
    1970: ["1969-70", "1973-77"],
    1975: ["1973-77", "1980-83"],
    1980: ["1980-83"],
    1985: ["1980-83"],
}


def merged_slot_shares(evidence):
    """slot -> {genre class: highest share across the guide eras}."""
    out = collections.defaultdict(dict)
    for slots in evidence["slot_shares"].values():
        for slot, shares in slots.items():
            for cls, v in shares.items():
                out[slot][cls] = max(out[slot].get(cls, 0.0), v)
    return out


def show_classes(genre, mapped):
    if mapped == "kids":   # our own category for the children's/cartoon shows
        return {"cartoons/children"}
    toks = [t.strip() for t in (genre or "").lower().split(",") if t.strip()]
    classes = {TOKEN_CLASS[t] for t in toks if t in TOKEN_CLASS}
    if not classes and mapped in TOKEN_CLASS:
        classes = {TOKEN_CLASS[mapped]}
    return classes


def eligible(classes, slot, shares, direct):
    """True/False for a show in a guide slot, or None when there is no basis either way.
    direct = {slot: that show's own guide listings}."""
    total = sum(direct.values())
    here = direct.get(slot, 0)
    # 1. the show's own listings: a clear presence in the slot turns it on
    if (total >= WELL_SEEN and here >= WELL_SEEN_MIN and here / total >= WELL_SEEN_SHARE) or \
            (total >= WELL_SEEN and here >= MANY_HITS and here / total >= MANY_HITS_SHARE) or \
            (SOME_SEEN <= total < WELL_SEEN and here / total >= SOME_SEEN_SHARE):
        return True
    if not classes:
        return None
    share = shares[slot]

    def share_of(c):
        if c == "FAMILY":   # family = comedy or children's
            return max(share.get("comedy", 0.0), share.get("cartoons/children", 0.0))
        return share.get(c, 0.0)

    # 2a. a dominated slot: only strongly represented genres qualify
    if max(share.values(), default=0.0) >= DOMINATED_SHARE:
        return all(share_of(c) >= STRONG_SHARE for c in classes)
    # 2b. otherwise a genre that is implausible in the slot vetoes it
    if any(share_of(c) < MIN_SHARE for c in classes):
        return False
    # 3. a well-seen show that never showed up there, with no dominant genre, is not airing there
    if total >= WELL_SEEN and not any(share_of(c) >= STRONG_SHARE for c in classes):
        return False
    return True


def derive_show_flags(evidence, show_id, genre, mapped, shares=None):
    """{flag: True/False} for the flags the guides give a basis for (others are left out)."""
    shares = shares or merged_slot_shares(evidence)
    direct = evidence["show_slots"].get(str(show_id), {})
    classes = show_classes(genre, mapped)
    out = {}
    for flag, slot in FLAG_SLOT.items():
        verdict = eligible(classes, slot, shares, direct)
        if verdict is not None:
            out[flag] = verdict
    return out


def derive_block_weights(evidence, group, start_hour, era_start, eligible_categories):
    """fill_rules weights for one block+era: the guides' category mix of that block, restricted to
    categories that have at least one eligible show, dropping anything under MIN_WEIGHT, renormalised.
    Returns {} when the guides give nothing usable (the caller keeps the existing weights)."""
    key = f"{group}|{start_hour}"
    acc, wsum = collections.Counter(), 0.0
    for era in ERA_SOURCES[era_start]:
        counts = evidence["block_mix"].get(era, {}).get(key)
        if not counts:
            continue
        total = sum(counts.values())
        if total < 15:      # too few listings to say anything
            continue
        w = math.sqrt(evidence["issues_per_era"].get(era, 1))
        for cat, n in counts.items():
            acc[cat] += w * n / total
        wsum += w
    if not wsum:
        return {}
    mix = {c: v / wsum for c, v in acc.items() if c in eligible_categories}
    mix = {c: v for c, v in mix.items() if v >= MIN_WEIGHT}
    total = sum(mix.values())
    return {c: round(v / total, 4) for c, v in mix.items()} if total else {}
