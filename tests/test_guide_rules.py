"""
Tests for the EXPERIMENTAL TV Guide-derived rule functions (tv_guide_research/guide_rules.py) on small synthetic
evidence. The rules are not applied to the database (see patch_guide_calibration.py --guide-rules).
"""
from tv_guide_research.guide_rules import (
    derive_block_weights, derive_show_flags, eligible, merged_slot_shares,
)

SYNTHETIC = {
    "issues_per_era": {"1965-66": 100, "1980-83": 20},
    "slot_shares": {"1965-66": {
        "SAT 07-12": {"cartoons/children": 0.76, "adventure/sci-fi": 0.06, "drama": 0.06, "comedy": 0.03,
                      "crime/mystery/suspense": 0.01},
        "WKD 16-19": {"comedy": 0.30, "cartoons/children": 0.25, "adventure/sci-fi": 0.12, "drama": 0.10,
                      "crime/mystery/suspense": 0.03},
    }},
    "show_slots": {},
    "block_mix": {"1965-66": {"SAT|9": {"kids": 80, "drama": 10, "western": 5, "family": 3, "adventure": 2}}},
}


def flags_for(genre, mapped, direct=None):
    ev = {**SYNTHETIC, "show_slots": {"1": direct} if direct else {}}
    return derive_show_flags(ev, 1, genre, mapped)


def test_kids_show_is_eligible_in_a_childrens_dominated_slot():
    assert flags_for("cartoon", "kids")["saturday_morning"] is True


def test_crime_and_sitcom_are_not_eligible_where_they_were_a_sliver_of_the_slot():
    assert flags_for("crime,scifi,drama", "adventure")["saturday_morning"] is False   # crime 1%
    assert flags_for("comedy", "family")["saturday_morning"] is False                 # comedy 3% < 4%


def test_sitcom_is_eligible_in_weekday_late_afternoon_where_comedy_dominates():
    assert flags_for("comedy", "family")["weekday_after_school"] is True


def test_dominated_slot_needs_a_strongly_represented_genre_or_direct_evidence():
    # adventure/sci-fi is 6% of Saturday morning: plausible, but not enough in a dominated slot...
    assert flags_for("scifi,adventure", "adventure")["saturday_morning"] is False
    # ...unless the show's own listings put it there
    assert flags_for("scifi,adventure", "adventure", {"SAT 07-12": 10, "SAT 12-19": 40})["saturday_morning"] is True


def test_a_few_stray_listings_do_not_count_as_evidence():
    # 2 of 400 listings in the slot (OCR can stamp a wrong time) must not switch a crime show on
    assert flags_for("crime,drama", "drama", {"SAT 07-12": 2, "WKD 16-19": 398})["saturday_morning"] is False


def test_show_with_no_genre_basis_is_left_alone():
    assert eligible(set(), "SAT 07-12", merged_slot_shares(SYNTHETIC), {}) is None


def test_block_weights_follow_the_guides_mix_for_categories_that_have_shows():
    w = derive_block_weights(SYNTHETIC, "SAT", 9, 1965, {"kids", "drama", "western"})
    assert set(w) == {"kids", "drama", "western"}      # family/adventure have no eligible show; adventure < 2% anyway
    assert abs(sum(w.values()) - 1) < 0.001
    assert w["kids"] > 0.7


def test_block_weights_empty_when_the_guides_have_too_little():
    assert derive_block_weights(SYNTHETIC, "SUN", 9, 1965, {"kids"}) == {}
