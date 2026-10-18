-- PROPOSED database change(s) for the new scheduler code. NOT executed. Review, then run it yourself if you want it.
-- The only data change the new code needs: make School House Rock a Saturday-morning interstitial
-- (it airs between programs, never in a slot of its own; 1973-1985, ABC and syndicated channels).
-- Before this change the row is: mapped_genre 'kids', eligible_year_start/end NULL, flags
--   weekday_early_morning, weekday_morning, weekday_early_afternoon, weekday_after_school, saturday_midday,
--   saturday_afternoon, saturday_early_evening, all sunday_* = TRUE; everything else FALSE.

BEGIN;

UPDATE show_syndication_eligibility
SET mapped_genre = 'interstitial',
    eligible_year_start = 1973,
    eligible_year_end = 1985,
    weekday_early_morning = FALSE, weekday_morning = FALSE, weekday_late_morning = FALSE, weekday_noon = FALSE,
    weekday_early_afternoon = FALSE, weekday_after_school = FALSE, weekday_early_evening = FALSE,
    saturday_early_morning = TRUE, saturday_morning = TRUE, saturday_midday = FALSE, saturday_afternoon = FALSE,
    saturday_early_evening = FALSE,
    sunday_early_morning = FALSE, sunday_morning = FALSE, sunday_midday = FALSE, sunday_afternoon = FALSE,
    sunday_early_evening = FALSE
WHERE show_id = 150;   -- School House Rock

-- expect exactly 1 row updated, then COMMIT; otherwise ROLLBACK.
COMMIT;

-- To undo, put the row back to its original values (see the comment at the top).
