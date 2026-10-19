BEGIN;
DELETE FROM fill_rules WHERE year_start IS NULL AND start_time IN ('23:00', '00:00')
  AND end_time IN ('23:59:59', '06:00');
ALTER TABLE show_syndication_eligibility DROP COLUMN IF EXISTS late_night;
COMMIT;
