-- PROPOSAL ONLY: not applied.
BEGIN;
UPDATE schedules SET effective_start_date='1964-01-01', effective_end_date='1964-12-31' WHERE schedule_id=2086;
UPDATE schedules SET effective_start_date='1964-01-01', effective_end_date='1964-12-31' WHERE schedule_id=2133;
UPDATE schedules SET effective_start_date='1964-01-01', effective_end_date='1964-12-31' WHERE schedule_id=2144;
UPDATE schedules SET effective_start_date='1968-01-01', effective_end_date='1968-12-31' WHERE schedule_id=2352;
UPDATE schedules SET effective_start_date='1977-01-01', effective_end_date='1977-12-31' WHERE schedule_id=3016;
UPDATE schedules SET effective_start_date='1965-01-01', effective_end_date='1965-12-31' WHERE schedule_id=4034;
UPDATE schedules SET effective_start_date='1965-01-01', effective_end_date='1965-12-31' WHERE schedule_id=4048;
UPDATE schedules SET effective_start_date='1972-01-01', effective_end_date='1972-12-31' WHERE schedule_id=4120;
UPDATE schedules SET effective_start_date='1969-01-01', effective_end_date='1969-12-31' WHERE schedule_id=4444;
UPDATE schedules SET effective_start_date='1969-01-01', effective_end_date='1969-12-31' WHERE schedule_id=4445;
COMMIT;
