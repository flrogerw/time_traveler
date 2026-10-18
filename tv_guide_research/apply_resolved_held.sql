-- PROPOSAL ONLY: not applied.
BEGIN;
UPDATE schedules SET effective_start_date='1964-09-14', effective_end_date='1965-03-01' WHERE schedule_id=2086;
UPDATE schedules SET effective_start_date='1964-09-24', effective_end_date='1965-06-10' WHERE schedule_id=2133;
UPDATE schedules SET effective_start_date='1964-09-18', effective_end_date='1965-05-21' WHERE schedule_id=2144;
UPDATE schedules SET effective_start_date='1968-09-29', effective_end_date='1969-04-20' WHERE schedule_id=2352;
UPDATE schedules SET effective_start_date='1977-10-02', effective_end_date='1978-04-23' WHERE schedule_id=3016;
UPDATE schedules SET effective_start_date='1966-01-12', effective_end_date='1966-05-05' WHERE schedule_id=4034;
UPDATE schedules SET effective_start_date='1966-01-12', effective_end_date='1966-05-05' WHERE schedule_id=4048;
UPDATE schedules SET effective_start_date='1973-01-05', effective_end_date='1973-03-30' WHERE schedule_id=4120;
UPDATE schedules SET effective_start_date='1969-09-01', effective_end_date='1969-12-27' WHERE schedule_id=4444;
UPDATE schedules SET effective_start_date='1969-09-06', effective_end_date='1969-12-27' WHERE schedule_id=4445;
COMMIT;
