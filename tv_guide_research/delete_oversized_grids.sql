-- PROPOSAL ONLY: not applied. Deletes whole day grids (ABC 1966 Sat, ABC 1970 Mon, CBS 1972 Sat); they regenerate on the next run.
BEGIN;
DELETE FROM time_slot_schedules WHERE channel_id = 4 AND schedule_id = 1966 AND channel_dow = 5;
DELETE FROM time_slot_schedules WHERE channel_id = 4 AND schedule_id = 1970 AND channel_dow = 0;
DELETE FROM time_slot_schedules WHERE channel_id = 1 AND schedule_id = 1972 AND channel_dow = 5;
COMMIT;
