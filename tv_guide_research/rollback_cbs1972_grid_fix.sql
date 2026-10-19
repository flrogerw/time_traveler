BEGIN;
DELETE FROM time_slot_schedules WHERE channel_id = 1 AND schedule_id = 1972 AND channel_dow = 5 AND start_time >= '08:30' AND start_time < '20:00';
INSERT INTO time_slot_schedules (schedule_id, channel_id, start_time, duration, channel_dow) VALUES
  (1972, 1, '08:30', INTERVAL '41400 seconds', 5);
COMMIT;
