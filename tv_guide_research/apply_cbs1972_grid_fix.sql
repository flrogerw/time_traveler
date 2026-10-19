-- PROPOSAL ONLY: not applied. Replaces the 690-minute 08:30 slot (21 slots, 08:30-20:00).
BEGIN;
DELETE FROM time_slot_schedules WHERE channel_id = 1 AND schedule_id = 1972 AND channel_dow = 5 AND start_time >= '08:30' AND start_time < '20:00';
INSERT INTO time_slot_schedules (schedule_id, channel_id, start_time, duration, channel_dow) VALUES
  (1972, 1, '08:30', INTERVAL '3600 seconds', 5),
  (1972, 1, '09:30', INTERVAL '1800 seconds', 5),
  (1972, 1, '10:00', INTERVAL '1800 seconds', 5),
  (1972, 1, '10:30', INTERVAL '1800 seconds', 5),
  (1972, 1, '11:00', INTERVAL '1800 seconds', 5),
  (1972, 1, '11:30', INTERVAL '3600 seconds', 5),
  (1972, 1, '12:30', INTERVAL '1800 seconds', 5),
  (1972, 1, '13:00', INTERVAL '1800 seconds', 5),
  (1972, 1, '13:30', INTERVAL '1800 seconds', 5),
  (1972, 1, '14:00', INTERVAL '1800 seconds', 5),
  (1972, 1, '14:30', INTERVAL '1800 seconds', 5),
  (1972, 1, '15:00', INTERVAL '1800 seconds', 5),
  (1972, 1, '15:30', INTERVAL '1800 seconds', 5),
  (1972, 1, '16:00', INTERVAL '1800 seconds', 5),
  (1972, 1, '16:30', INTERVAL '1800 seconds', 5),
  (1972, 1, '17:00', INTERVAL '1800 seconds', 5),
  (1972, 1, '17:30', INTERVAL '1800 seconds', 5),
  (1972, 1, '18:00', INTERVAL '1800 seconds', 5),
  (1972, 1, '18:30', INTERVAL '1800 seconds', 5),
  (1972, 1, '19:00', INTERVAL '1800 seconds', 5),
  (1972, 1, '19:30', INTERVAL '1800 seconds', 5);
COMMIT;
