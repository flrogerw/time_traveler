-- PROPOSAL ONLY: not applied. Shows reviewed in assign_slots_ui.py with no slots selected.
BEGIN;
UPDATE shows SET show_is_syndicated = FALSE WHERE show_id IN (180, 304, 326, 132, 13, 178, 336, 312, 66, 225, 311);
COMMIT;
