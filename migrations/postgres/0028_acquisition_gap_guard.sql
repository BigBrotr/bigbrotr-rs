-- Gap insertion already defers the complete terminal-job check until COMMIT.
-- An earlier SET CONSTRAINTS fails unless cancellation is complete; afterwards
-- the terminal job and gap are immutable. Rechecking every ordinary job UPDATE
-- adds no invariant and needlessly requires recovery-table grants for acceptance.
-- Preserve 0027 and all retained rows; only remove its redundant trigger.
DROP TRIGGER acquisition_abandonment_complete ON control.jobs;
