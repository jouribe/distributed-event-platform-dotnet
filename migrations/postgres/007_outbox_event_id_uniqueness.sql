-- Ensure outbox idempotency at storage level: one outbox row per event_id.
-- Keep unpublished rows first so pending publish is never discarded during deduplication.
WITH ranked AS (
    SELECT
        id,
        row_number() OVER (
            PARTITION BY event_id
            ORDER BY (published_at IS NULL) DESC, created_at ASC, id ASC
        ) AS rn
    FROM event_platform.outbox_events
)
DELETE FROM event_platform.outbox_events o
USING ranked r
WHERE o.id = r.id
  AND r.rn > 1;

CREATE UNIQUE INDEX IF NOT EXISTS ux_outbox_events_event_id
    ON event_platform.outbox_events (event_id);
