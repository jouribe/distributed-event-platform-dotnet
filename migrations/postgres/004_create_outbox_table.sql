-- Outbox table for reliable event publishing
-- Ensures events published to Redis are not lost even if API crashes after publish but before DB update
CREATE TABLE IF NOT EXISTS event_platform.outbox_events (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id            uuid            NOT NULL REFERENCES event_platform.events(id) ON DELETE CASCADE,
    stream_name         text            NOT NULL,
    payload             jsonb           NOT NULL,
    created_at          timestamptz     NOT NULL DEFAULT now(),
    published_at        timestamptz     NULL,
    publish_attempts    integer         NOT NULL DEFAULT 0,
    last_error          text            NULL
);

-- Index for polling unpublished messages (background publisher)
CREATE INDEX IF NOT EXISTS ix_outbox_events_published_at
    ON event_platform.outbox_events (published_at)
    WHERE published_at IS NULL;

-- Index for cleanup of old published messages
CREATE INDEX IF NOT EXISTS ix_outbox_events_created_at
    ON event_platform.outbox_events (created_at);
