CREATE TABLE IF NOT EXISTS event_platform.events (
  id                uuid PRIMARY KEY,

  tenant_id          text        NOT NULL,
  event_type         text        NOT NULL,

  occurred_at        timestamptz NOT NULL,
  received_at        timestamptz NOT NULL DEFAULT now(),

  payload            jsonb       NOT NULL,

  idempotency_key    text        NULL,
  correlation_id     uuid        NOT NULL,

  status             text        NOT NULL,
  attempts           integer     NOT NULL DEFAULT 0,
  next_attempt_at    timestamptz NULL,
  last_error         text        NULL
);

-- Idempotency per tenant (only when key is present)
CREATE UNIQUE INDEX IF NOT EXISTS ux_events_tenant_idempotency_key
  ON event_platform.events (tenant_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

-- Worker polling
CREATE INDEX IF NOT EXISTS ix_events_status_next_attempt_at
  ON event_platform.events (status, next_attempt_at);

-- Tenant timeline
CREATE INDEX IF NOT EXISTS ix_events_tenant_received_at_desc
  ON event_platform.events (tenant_id, received_at DESC);

-- Type timeline
CREATE INDEX IF NOT EXISTS ix_events_type_received_at_desc
  ON event_platform.events (event_type, received_at DESC);

-- Correlation tracing
CREATE INDEX IF NOT EXISTS ix_events_correlation_id
  ON event_platform.events (correlation_id);

-- Guardrails
ALTER TABLE event_platform.events
  ADD CONSTRAINT chk_events_attempts_nonneg CHECK (attempts >= 0);

ALTER TABLE event_platform.events
  ADD CONSTRAINT chk_events_tenant_id_not_blank CHECK (length(btrim(tenant_id)) > 0);

ALTER TABLE event_platform.events
  ADD CONSTRAINT chk_events_event_type_not_blank CHECK (length(btrim(event_type)) > 0);

ALTER TABLE event_platform.events
  ADD CONSTRAINT chk_events_status_allowed
  CHECK (status IN (
    'RECEIVED',
    'QUEUED',
    'PROCESSING',
    'SUCCEEDED',
    'FAILED_RETRYABLE',
    'FAILED_TERMINAL'
  ));
