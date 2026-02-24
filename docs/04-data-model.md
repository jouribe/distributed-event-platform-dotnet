# Data Model

Table: events

- id (uuid, PK)
- tenant_id (text)
- event_type (text)
- occurred_at (timestamptz)
- received_at (timestamptz)
- source (text)
- payload (jsonb)
- idempotency_key (text)
- correlation_id (uuid)
- status (text)
- attempts (int)
- next_attempt_at (timestamptz)
- last_error (text)

Constraints:

UNIQUE (tenant_id, idempotency_key)

Recommended domain constraints:

- `source` must be non-empty.
- `correlation_id` must be non-empty UUID.
- `occurred_at <= received_at`.
- `next_attempt_at` must be null unless `status = FAILED_RETRYABLE`.

## Recommended indexes

- `idx_events_status_next_attempt_at` on `(status, next_attempt_at)`
- `idx_events_correlation_id` on `(correlation_id)`
- `idx_events_received_at` on `(received_at DESC)`

## Status constraint

Recommended allowed values:

- `RECEIVED`
- `QUEUED`
- `PROCESSING`
- `SUCCEEDED`
- `FAILED_RETRYABLE`
- `FAILED_TERMINAL`

## Notes

- `attempts` starts at `0` and increments per processing attempt.
- `next_attempt_at` is null unless retry is scheduled.
- `last_error` stores sanitized, non-sensitive diagnostics.
