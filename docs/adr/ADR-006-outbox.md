# ADR-006: Outbox Pattern for Publish Reliability

## Status
Proposed (Recommended)

## Context
Writing to Postgres and publishing to Redis are two separate operations.
Crashes can create inconsistency:
- event stored but not published
- published but not stored (rare, but possible depending on order)

## Decision
Implement Outbox:
- API writes event + outbox row in the same DB transaction
- A publisher component reads unsent outbox rows and publishes to Redis
- After successful publish, mark outbox as sent

Recommended outbox fields:

- `id`
- `event_id`
- `stream_name`
- `payload`
- `created_at`
- `published_at`
- `publish_attempts`

## Consequences
- Stronger delivery guarantees (no lost publish).
- Extra table and background publisher.
- Adds complexity but improves correctness.
