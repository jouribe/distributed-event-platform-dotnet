# ADR-004: Idempotency Strategy

## Status
Accepted

## Context
Clients may retry requests due to timeouts or transient failures.
Without idempotency, the system would create duplicated events and side effects.

## Decision
Ingress idempotency is enforced by:
- `Idempotency-Key` header (preferred)
- Scoped by `tenant_id`
- Database constraint: UNIQUE (tenant_id, idempotency_key)

Duplicate request behavior:
- Return existing `event_id` + current status
- Do not publish to broker again

Worker idempotency:
- Handlers must be idempotent (unique constraints / upserts / processed_events table)

## Consequences
- Clients must send Idempotency-Key to get full safety.
- We can safely accept retries without duplicating events.
