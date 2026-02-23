# Idempotency Strategy

## Ingress

Use Idempotency-Key header.

If (tenant_id, idempotency_key) exists:
- Return existing event_id.
- Do not publish again.

Recommended API behavior:

- Return `200 OK` with existing `event_id` and `status`.
- Include a response flag like `idempotency_replayed=true`.

## Worker

Handlers must prevent duplicate side effects:

Options:
- Unique business constraints
- Upsert
- processed_events table

## Boundary of responsibility

- Ingress idempotency prevents duplicate event creation.
- Worker idempotency prevents duplicate side effects.
- Both are required for at-least-once systems.
