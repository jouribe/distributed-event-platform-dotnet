# Event Contract

## Envelope

```json
{
  "event_id": "uuid",
  "event_type": "domain.action",
  "occurred_at": "timestamp (ISO-8601)",
  "source": "string",
  "tenant_id": "string",
  "idempotency_key": "string",
  "correlation_id": "uuid",
  "payload": { "any": "json" }
}
```

## Required fields

- `event_type`
- `tenant_id`
- `idempotency_key`
- `payload`

## Rules

- `event_id` is generated if missing.
- `correlation_id` is generated if missing.
- `idempotency_key` is required for safe retries.
- `event_type` must map to a registered handler.

## Transport headers

- `Idempotency-Key` (preferred source for idempotency key)
- `X-Correlation-Id` (optional, generated when absent)

## Validation expectations

- Reject malformed or non-JSON payloads.
- Reject unknown `event_type` values.
- Reject empty or whitespace-only `tenant_id`.
- Preserve original payload without shape mutation.
