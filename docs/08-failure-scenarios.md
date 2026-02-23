# Failure Scenarios

## Duplicate HTTP requests
Handled by ingress idempotency.

## Worker crash before ACK
Broker redelivers.
Handler idempotency prevents duplicate effects.

## Redis restart
System reconnects.
Redelivery possible but safe.

## Database unavailable
API fails fast.
Worker retries or pauses.

## Unknown event_type

- Ingestion should reject request.
- If detected at worker stage, mark `FAILED_TERMINAL`.

## Poison message

- Repeated deterministic failures should end in `FAILED_TERMINAL`.
- Route to DLQ workflow when implemented.

## Operational checks

- Alert on spike of `FAILED_TERMINAL`.
- Alert on growing pending entries in Redis consumer group.
- Alert when retry backlog exceeds threshold.
