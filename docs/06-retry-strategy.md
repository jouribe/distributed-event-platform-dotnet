# Retry Strategy

Max attempts: 5

Backoff:
2^attempt seconds
Cap: 60 seconds

Retry only on retryable errors (network, timeouts).

After max attempts:
Mark FAILED_TERMINAL.

## Retryable vs terminal

Retryable examples:

- transient network errors
- temporary service unavailability
- timeout exceptions

Terminal examples:

- schema/validation errors
- unknown event type
- invariant violations

## Formula

- delay = `min(2^attempt, 60)` seconds
- `attempt` starts at `1` for first retry

## DLQ direction

- Terminal failures should be forwarded to DLQ workflow in a later phase.
