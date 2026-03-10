# Processing Model

## Delivery Semantics

Broker: at-least-once

Implication:
Handlers MUST be idempotent.

## Status Machine

RECEIVED
→ QUEUED
→ PROCESSING
→ SUCCEEDED

On failure:

PROCESSING
→ FAILED_RETRYABLE
→ QUEUED (after delay)

or

PROCESSING
→ FAILED_TERMINAL

## Status definitions

- `RECEIVED`: persisted in PostgreSQL, not yet visible to consumers.
- `QUEUED`: `OutboxPublisherService` has published the event to Redis and atomically recorded the transition.
- `PROCESSING`: worker claimed event and started handling.
- `SUCCEEDED`: handler completed and state persisted.
- `FAILED_RETRYABLE`: transient failure, retry eligible.
- `FAILED_TERMINAL`: no more retries, manual or DLQ flow needed.

## Worker startup sequence

On each startup the worker performs the following steps in order:

1. Ensures the Redis Consumer Group exists (creates it if absent).
2. Drains its own Pending Entries List (PEL) — messages previously read by this consumer instance but not yet ACKed.
3. Reclaims stale messages from crashed or slow consumers via `XAUTOCLAIM`. On Redis versions that do not support `XAUTOCLAIM`, the worker falls back to `XPENDING` + `XCLAIM`.

## Worker message loop

The worker continuously alternates between two read modes:

- `XREADGROUP >` — reads new, undelivered messages.
- `XAUTOCLAIM` (or `XPENDING` + `XCLAIM` fallback) — reclaims messages stuck in other consumers' PELs past the visibility timeout.

## Worker execution rules

- Transition to `PROCESSING` before invoking handler.
- Increment `attempts` when entering `PROCESSING` (one increment per processing attempt).
- Persist result state before ACK.
- On retryable error, compute `next_attempt_at` and requeue.
- On terminal error, stop retries and mark `FAILED_TERMINAL`.

## Domain invariants

- `source` is required and non-empty.
- `correlation_id` must be a non-empty UUID.
- `occurred_at` cannot be later than `received_at`.
- `next_attempt_at` is required only in `FAILED_RETRYABLE`; it must be null in all other statuses.
