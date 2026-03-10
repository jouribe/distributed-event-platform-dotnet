# Architecture

## Style

- Clean Architecture
- Vertical Slice (per feature)
- Modular Monolith
- Two executables:
  - EventIngestion.Api
  - EventWorker

## Design principles

- PostgreSQL is the source of truth for event lifecycle.
- Redis is transport, not persistence.
- Workers are stateless and horizontally scalable.
- Duplicates are expected; idempotency is mandatory.

## Current scope

- In scope: ingestion, async processing, idempotency, retry, event tracking, and baseline observability.
- Out of scope for this release: full runbooks and next-phase reliability improvements.

## Components

Client
  ->
EventIngestion.Api
  ->
PostgreSQL (source of truth)
  ->
Redis Streams
  ->
EventWorker
  ->
PostgreSQL (status transitions)

## Runtime boundaries

- `EventIngestion.Api`
  - Validates request envelope
  - Enforces ingress idempotency
  - Persists event and outbox row atomically (single transaction)

- `OutboxPublisherService` (hosted inside `EventIngestion.Api` process)
  - Polls unpublished outbox rows
  - Publishes each event to Redis via `IEventPublisher`
  - Atomically marks the outbox row as published and transitions the event from `RECEIVED → QUEUED`
  - Tracks publish failures per row for observability

- `EventWorker`
  - Reads from Redis Consumer Group
  - Performs state transitions in PostgreSQL
  - Executes handler by `event_type`
  - Applies retry / terminal failure policy

## Processing flow

1. Client submits event.
2. API validates and persists event + outbox row atomically (status: `RECEIVED`).
3. `OutboxPublisherService` polls unpublished outbox rows, publishes to Redis, and transitions event to `QUEUED`.
4. Worker consumes event from Redis Consumer Group.
5. Worker updates state to `PROCESSING`.
6. Worker executes handler.
7. Worker sets `SUCCEEDED` or `FAILED_*`.
8. Worker ACKs message.

## Reliability notes

- ACK occurs only after durable state transition.
- Worker crashes before ACK are safe (redelivery expected).
- Duplicate deliveries are handled by idempotent handlers.

## Known limitations

- DLQ flow is not implemented yet.
- Manual intervention or external routing is required for `FAILED_TERMINAL` events.

## Key guarantees

- Ingress idempotency.
- At-least-once processing.
- Handler-level idempotency.
- Event status tracking.
