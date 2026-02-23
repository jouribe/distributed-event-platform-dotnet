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

## Components

Client
  ↓
EventIngestion.Api
  ↓
PostgreSQL (source of truth)
  ↓
Redis Streams
  ↓
EventWorker
  ↓
PostgreSQL (status transitions)

## Runtime boundaries

- `EventIngestion.Api`
  - Validates request envelope
  - Enforces ingress idempotency
  - Persists event as durable record
  - Publishes (or schedules publish via outbox)

- `EventWorker`
  - Reads from Redis Consumer Group
  - Performs state transitions in PostgreSQL
  - Executes handler by `event_type`
  - Applies retry / terminal failure policy

## Processing Flow

1. Client submits event.
2. API validates and persists event (RECEIVED).
3. API publishes event to Redis.
4. Worker consumes event.
5. Worker updates state to PROCESSING.
6. Worker executes handler.
7. Worker sets SUCCEEDED or FAILED_*.
8. Worker ACKs message.

## Reliability notes

- ACK occurs only after durable state transition.
- Worker crashes before ACK are safe (redelivery expected).
- Duplicate deliveries are handled by idempotent handlers.

## Key Guarantees

- Ingress idempotency.
- At-least-once processing.
- Handler-level idempotency.
- Event status tracking.
