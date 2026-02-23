# ADR-003: Delivery Semantics (At-least-once)

## Status
Accepted

## Context
Distributed systems experience network issues and process crashes.
Exactly-once semantics are expensive and complex.

## Decision
Adopt at-least-once delivery semantics.

Implementation rules:
- Worker ACKs only after state is persisted safely.
- Handlers must be idempotent.
- Duplicate deliveries are expected and treated as normal.

## Consequences
- Some messages may be processed more than once.
- Side effects must be protected via unique constraints, upserts, or processed-events barriers.
