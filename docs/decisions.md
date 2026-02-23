# Architectural Decisions (Index)

This file tracks architectural decisions using ADRs (Architecture Decision Records).

## Accepted

### ADR-001: Architecture Style
- **Decision:** Clean Architecture + Vertical Slice
- **Reason:** Maintainability, clear boundaries, easy modular extraction later
- **File:** docs/adr/ADR-001-architecture-style.md

### ADR-002: Broker Choice (Redis Streams)
- **Decision:** Redis Streams + Consumer Groups
- **Reason:** ACK support, pending tracking, consumer groups, lightweight ops
- **File:** docs/adr/ADR-002-broker-choice.md

### ADR-003: Delivery Semantics (At-least-once)
- **Decision:** At-least-once delivery
- **Reason:** Practical distributed systems approach; exactly-once is costly
- **File:** docs/adr/ADR-003-delivery-semantics.md

### ADR-004: Idempotency Strategy
- **Decision:** Ingress idempotency using Idempotency-Key scoped by tenant + UNIQUE constraint
- **Reason:** Client retries must not duplicate events
- **File:** docs/adr/ADR-004-idempotency-strategy.md

### ADR-005: PostgreSQL as Source of Truth
- **Decision:** Postgres stores event envelope + status transitions; Redis is transport only
- **Reason:** Reliability, auditability, retry scheduling
- **File:** docs/adr/ADR-005-persistence-source-of-truth.md

## Proposed (Recommended)

### ADR-006: Outbox Pattern for Publish Reliability
- **Decision:** Outbox table + background publisher
- **Reason:** Atomic persist + publish to prevent lost publishes
- **File:** docs/adr/ADR-006-outbox.md

## Notes
- The project is built as a standalone solution first.
- Once stable, reusable packages/templates can be extracted in a later phase.

## ADR lifecycle

- `Accepted`: active decision to follow.
- `Proposed`: recommended but not yet enforced.
- `Superseded`: replaced by a newer ADR (must reference replacement).
