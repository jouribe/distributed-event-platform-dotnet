# Event Platform

## What This Is

A production-oriented distributed event processing platform built with .NET 10, Redis Streams, and PostgreSQL. It provides idempotent HTTP ingestion, reliable asynchronous processing with at-least-once delivery, exponential-backoff retry, and full event lifecycle tracking. The platform serves as both a reference architecture for distributed systems patterns and a foundation for real production usage.

## Core Value

Events that enter the system are never silently lost — every failure is visible, operable, and recoverable by an operator.

## Requirements

### Validated

- ✓ Idempotent HTTP ingestion (`POST /events` with tenant-scoped idempotency key) — v0.1
- ✓ Atomic outbox write (event + outbox row in single transaction) — v0.1
- ✓ OutboxPublisherService relays events from PostgreSQL to Redis Streams — v0.1
- ✓ Redis Consumer Groups with stale message reclaim (XAUTOCLAIM / XCLAIM fallback) — v0.1
- ✓ Event lifecycle state machine: `RECEIVED → QUEUED → PROCESSING → SUCCEEDED` — v0.1
- ✓ Retry path: `PROCESSING → FAILED_RETRYABLE → QUEUED` with exponential backoff (max 5 attempts, cap 60s) — v0.1
- ✓ Terminal failure: `PROCESSING → FAILED_TERMINAL` — v0.1
- ✓ Correlation ID propagation (header → DB → Redis → logs) — v0.1
- ✓ Basic health endpoint — v0.1
- ✓ DbMigrator with sequential SQL migrations — v0.1

### Active

- [ ] Route `FAILED_TERMINAL` events to a dedicated Redis DLQ stream
- [ ] Persist DLQ entry in PostgreSQL (new `dlq_events` table) with failure details
- [ ] Admin API: list and inspect dead-lettered events
- [ ] Admin API: replay a dead-lettered event (`FAILED_TERMINAL → QUEUED` + re-publish to main Redis stream, preserving EventId)
- [ ] Admin API secured via `X-Admin-Key` header with modular auth middleware (replaceable with JWT/OAuth2)
- [ ] Prometheus `/metrics` endpoint: ingestion/retry/DLQ counters + processing latency histograms
- [ ] Deep health checks: DB connectivity + Redis connectivity + outbox lag gauge
- [ ] Retry backlog inspector API: pending consumer group entries + stale message count
- [ ] Structured Grafana alerting rules: DLQ spike, processing lag, stale backlog threshold

### Out of Scope

- Exactly-once delivery guarantees — at-least-once with idempotent handlers is the model
- Business-specific domain logic — infrastructure and reference architecture only
- Multi-region replication — single-region for now
- Full operational runbooks — alerting rules are documented but runbook prose is deferred
- JWT/OAuth2 for Admin API — deferred; auth middleware must be modular to enable this later

## Context

v0.1 is complete and production-pattern compliant. The EventEnvelope aggregate uses a private constructor (`CreateNew` / `RehydrateFromPersistence`), state transitions are enforced by `EventLifecycle.EnsureTransition()`, and the outbox guarantees no event loss if Redis is temporarily unavailable at ingestion time.

The worker reclaims stale pending messages on startup and uses XAUTOCLAIM (with XPENDING+XCLAIM fallback for older Redis). The replay Admin API must re-publish directly to the main Redis stream (not via HTTP ingestion) to preserve the original EventId as the traceability anchor.

Metrics should be exposed via the `prometheus-net` library or OpenTelemetry with Prometheus exporter — choose whichever integrates more cleanly with the existing ASP.NET Core setup.

## Constraints

- **Tech stack**: .NET 10, PostgreSQL (Dapper), Redis Streams — no ORM, no MassTransit
- **Domain invariants**: EventEnvelope private constructor and lifecycle state machine must not be bypassed; replay transitions through domain methods only
- **Idempotency**: Replay preserves original EventId; PostgreSQL is the source of truth for event status
- **Auth modularity**: Admin API auth middleware must be replaceable — no tight coupling to static key implementation
- **Migration pattern**: All schema changes via sequential SQL files in `migrations/postgres/`

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| DLQ as separate Redis stream | Isolates poison messages from main processing without blocking consumer group | — Pending |
| PostgreSQL DLQ table | Durable record survives Redis restarts; enables admin queries without touching Redis | — Pending |
| Replay bypasses HTTP ingestion | Preserves original EventId for traceability; avoids re-validation of already-validated events | — Pending |
| X-Admin-Key with modular auth | High development velocity for reference architecture; explicitly designed for JWT upgrade path | — Pending |
| Prometheus via prometheus-net | Mature .NET library, ASP.NET Core middleware integration, no OTLP collector required | — Pending |

---
*Last updated: 2026-03-12 after initialization*
