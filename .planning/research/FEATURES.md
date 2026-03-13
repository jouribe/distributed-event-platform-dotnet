# Features Research: DLQ + Observability for .NET 10 Event Platform

## Domain
DLQ management and operational observability for a distributed event processor (subsequent milestone on top of v0.1).

---

## Table Stakes
*Must have — operators cannot run in production without these.*

### DLQ Core

| Feature | Complexity | Notes |
|---------|-----------|-------|
| DLQ routing for FAILED_TERMINAL | MEDIUM | Worker writes to dedicated Redis `events:dlq` stream + `dlq_events` PostgreSQL table atomically when calling `MarkTerminalFailure`. Without this, terminal failures are silent. |
| `dlq_events` PostgreSQL table | LOW | Columns: `id`, `event_id` (FK), `tenant_id`, `event_type`, `failed_at`, `failure_reason`, `attempts`, `replayed_at` (nullable), `replayed_by` (nullable), `payload` (JSONB). Survives Redis restarts; enables admin queries. |
| Admin API `GET /admin/dlq` | MEDIUM | Paginated list. Filtered by `tenant_id` / `event_type`. Reads from PostgreSQL, not Redis. |
| Admin API `GET /admin/dlq/{eventId}` | LOW | Full DLQ row joined to event lifecycle history. |
| Admin API `POST /admin/dlq/{eventId}/replay` | HIGH | **Highest-risk feature.** Requires new `FAILED_TERMINAL → QUEUED` transition in `EventLifecycle.cs`. Bypasses HTTP ingestion to preserve original `EventId`. Records `replayed_at` on `dlq_events`. Writes atomically: event status update + outbox entry in one transaction. |
| `X-Admin-Key` middleware | LOW | Header-based auth with `IAdminAuthPolicy` interface. Modular — JWT upgrade replaces only the DI registration. |

### Observability

| Feature | Complexity | Notes |
|---------|-----------|-------|
| Prometheus `/metrics` endpoint | MEDIUM | Via `OpenTelemetry.Exporter.Prometheus.AspNetCore` — integrates with existing `System.Diagnostics.Metrics` instrumentation in `IngestionMetrics.cs`. Do NOT use prometheus-net (API conflict). |
| `dlq_depth_total` gauge | LOW | Labeled by `tenant_id` + `event_type`. Reads from `dlq_events WHERE replayed_at IS NULL`. Critical for alerting. |
| Processing latency histogram | MEDIUM | `event_processing_duration_seconds` measured in Worker from QUEUED→SUCCEEDED. Enables p50/p95/p99 SLOs. |
| Deep outbox lag health check | MEDIUM | Queries unpublished outbox rows older than configurable threshold. Returns `Degraded`/`Unhealthy`. Elevates `/health` from connectivity check to functional health. |
| DB + Redis deep health checks | LOW | `SELECT 1` for PostgreSQL; `PING` via StackExchange.Redis. Custom `IHealthCheck` implementations. |

---

## Differentiators
*Competitive/quality advantage — include in this milestone.*

| Feature | Complexity | Notes |
|---------|-----------|-------|
| Grafana alerting rules | LOW | Provisioned as JSON/YAML files in repo — code-owned, drift-proof. Rules: DLQ spike, processing lag, stale backlog threshold. |
| Tenant-scoped metric labels | LOW | All counters/gauges labeled by `tenant_id`. High value for multi-tenant operations. |
| Retry backlog inspector `GET /admin/retry/backlog` | LOW | Returns pending consumer group entries + stale message counts. Optionally bucketed: `due_now` / `scheduled` / `overdue`. |

---

## Anti-Features
*Explicitly do NOT build in this milestone.*

| Feature | Reason |
|---------|--------|
| Bulk DLQ replay | Dangerous without per-event review — operator must replay individually. Defer to v0.3. |
| DLQ auto-retry schedule | Creates infinite loops for permanently broken events. Terminal means terminal. |
| JWT/OAuth2 Admin API auth | Orthogonal scope. Modular middleware interface makes this a future swap, not a blocker. |
| Full Grafana dashboard UI | Dashboards drift. Maintain alert rules (code-owned) not dashboard JSON (UI-managed). |
| OpenTelemetry OTLP export | Adds collector infrastructure dependency. Prometheus scrape endpoint is sufficient for this milestone. |
| Payload search/filter in DLQ API | JSONB path query injection risk. `tenant_id` + `event_type` filter covers 90% of operator lookups. |

---

## Critical Dependency

**The replay feature (`POST /admin/dlq/{eventId}/replay`) requires a new domain transition `FAILED_TERMINAL → QUEUED` in `EventLifecycle.cs`.** This is the highest-risk item — it modifies the core invariant enforcement logic. It must be covered by state machine unit tests before any other replay code is built.

---

## Suggested Build Order

1. `dlq_events` migration (schema foundation)
2. DLQ routing in Worker (FAILED_TERMINAL → DLQ stream + table)
3. New `EventLifecycle.EnsureTransition(FAILED_TERMINAL → QUEUED)` + domain method + unit tests
4. Admin API endpoints (list, inspect, replay) + `X-Admin-Key` middleware
5. OpenTelemetry Prometheus export setup + metrics/gauges
6. Deep health checks (outbox lag + DB + Redis)
7. Processing latency histogram + retry backlog inspector API
8. Grafana alerting rules

---

## Dependencies Between Features

```
dlq_events migration
  └─► DLQ routing in Worker
        └─► dlq_depth_total gauge

EventLifecycle FAILED_TERMINAL→QUEUED transition
  └─► Admin API replay endpoint
        └─► dlq_events replayed_at update

OpenTelemetry setup
  └─► /metrics endpoint
        └─► all counters/gauges/histograms
              └─► Grafana alerting rules

Admin API auth middleware
  └─► all Admin API endpoints
```
