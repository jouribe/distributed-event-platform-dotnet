# Requirements: Event Platform v0.2

**Defined:** 2026-03-12
**Core Value:** Events that enter the system are never silently lost — every failure is visible, operable, and recoverable by an operator.

## v1 Requirements

### DLQ Core

- [ ] **DLQ-01**: System atomically transitions event to FAILED_TERMINAL and writes dlq_events row in a single database transaction (follows InsertWithOutboxAsync pattern; no event loss window)
- [ ] **DLQ-02**: System routes FAILED_TERMINAL events to a dedicated Redis `events:dlq` stream via XADD with approximate MAXLEN trimming (PostgreSQL is the durable store)
- [ ] **DLQ-03**: System persists a dlq_events row for every terminal failure with: event_id, tenant_id, event_type, payload (JSONB), failure_reason, attempt_count, correlation_id, dead_lettered_at, replayed_at (nullable), replayed_by (nullable)
- [ ] **DLQ-04**: EventLifecycle.EnsureTransition supports FAILED_TERMINAL → QUEUED edge; EventEnvelope exposes ReplayFromDlq() domain method that resets attempts to 0; state machine unit tests cover this transition before any replay code is written

### Admin API

- [ ] **ADM-01**: Operator can list dead-lettered events via GET /admin/dlq with pagination and optional filters for tenant_id and event_type; results read from PostgreSQL
- [ ] **ADM-02**: Operator can inspect a specific dead-lettered event via GET /admin/dlq/{eventId} returning full dlq_events row including failure_reason, attempt_count, and replayed_at
- [ ] **ADM-03**: Operator can replay a dead-lettered event via POST /admin/dlq/{eventId}/replay; system follows DB-first ordering: TryTransitionStatusAsync(FAILED_TERMINAL→QUEUED) + attempts reset → MarkReplayedAsync → PublishAsync to main Redis stream; returns 409 on concurrent replay attempt
- [ ] **ADM-04**: Operator can inspect the retry backlog via GET /admin/retry-backlog returning pending consumer group entry count and stale message count from Redis

### Admin Auth

- [ ] **AUTH-01**: All /admin/* endpoints require valid X-Admin-Key header; requests without valid key return 401
- [ ] **AUTH-02**: Admin auth is implemented via IAdminAuthPolicy interface with StaticKeyAuthPolicy as the concrete implementation; upgrading to JWT requires only a DI registration swap with zero changes to middleware or endpoint code

### Observability — Metrics

- [ ] **OBS-01**: Both EventIngestion.Api and EventWorker expose GET /metrics in Prometheus text format via OpenTelemetry.Exporter.Prometheus.AspNetCore (bridges existing System.Diagnostics.Metrics instrumentation)
- [ ] **OBS-02**: System records ingestion_requests_total counter (labeled: event_type, result[accepted|idempotent_replay|rejected]) and dlq_routed_total counter (labeled: event_type, error_category[transient|validation|invariant|unknown])
- [ ] **OBS-03**: System records event_processing_duration_seconds histogram in EventWorker measuring time from QUEUED to SUCCEEDED, enabling p50/p95/p99 percentiles (labeled: event_type)
- [ ] **OBS-04**: System records dlq_depth_total gauge reflecting COUNT of dlq_events WHERE replayed_at IS NULL, labeled by event_type; updated after each DLQ write and replay

### Health Checks

- [ ] **HLT-01**: EventIngestion.Api and EventWorker expose deep readiness check at /health/ready that queries PostgreSQL connectivity (SELECT 1) tagged [ready]
- [ ] **HLT-02**: EventIngestion.Api and EventWorker expose deep readiness check for Redis connectivity (PING via StackExchange.Redis) tagged [ready]
- [ ] **HLT-03**: EventIngestion.Api exposes outbox lag health check: returns Degraded when unpublished outbox rows older than 30 seconds exist; returns Unhealthy when lag exceeds 5 minutes; thresholds are configurable via appsettings

### Alerting

- [ ] **ALT-01**: Grafana alerting rule fires when dlq_depth_total increases by a configurable threshold within a configurable window (DLQ spike detection)
- [ ] **ALT-02**: Grafana alerting rule fires when event_processing_duration_seconds p95 exceeds a configurable threshold (processing lag detection)
- [ ] **ALT-03**: Grafana alerting rule fires when pending consumer group entry count exceeds a configurable threshold (stale backlog detection)

## v2 Requirements

### DLQ Management

- **DLQ-V2-01**: Operator can replay multiple dead-lettered events in bulk with per-event confirmation
- **DLQ-V2-02**: Automated DLQ reprocessor consumer group on events:dlq stream with configurable retry policy

### Auth

- **AUTH-V2-01**: Admin API supports JWT/OAuth2 bearer token authentication (IAdminAuthPolicy swap — zero middleware changes required)

### Observability

- **OBS-V2-01**: OpenTelemetry OTLP export to collector for distributed tracing
- **OBS-V2-02**: Full Grafana dashboard (panels for ingestion rate, DLQ depth, processing latency, retry backlog)

## Out of Scope

| Feature | Reason |
|---------|--------|
| Bulk DLQ replay | Dangerous without per-event review; operator must replay individually to inspect each event |
| DLQ auto-retry schedule | Creates infinite loops for permanently broken events; terminal means terminal |
| JWT/OAuth2 for Admin API | Orthogonal scope; IAdminAuthPolicy interface is the upgrade path |
| Full Grafana dashboard UI | Dashboards drift; alert rules (code-owned YAML) are sufficient for this milestone |
| OpenTelemetry OTLP export | Adds collector infrastructure dependency; Prometheus scrape is sufficient |
| Payload search/filter in DLQ API | JSONB path query injection risk; tenant_id + event_type filter covers 90% of operator lookups |
| Exactly-once delivery | At-least-once with idempotent handlers is the delivery model |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| DLQ-01 | Phase 1 | Pending |
| DLQ-02 | Phase 2 | Pending |
| DLQ-03 | Phase 1 | Pending |
| DLQ-04 | Phase 2 | Pending |
| ADM-01 | Phase 5 | Pending |
| ADM-02 | Phase 5 | Pending |
| ADM-03 | Phase 5 | Pending |
| ADM-04 | Phase 5 | Pending |
| AUTH-01 | Phase 5 | Pending |
| AUTH-02 | Phase 5 | Pending |
| OBS-01 | Phase 3 | Pending |
| OBS-02 | Phase 3 | Pending |
| OBS-03 | Phase 3 | Pending |
| OBS-04 | Phase 3 | Pending |
| HLT-01 | Phase 4 | Pending |
| HLT-02 | Phase 4 | Pending |
| HLT-03 | Phase 4 | Pending |
| ALT-01 | Phase 6 | Pending |
| ALT-02 | Phase 6 | Pending |
| ALT-03 | Phase 6 | Pending |

**Coverage:**
- v1 requirements: 20 total
- Mapped to phases: 20
- Unmapped: 0 ✓

---
*Requirements defined: 2026-03-12*
*Last updated: 2026-03-12 after initial definition*
