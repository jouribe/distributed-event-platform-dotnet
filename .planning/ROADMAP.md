# Roadmap: Event Platform v0.2

## Overview

v0.1 shipped a production-pattern event processing pipeline (idempotent ingestion, outbox, Redis Streams consumer groups, lifecycle state machine). v0.2 hardens that pipeline for real operations: terminal failures are durably captured in a Dead Letter Queue, operators can inspect any event and replay dead-lettered events via HTTP APIs, every critical counter and latency is exported as Prometheus metrics, health endpoints give deep signal on infrastructure connectivity and outbox lag, and Grafana alerting rules fire before problems become incidents. A final refactor phase standardizes naming, removes dead code, and normalizes patterns across the codebase.

**Note:** Issue #66 (schema_version field) is in progress as a prerequisite — it is not a roadmap phase.
**Out of scope:** Issue #71 (k6 load testing) is excluded from this milestone.

Phases execute in dependency order: 1 → 2 → 3 → 4 → 5 → 6 (Phase 7 depends only on Phase 4; can be parallelized with Phase 5 or 6 if bandwidth allows). Phase 8 executes last.

## Phases

- [ ] **Phase 1: DLQ Schema and Persistence** - Establish the dlq_events table, IDlqRepository interface, and DlqRepository implementation that all subsequent DLQ work depends on
- [ ] **Phase 2: DLQ Routing and Domain Lifecycle Extension** - Wire terminal failure routing from Worker to DLQ Redis stream and dlq_events table, extend EventLifecycle with FAILED_TERMINAL → QUEUED transition, enforce ACK safety
- [ ] **Phase 3: Event Inspection API** - Expose read-only endpoints so operators can look up any event by ID and filter/list events without direct database access
- [ ] **Phase 4: Prometheus Metrics** - Expose /metrics via OpenTelemetry Prometheus exporter in both services; register all ingestion, DLQ, and processing counters/histograms/gauges; wire local Prometheus and Grafana stack with starter dashboard
- [ ] **Phase 5: Deep Health Checks** - Replace basic health endpoint with deep DB/Redis connectivity checks and configurable outbox lag health check in both services
- [ ] **Phase 6: Admin API** - Deliver secured /v1/events/{id}/requeue and /admin route group with DLQ list, inspect, and retry-backlog endpoints behind X-Admin-Key middleware with modular IAdminAuthPolicy
- [ ] **Phase 7: Grafana Alerting Rules** - Provision code-owned Grafana alerting rules for DLQ spike, processing lag, and stale backlog detection
- [ ] **Phase 8: Codebase Refactor** - Repository-wide refactoring for code standardization, cleanup, and consistency without behavior changes

## Phase Details

### Phase 1: DLQ Schema and Persistence
**Goal**: The dlq_events PostgreSQL table exists and is readable/writable through a clean repository interface, unblocking all DLQ routing, Admin API, and gauge instrumentation that depends on it
**Depends on**: Nothing (first phase of v0.2 milestone; v0.1 is complete)
**Requirements**: DLQ-01, DLQ-03
**Success Criteria** (what must be TRUE):
  1. A new `dlq_events` table exists in PostgreSQL with all required columns (event_id, tenant_id, event_type, payload JSONB, failure_reason, attempt_count, correlation_id, dead_lettered_at, replayed_at, replayed_by) after running DbMigrator
  2. IDlqRepository interface and DlqRepository implementation exist with InsertAsync, GetByIdAsync, ListAsync (paginated, filterable by tenant_id/event_type), and MarkReplayedAsync operations
  3. Terminal failure handling in the Worker atomically writes the dlq_events row and updates event status in a single transaction with no event loss window
  4. Unit tests cover DlqRepository operations; integration tests verify the atomic transaction against a real PostgreSQL container
**Plans**: TBD

### Phase 2: DLQ Routing and Domain Lifecycle Extension
**Goal**: Terminal failures in the Worker are routed to a configurable DLQ Redis stream and the dlq_events table with full metadata, the EventEnvelope domain model supports the FAILED_TERMINAL → QUEUED transition required for replay, and the Worker never ACKs a main stream entry when terminal failure persistence or DLQ publish fails
**Depends on**: Phase 1
**Requirements**: DLQ-02, DLQ-04
**Success Criteria** (what must be TRUE):
  1. When the Worker marks an event FAILED_TERMINAL, an entry appears on the configured DLQ Redis stream (default `events:dlq`, overridable via config) within the same processing cycle via XADD with approximate MAXLEN trimming
  2. The DLQ Redis message includes all required metadata: event_id, tenant_id, event_type, correlation_id, attempts, last_error, failed_at, source_entry_id
  3. IDlqRouter / DlqRouter coordinates PostgreSQL insert and Redis stream publish for terminal failures; Worker depends only on IDlqRouter
  4. If DlqRepository.InsertAsync fails OR DlqRouter.PublishAsync fails, the Worker does NOT call XACK on the main stream entry — the entry remains pending and is reclaimed by XAUTOCLAIM on the next cycle
  5. EventLifecycle.EnsureTransition() accepts FAILED_TERMINAL → QUEUED as a valid edge; all other invalid transitions from FAILED_TERMINAL still throw
  6. EventEnvelope exposes a ReplayFromDlq() domain method that transitions status and resets attempt count to 0
  7. State machine unit tests for the FAILED_TERMINAL → QUEUED edge are green before any replay handler code is written
**Plans**: TBD

### Phase 3: Event Inspection API
**Goal**: Operators can look up any event by ID and filter/list events without needing direct database access
**Depends on**: Phase 1
**Requirements**: INSP-01, INSP-02
**Success Criteria** (what must be TRUE):
  1. GET /v1/events/{id} returns status, attempts, next_attempt_at, last_error, correlation_id, schema_version, event_type, and tenant_id for a known event; returns 404 for an unknown ID
  2. GET /v1/events accepts query parameters tenant_id, status, from (timestamp), and to (timestamp); results are paginated and use deterministic ordering
  3. Both endpoints are accessible without authentication (read-only, trusted network assumption); no AuthZ redesign is required
  4. The inspection endpoints read from PostgreSQL via an existing or new query method on IEventRepository; no new infrastructure dependencies are introduced
**Plans**: TBD

### Phase 4: Prometheus Metrics
**Goal**: Operators can scrape /metrics from both EventIngestion.Api and EventWorker and see ingestion counters, DLQ counters, processing latency histograms, and DLQ depth gauge in Prometheus text format; a local Prometheus and Grafana stack is wired and a starter dashboard is provisioned in the repository
**Depends on**: Phase 2
**Requirements**: OBS-01, OBS-02, OBS-03, OBS-04, OBS-05, OBS-06
**Success Criteria** (what must be TRUE):
  1. GET /metrics returns Prometheus text format on both EventIngestion.Api and EventWorker; no 404 or 500
  2. `ingestion_requests_total` counter increments with correct labels (event_type, result) on each POST /events call; `dlq_routed_total` counter increments with correct labels (event_type, error_category) on each terminal failure
  3. `event_processing_duration_seconds` histogram in EventWorker records a sample for each event processed from QUEUED to SUCCEEDED; p50/p95/p99 buckets are present
  4. `dlq_depth_total` gauge reflects the current count of unreplayed dlq_events rows and updates after each DLQ write and each successful replay
  5. A docker-compose profile or documented setup in deployments/ runs Prometheus and Grafana locally and Prometheus successfully scrapes /metrics from both services
  6. A starter Grafana dashboard JSON file provisioned in the repository covers throughput, failures, retries, and DLQ backlog panels; the dashboard loads without errors in the local Grafana instance
**Plans**: TBD

### Phase 5: Deep Health Checks
**Goal**: Both services expose /health/ready with genuine infrastructure connectivity probes; the API additionally exposes an outbox lag check that returns Degraded or Unhealthy before an operator would otherwise notice a problem
**Depends on**: Phase 4
**Requirements**: HLT-01, HLT-02, HLT-03
**Success Criteria** (what must be TRUE):
  1. GET /health/ready on both services returns Healthy when PostgreSQL responds to SELECT 1 and Unhealthy when it does not
  2. GET /health/ready on both services returns Healthy when Redis responds to PING and Unhealthy when it does not
  3. GET /health/ready on EventIngestion.Api returns Degraded when unpublished outbox rows older than 30 seconds exist, and Unhealthy when lag exceeds 5 minutes; both thresholds are configurable via appsettings
**Plans**: TBD

### Phase 6: Admin API
**Goal**: An operator can list, inspect, and requeue dead-lettered events and inspect the retry backlog via secured HTTP endpoints, with auth middleware that can be swapped from static key to JWT without touching endpoint code
**Depends on**: Phase 1, Phase 2, Phase 4
**Requirements**: ADM-01, ADM-02, ADM-03, ADM-04, AUTH-01, AUTH-02
**Success Criteria** (what must be TRUE):
  1. GET /admin/dlq returns a paginated list of dead-lettered events filterable by tenant_id and event_type; requests without a valid X-Admin-Key header return 401
  2. GET /admin/dlq/{eventId} returns the full dlq_events row including failure_reason, attempt_count, and replayed_at for a known event; returns 404 for an unknown eventId
  3. POST /v1/events/{id}/requeue transitions a FAILED_TERMINAL (or FAILED_RETRYABLE) event to QUEUED (with attempts reset to 0), marks dlq_events.replayed_at when source state is FAILED_TERMINAL, and publishes the event to the main Redis stream; calling requeue on an already-QUEUED event returns success (200 with current state); invalid source state transitions return an explicit 4xx client error
  4. GET /admin/retry-backlog returns pending consumer group entry count and stale message count from Redis
  5. Replacing StaticKeyAuthPolicy with a JWT implementation requires only a DI registration change; no middleware or endpoint code is modified
**Plans**: TBD

### Phase 7: Grafana Alerting Rules
**Goal**: Code-owned Grafana alerting rules provisioned in the repository fire for DLQ spike, processing lag, and stale backlog conditions without requiring manual dashboard configuration
**Depends on**: Phase 4
**Requirements**: ALT-01, ALT-02, ALT-03
**Success Criteria** (what must be TRUE):
  1. A Grafana alerting rule file exists in the repository that fires when dlq_depth_total increases by a configurable threshold within a configurable time window
  2. A Grafana alerting rule exists that fires when event_processing_duration_seconds p95 exceeds a configurable threshold
  3. A Grafana alerting rule exists that fires when pending consumer group entry count exceeds a configurable threshold
  4. All alerting rule thresholds and windows are parameterized in the YAML/JSON rule files (no hardcoded values)
**Plans**: TBD

### Phase 8: Codebase Refactor
**Goal**: The repository-wide codebase is standardized, cleaned, and consistent across all layers with no behavior changes — naming conventions applied, dead code removed, duplicated logic consolidated, and test code improved
**Depends on**: Phase 7 (all feature phases complete)
**Requirements**: REF-01
**Success Criteria** (what must be TRUE):
  1. CI passes and the 70% coverage gate is satisfied after all refactoring changes
  2. No dead code, stale comments, or redundant abstractions remain in Domain, Application, Infrastructure, API, or Worker projects
  3. Naming conventions are applied consistently across all layers; layer boundary rules are valid (Application has no Infrastructure references, Domain has no Application references)
  4. Logging, validation, and error-handling patterns are normalized — no ad-hoc deviations from the established patterns in any project
  5. Test code duplication is reduced; fixture and helper reuse is improved across unit and integration test suites
**Plans**: TBD

## Progress

**Execution Order:**
Phases execute in dependency order: 1 → 2 → 3 → 4 → 5 → 6 (Phase 7 depends only on Phase 4; can be parallelized with Phase 5 or 6 if bandwidth allows). Phase 8 executes last after all feature phases.

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. DLQ Schema and Persistence | 0/? | Not started | - |
| 2. DLQ Routing and Domain Lifecycle Extension | 0/? | Not started | - |
| 3. Event Inspection API | 0/? | Not started | - |
| 4. Prometheus Metrics | 0/? | Not started | - |
| 5. Deep Health Checks | 0/? | Not started | - |
| 6. Admin API | 0/? | Not started | - |
| 7. Grafana Alerting Rules | 0/? | Not started | - |
| 8. Codebase Refactor | 0/? | Not started | - |
