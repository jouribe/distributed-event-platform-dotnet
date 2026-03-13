# Project Research Summary

**Project:** DLQ + Observability for .NET 10 Event Platform
**Domain:** Distributed event processing — operational hardening milestone (v0.2)
**Researched:** 2026-03-12
**Confidence:** HIGH

## Executive Summary

This milestone adds Dead Letter Queue (DLQ) routing, an Admin API for inspection and replay, Prometheus metrics exposition, and deep health checks on top of the existing v0.1 event processing platform. The platform already has a well-established architecture (Outbox pattern, Redis Streams Consumer Groups, Dapper/PostgreSQL, Clean Architecture boundaries), and every new component must follow those established conventions rather than introduce parallel patterns. The research is unusually concrete because it was conducted against the actual existing codebase — not against a greenfield problem — which raises confidence well above typical research.

The recommended approach is incremental and dependency-ordered: the `dlq_events` schema must exist before any routing or API code is written; the `FAILED_TERMINAL → QUEUED` domain transition must be tested before replay is implemented; OpenTelemetry metrics must be wired before any new counters are registered; and health checks must be designed with configurable thresholds from day one. Only 2 new NuGet packages are required (`OpenTelemetry.Exporter.Prometheus.AspNetCore` and `OpenTelemetry.Extensions.Hosting`) — everything else reuses the existing Dapper, StackExchange.Redis, and PostgreSQL infrastructure already in place.

The two highest-risk areas are DLQ atomicity (event loss window if `MarkTerminalFailureAsync` and `IDlqRepository.InsertAsync` are not in the same transaction) and replay correctness (bypassing domain transition enforcement, wrong ordering of DB vs Redis operations, and forgetting to reset the `attempts` counter to 0). Both are entirely preventable if the design is addressed in Phase 1 and Phase 2 respectively. All other pitfalls are moderate and have clear, low-effort mitigations.

## Key Findings

### Recommended Stack

The existing infrastructure handles DLQ persistence (Dapper + Npgsql), DLQ routing to Redis (`StackExchange.Redis.IDatabase.StreamAddAsync`), and Admin API auth (custom 30-line `IMiddleware` + `IAdminAuthPolicy` interface). The only genuine gap is Prometheus exposition: the existing `IngestionMetrics.cs` already uses `System.Diagnostics.Metrics` (`Meter`/`Counter<T>`), which is the standard .NET instrumentation API that OpenTelemetry reads natively. Adding `prometheus-net` would create an incompatible parallel metrics API and must not be used.

See `.planning/research/STACK.md` for full package rationale.

**Core technologies:**
- `OpenTelemetry.Exporter.Prometheus.AspNetCore` 1.9.0: Prometheus `/metrics` endpoint — bridges existing `System.Diagnostics.Metrics` instrumentation to Prometheus format with zero migration cost
- `OpenTelemetry.Extensions.Hosting` 1.9.0: OTel host integration for both API and Worker services
- Existing Dapper + Npgsql: DLQ persistence via new `dlq_events` table — no new ORM
- Existing StackExchange.Redis: DLQ Redis stream routing via `StreamAddAsync` to `events:dlq`
- Custom `IAdminAuthPolicy` interface: modular Admin API auth — static key now, JWT-swap later with zero middleware changes
- `Microsoft.Extensions.Diagnostics.HealthChecks` (in-framework): deep health checks with custom Dapper queries — no xabaril packages needed

### Expected Features

See `.planning/research/FEATURES.md` for full feature table with complexity ratings.

**Must have (table stakes):**
- DLQ routing for `FAILED_TERMINAL` events to `events:dlq` Redis stream + `dlq_events` PostgreSQL table
- `GET /admin/dlq` — paginated, filterable by `tenant_id` / `event_type`, reads from PostgreSQL
- `GET /admin/dlq/{eventId}` — full DLQ row with lifecycle history
- `POST /admin/dlq/{eventId}/replay` — highest-risk feature; requires `FAILED_TERMINAL → QUEUED` domain transition and attempts reset
- `X-Admin-Key` middleware with `IAdminAuthPolicy` interface
- Prometheus `/metrics` endpoint via OpenTelemetry
- `dlq_depth_total` gauge labeled by `tenant_id` + `event_type`
- `event_processing_duration_seconds` histogram for p50/p95/p99 SLOs
- Deep outbox lag health check (`Degraded` / `Unhealthy` with configurable thresholds)
- DB + Redis connectivity health checks

**Should have (differentiators — include in this milestone):**
- Grafana alerting rules as provisioned YAML/JSON in repo (code-owned, drift-proof)
- Tenant-scoped metric labels on all counters and gauges
- `GET /admin/retry/backlog` — pending consumer group entries + stale message counts

**Defer (v0.3+):**
- Bulk DLQ replay — dangerous without per-event review
- DLQ auto-retry schedule — creates infinite loops for permanently broken events
- JWT/OAuth2 Admin API auth — modular interface makes this a future swap
- Full Grafana dashboard UI — alert rules are code-owned; dashboard JSON is UI-managed
- OpenTelemetry OTLP export — Prometheus scrape is sufficient for this milestone
- Payload search/filter in DLQ API — JSONB path query injection risk; `tenant_id` + `event_type` covers 90% of lookups

**Critical dependency:** The replay endpoint requires `FAILED_TERMINAL → QUEUED` added to `EventLifecycle.EnsureTransition()`. This modifies core invariant enforcement and must be unit-tested before any replay code is written.

### Architecture Approach

All new components fit within existing Clean Architecture boundaries. `IDlqRouter` and `IDlqRepository` are Application-layer interfaces; their implementations live in Infrastructure. The Admin API is added to the existing `EventIngestion.Api` project (not a new service) — it already has all required DI registrations and connection config. Metrics are recorded at call-site in Worker and API handlers, never inside domain objects. Replay goes through `TryTransitionStatusAsync` (existing optimistic-concurrency guard) with DB-first ordering: commit status change, mark DLQ entry replayed, then publish to Redis.

See `.planning/research/ARCHITECTURE.md` for component diagrams, data flow sequences, and interface signatures.

**Major components:**
1. `IDlqRouter` / `DlqRouter` (Infrastructure) — coordinates atomic PostgreSQL insert + Redis stream publish for terminal failures; Worker only depends on this single interface
2. `IDlqRepository` / `DlqRepository` (Infrastructure) — Dapper CRUD over `dlq_events` table; read by Admin API, written by `DlqRouter`
3. Admin route group in `EventIngestion.Api` — `/admin` MapGroup with `AdminAuthFilter` endpoint filter; hosts DLQ list/inspect/replay and retry backlog endpoints
4. `WorkerMetrics` (new) + extended `IngestionMetrics` (existing) — `System.Diagnostics.Metrics` instrumentation at call-site; exposed via OTel Prometheus exporter
5. `OutboxLagHealthCheck`, `PostgresHealthCheck`, `RedisHealthCheck` — custom `IHealthCheck` implementations with configurable thresholds

### Critical Pitfalls

See `.planning/research/PITFALLS.md` for full details including warning signs and prevention code.

1. **Non-atomic DLQ routing (event loss window)** — `MarkTerminalFailureAsync` and `IDlqRepository.InsertAsync` must execute in a single database transaction, following the established `InsertWithOutboxAsync` pattern. Design this in Phase 1; retrofitting is painful.
2. **Replay bypassing domain transition** — `FAILED_TERMINAL → QUEUED` is not a valid edge today. Add `ReplayFromDlq()` to `EventEnvelope` calling `EventLifecycle.EnsureTransition()`, write state machine unit tests, then build replay code. Never issue a raw SQL UPDATE bypassing the domain.
3. **Replay reusing exhausted attempt count** — replayed events have `Attempts = MaxAttempts`. The replay transition SQL must include `attempts = 0` or the event immediately re-dead-letters on the first Worker error.
4. **Redis publish before PostgreSQL commit on replay** — always DB-first: `TryTransitionStatusAsync` → `MarkReplayedAsync` → `PublishAsync`. A Redis publish before a failed DB commit produces a Worker processing an event still marked `FAILED_TERMINAL`.
5. **Prometheus cardinality explosion** — never use `event_id`, `correlation_id`, or raw error strings as metric label values. Only bounded values: `event_type` (bounded by config), `error_category` (enum), `stream` (two values). `tenant_id` requires case-by-case evaluation.
6. **Health check false positives from outbox lag** — never threshold at zero. Return `Degraded` for lag > 30s, `Unhealthy` only for lag > 5 minutes. Tag as `["ready"]` not `["live"]`.

## Implications for Roadmap

Based on the dependency graph in FEATURES.md and the build order in ARCHITECTURE.md, a 6-phase structure is well-supported by the research:

### Phase 1: DLQ Schema and Persistence Foundation

**Rationale:** Every other DLQ-related component (Worker routing, Admin API, metrics gauge) depends on the `dlq_events` table existing and `IDlqRepository` being available. This is the unavoidable foundation.
**Delivers:** `dlq_events` PostgreSQL migration, `IDlqRepository` interface, `DlqRepository` implementation, database migration execution.
**Addresses:** DLQ Core table stakes (schema portion).
**Avoids:** Pitfall 1 (non-atomic DLQ routing) — the combined `MarkTerminalAndWriteDlqAsync` transaction method must be designed here, not retrofitted later.

### Phase 2: DLQ Routing in Worker + Domain Lifecycle Extension

**Rationale:** The Worker's DLQ routing path and the `FAILED_TERMINAL → QUEUED` lifecycle transition are tightly coupled — both modify the Worker's terminal failure handling and both require domain unit tests before any Admin API code is written.
**Delivers:** `IDlqRouter` / `DlqRouter`, Worker wiring for terminal failure → DLQ, `EventEnvelope.ReplayFromDlq()` domain method, `FAILED_TERMINAL → QUEUED` edge in `EventLifecycle`, state machine unit tests, `maxLength` trimming on DLQ Redis stream.
**Addresses:** DLQ routing table stake, DLQ Redis stream retention.
**Avoids:** Pitfall 2 (replay bypassing domain transition), Pitfall 8 (DLQ Redis stream unbounded growth).

### Phase 3: Prometheus Metrics

**Rationale:** No schema changes, no new Admin HTTP surface — safe to ship independently after the DLQ routing path exists (so `dlq_depth_total` has real data to measure). Establishes label conventions before any new counter is added.
**Delivers:** OTel Prometheus setup in both services, `/metrics` endpoint, `dlq_depth_total` gauge, `worker_events_processed_total` counter, `worker_events_dlq_total` counter, `event_processing_duration_seconds` histogram, tenant-scoped labels on all metrics.
**Addresses:** All Observability table stakes (metrics subset).
**Avoids:** Pitfall 4 (cardinality explosion) — label conventions locked in during this phase.

### Phase 4: Deep Health Checks

**Rationale:** Pure read-only, no schema changes, no admin surface. Depends on Phase 3 (Worker has HTTP host). Isolated scope means zero regression risk.
**Delivers:** `PostgresHealthCheck` (`SELECT 1`), `RedisHealthCheck` (`PING`), `OutboxLagHealthCheck` (configurable `Degraded` / `Unhealthy` thresholds), `outbox_pending_total` Prometheus gauge.
**Addresses:** Observability table stakes (health check subset).
**Avoids:** Pitfall 5 (health check false positives) — thresholds designed from day one, not after first false alert.

### Phase 5: Admin API (Inspect + Replay)

**Rationale:** Depends on Phase 1 (DLQ table to query), Phase 2 (DLQ entries and `FAILED_TERMINAL → QUEUED` transition), and Phase 3 (metrics to record admin actions). Replay is the highest-risk feature and must follow strict DB-first ordering.
**Delivers:** `/admin` route group with `AdminAuthFilter`, `GET /admin/dlq`, `GET /admin/dlq/{eventId}`, `POST /admin/dlq/{eventId}/replay` (with attempts reset), `GET /admin/retry/backlog`, `IAdminAuthPolicy` / `StaticKeyAuthPolicy`.
**Addresses:** All Admin API table stakes, retry backlog differentiator.
**Avoids:** Pitfall 2 (domain bypass), Pitfall 3 (Redis-before-DB on replay), Pitfall 6 (admin auth tight coupling), Pitfall 7 (replay reusing exhausted attempt count).

### Phase 6: Grafana Alerting Rules

**Rationale:** Pure configuration — YAML/JSON files provisioned in repo. Depends on Phase 3 (metrics emitting). No code risk, pure operational value.
**Delivers:** Code-owned Grafana alerting rules for DLQ spike, processing lag, stale backlog threshold. Provisioned via repo, not via Grafana UI.
**Addresses:** Grafana alerting differentiator.
**Avoids:** Dashboard drift (alert rules are code-owned, not UI-managed dashboard JSON).

### Phase Ordering Rationale

- Phases 1 → 2 are strictly ordered by hard dependency: the DLQ table must exist before routing writes to it, and the domain transition must be tested before replay is built.
- Phase 3 is independent of Phase 2 conceptually but benefits from having real DLQ data; placing it here keeps the Admin API scope clean in Phase 5.
- Phase 4 is independent of Phase 5 and has no regression risk — inserting it before the Admin API reduces the total surface area of Phase 5.
- Phase 5 is the only phase with meaningful cross-cutting risk (domain invariants + DB/Redis ordering + auth). All prerequisite foundations are in place by this point.
- Phase 6 has zero code risk and can be parallelized with Phase 5 if bandwidth allows.

### Research Flags

Phases with well-documented patterns — skip `research-phase`:
- **Phase 1:** Established repository pattern, identical to existing `IEventRepository` / `IOutboxRepository` — straightforward implementation.
- **Phase 3:** OTel Prometheus integration is well-documented; existing `IngestionMetrics.cs` shows the exact pattern to follow.
- **Phase 4:** Standard `IHealthCheck` pattern; thresholds are the only design decision and are resolved in research.
- **Phase 6:** Pure YAML/JSON configuration; Grafana alerting rules have extensive documentation.

Phases that may benefit from deeper research during planning:
- **Phase 2:** The combined `MarkTerminalAndWriteDlqAsync` atomic transaction design needs careful review of the existing `InsertWithOutboxAsync` implementation to ensure the pattern is applied consistently. Recommend reading the existing implementation before writing the new method.
- **Phase 5:** The replay flow touches the most critical invariant enforcement in the system (`EventLifecycle.EnsureTransition`). Recommend a pre-implementation checklist review against Pitfalls 2, 3, 6, and 7 before writing any replay handler code.

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH | Research conducted against the actual codebase; existing `IngestionMetrics.cs` verified; package versions confirmed against NuGet. Only 2 new packages required. |
| Features | HIGH | Feature set derived from operational requirements of the existing system; dependency graph is explicit and verified against current code structure. |
| Architecture | HIGH | Component boundaries follow established Clean Architecture pattern already in use; interface signatures are concrete and match existing conventions exactly. |
| Pitfalls | HIGH | All 8 pitfalls are grounded in the specific codebase patterns (e.g., `InsertWithOutboxAsync`, `TryTransitionStatusAsync`, `IngestionMetrics`) — not generic advice. |

**Overall confidence:** HIGH

### Gaps to Address

- **Attempts reset SQL**: The exact SQL for `TryTransitionStatusAsync` with `attempts = 0` needs to be confirmed against the existing `TryTransitionStatusAsync` implementation in `EventRepository.cs` before Phase 5. The research specifies the requirement clearly; the implementation detail needs to match the existing method signature.
- **Tenant cardinality**: Whether `tenant_id` is safe as a Prometheus label depends on the actual number of tenants in production. The research flags this as potentially unbounded. Validate the tenant count before including it in metric labels — if bounded to <100 tenants, it is safe.
- **Worker HTTP host for health checks**: Phase 4 assumes `EventWorker` has an HTTP host to expose health endpoints. Verify this is already configured in `EventWorker/Program.cs` before Phase 4 begins.

## Sources

### Primary (HIGH confidence)
- Existing codebase (`src/`, `tests/`) — all architectural decisions validated against actual implementation files
- `.NET 10 System.Diagnostics.Metrics` official documentation — Meter/Counter/Histogram API
- `OpenTelemetry.Exporter.Prometheus.AspNetCore` 1.9.0 NuGet — Prometheus exporter for .NET
- `Microsoft.Extensions.Diagnostics.HealthChecks` — in-framework, no version uncertainty

### Secondary (MEDIUM confidence)
- OpenTelemetry .NET documentation — OTel Prometheus integration patterns
- Redis Streams documentation — `StreamAddAsync` `maxLength` / `useApproximateMaxLength` parameters
- Grafana alerting provisioning documentation — YAML rule file format

### Tertiary (LOW confidence)
- General Prometheus cardinality guidance — label value bounds; tenant cardinality must be validated against actual production data

---
*Research completed: 2026-03-12*
*Ready for roadmap: yes*
