# Architecture Research: DLQ + Observability for .NET 10 Event Platform

## Component Placement

### Where DLQ Routing Lives

```
Worker.cs (decision point)
  ↓  calls
IDlqRouter (Application/Abstractions) — narrow interface, keeps Worker DI graph small
  ↓  implemented by
DlqRouter (Infrastructure) — coordinates two side effects:
  ├─► IDlqRepository.InsertAsync()          → PostgreSQL dlq_events table
  └─► IConnectionMultiplexer.StreamAddAsync  → Redis events:dlq stream
```

**Worker only knows `IDlqRouter`** — it does not directly reference `IDlqRepository` or Redis. This keeps `Worker.cs` testable by mocking a single interface.

### Where the Admin API Lives

**Add to existing `EventIngestion.Api`** — not a new service.

Rationale:
- Already has DI registrations for `IEventRepository`, `IEventPublisher`, `IDbConnectionFactory`
- Adding `IDlqRepository` to the same DI container is trivial
- A separate Admin service would require duplicating connection config and repository registrations
- Admin traffic is low-frequency; isolation gain is negligible at this stage

Implementation: `/admin` route group with scoped `IEndpointFilter`:

```csharp
var adminGroup = app.MapGroup("/admin")
    .AddEndpointFilter<AdminAuthFilter>();

adminGroup.MapGet("/dlq", AdminDlqEndpoints.ListAsync);
adminGroup.MapGet("/dlq/{eventId:guid}", AdminDlqEndpoints.GetAsync);
adminGroup.MapPost("/dlq/{eventId:guid}/replay", AdminDlqEndpoints.ReplayAsync);
adminGroup.MapGet("/retry-backlog", AdminRetryBacklogEndpoints.GetAsync);
```

### Where Metrics Are Instrumented

**At call-site in Worker handlers and API handlers — NOT in the domain layer.**

- `IngestionMetrics.cs` (exists): extend with `dlq_depth_total` gauge
- `WorkerMetrics.cs` (new): `worker_events_processed_total`, `worker_events_dlq_total`, `event_processing_duration_seconds` histogram
- Record metrics **after** domain operations complete, not inside `EventEnvelope` methods

### How Replay Maintains EventEnvelope Invariants

Replay goes through the existing `TryTransitionStatusAsync` optimistic-concurrency guard:

```
Admin replay request
  ↓
Load EventEnvelope via IEventRepository.GetByIdAsync()
  ↓
Guard: status must be FAILED_TERMINAL (return 409 if not)
  ↓
IEventRepository.TryTransitionStatusAsync(eventId, FAILED_TERMINAL, QUEUED)
  → returns false = concurrent replay won → 409 Conflict
  → returns true = this request owns the replay
  ↓
IDlqRepository.MarkReplayedAsync(dlqEntryId)   [set replayed_at]
  ↓
IEventPublisher.PublishAsync(envelope)         [re-publish to main stream]
  ↓
200 OK { eventId, status: "QUEUED" }
```

**No new domain method needed** — `TryTransitionStatusAsync` already exists in `IEventRepository`. The `FAILED_TERMINAL → QUEUED` transition must be added to `EventLifecycle.EnsureTransition()` and covered by state machine unit tests before replay code is built.

---

## Data Flows

### DLQ Routing Flow

```
Worker.HandleEntryWithPersistenceAsync
    attempts >= MaxAttempts
        ↓
    eventRepository.MarkTerminalFailureAsync(eventId, error)
    [events table: status=FAILED_TERMINAL]
        ↓
    dlqRouter.RouteAsync(eventId, error)
        ↓ (both must succeed — DlqRouter handles partial failure retry)
    dlqRepository.InsertAsync(dlqEntry) → dlq_events table
    database.StreamAddAsync("events:dlq") → Redis DLQ stream
        ↓
    WorkerMetrics.DlqRoutedTotal.Add(1, tenant_id, event_type)
        ↓
    return true  (XACK main stream entry)
```

### Admin Replay Flow

```
POST /admin/dlq/{eventId}/replay
    ↓
AdminAuthFilter validates X-Admin-Key
    ↓
eventRepository.GetByIdAsync(eventId)
    ↓
guard: status == FAILED_TERMINAL (else 409)
    ↓
TryTransitionStatusAsync(eventId, FAILED_TERMINAL, QUEUED)
    ↓ false → 409 Conflict (concurrent replay)
    ↓ true → this request owns the replay
dlqRepository.MarkReplayedAsync(dlqEntryId)
    ↓
eventPublisher.PublishAsync(envelope)   [main stream: events:ingress]
    ↓
AdminMetrics.ReplayTotal.Add(1)
    ↓
200 OK { eventId, status: "QUEUED" }
```

### Metrics Exposition Flow

```
System.Diagnostics.Metrics (Meter/Counter/Histogram)
    ↓  bridged by
OpenTelemetry.Exporter.Prometheus.AspNetCore (MeterProvider)
    ↓
GET /metrics → Prometheus text format
    ↓
Prometheus scrapes every 15s
    ↓
Grafana reads from Prometheus data source → alert rules fire
```

---

## Build Order

| Phase | What | Why first |
|-------|------|-----------|
| 1 | `dlq_events` migration + `IDlqRepository` + `DlqRepository` | Foundation — everything else depends on this table existing |
| 2 | `IDlqRouter` + `DlqRouter` + Worker wiring + `FAILED_TERMINAL→QUEUED` EventLifecycle transition | Completes DLQ write path before exposing it via API |
| 3 | OTel Prometheus setup + extend `IngestionMetrics` + `WorkerMetrics` + `/metrics` endpoint | No schema changes, no new HTTP admin surface — safe to ship independently |
| 4 | Deep health checks (`OutboxLagHealthCheck`, `RedisHealthCheck`) | Depends on Phase 3 (Worker has HTTP host); pure read-only |
| 5 | Admin API endpoints + `AdminAuthFilter` | Depends on Phase 1 (DLQ table) and Phase 2 (DLQ entries to query) |
| 6 | Grafana alerting rules (YAML) | Pure config; depends on metrics emitting (Phase 3) |

---

## Anti-Patterns to Avoid

### DLQ Domain Bleeding
**Don't** add `DlqEventId` or `MarkDlqRouted()` to `EventEnvelope`. DLQ routing is an infrastructure concern. The domain already records `FAILED_TERMINAL` — that is the domain truth. Where the event ends up operationally is Infrastructure's responsibility.

### Replay Through HTTP Ingestion (`POST /events`)
**Don't** replay by posting the original payload to `/events`. It would fail the idempotency check (same key exists) and lose the original EventId. Admin API must call `TryTransitionStatusAsync` + `IEventPublisher.PublishAsync` directly with the rehydrated envelope.

### Metrics at the Domain Layer
**Don't** record metrics inside `EventEnvelope` methods. Domain objects must not depend on observability infrastructure. Record at the call-site in Worker/DlqRouter/Admin handler after domain operations complete.

### Global Admin Auth Middleware
**Don't** use `app.UseMiddleware<AdminAuthMiddleware>()` globally — it would apply to `POST /events` and `/health`. Use `MapGroup("/admin").AddEndpointFilter<AdminAuthFilter>()` to scope auth to admin routes only.

### Bypassing TryTransitionStatusAsync on Replay
**Don't** call `UpdateStatusAsync(eventId, QUEUED)` unconditionally. Two concurrent replays would both succeed, causing duplicate Redis publishes. Use `TryTransitionStatusAsync` — the second caller gets `false` and returns 409 Conflict.

---

## New Interfaces (Application layer)

```csharp
// EventPlatform.Application/Abstractions/IDlqRouter.cs
public interface IDlqRouter
{
    Task RouteAsync(Guid eventId, string lastError, CancellationToken ct = default);
}

// EventPlatform.Application/Abstractions/IDlqRepository.cs
public interface IDlqRepository
{
    Task InsertAsync(DlqEntry entry, CancellationToken ct = default);
    Task<IReadOnlyList<DlqEntry>> ListAsync(string? tenantId, string? eventType, int page, int pageSize, CancellationToken ct = default);
    Task<DlqEntry?> GetByEventIdAsync(Guid eventId, CancellationToken ct = default);
    Task MarkReplayedAsync(Guid dlqEntryId, CancellationToken ct = default);
}

// EventPlatform.Application/Abstractions/IAdminAuthPolicy.cs
public interface IAdminAuthPolicy
{
    bool IsAuthorized(HttpContext context);
}
```

## New `dlq_events` Table Schema

```sql
CREATE TABLE dlq_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id        UUID NOT NULL REFERENCES events(id),
    tenant_id       TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    payload         JSONB NOT NULL,
    failure_reason  TEXT NOT NULL,
    attempt_count   INT NOT NULL,
    correlation_id  TEXT,
    dead_lettered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    replayed_at     TIMESTAMPTZ,
    replayed_by     TEXT
);

CREATE INDEX ix_dlq_events_tenant_type ON dlq_events (tenant_id, event_type);
CREATE INDEX ix_dlq_events_replayed ON dlq_events (replayed_at) WHERE replayed_at IS NULL;
```
