# Pitfalls Research: DLQ + Observability for .NET 10 Event Platform

## Domain
DLQ routing, Admin API replay, Prometheus metrics, and health checks added to an existing .NET 10 / Redis Streams / PostgreSQL event processor.

---

## Pitfall 1 — Non-Atomic DLQ Routing (Event Loss Window)

**Risk level:** CRITICAL

**What goes wrong:** If `MarkTerminalFailureAsync` (UPDATE events) and `IDlqRepository.InsertAsync` (INSERT dlq_events) are separate sequential calls, a process crash between them leaves orphaned `FAILED_TERMINAL` rows with no corresponding DLQ record. The event is "dead" but invisible to operators.

**Warning signs:**
- `dlq_events` row count < `events WHERE status = 'FAILED_TERMINAL'` count
- Events stuck at `FAILED_TERMINAL` with no DLQ entry
- DLQ list API returns fewer events than expected

**Prevention:** Combine into a single transaction following the established `InsertWithOutboxAsync` pattern. Introduce `MarkTerminalAndWriteDlqAsync(eventId, dlqEntry, connection, transaction)` in `IEventRepository` that atomically UPDATEs the events row and INSERTs the dlq_events row in one database transaction.

**Phase:** Phase 1 (schema + DLQ persistence) — design this from the start, not as a retrofit.

---

## Pitfall 2 — Replay Bypassing Domain Transition

**Risk level:** CRITICAL

**What goes wrong:** `FAILED_TERMINAL → QUEUED` is not currently a valid edge in `EventLifecycle.CanTransition`. A developer issues a raw SQL UPDATE (`UPDATE events SET status = 'QUEUED' WHERE id = @id`) bypassing `EnsureTransition()`. The domain invariant is violated silently — the state machine is now broken for this event.

**Warning signs:**
- No new unit test for the `FAILED_TERMINAL → QUEUED` transition
- `TryTransitionStatusAsync` used without first adding the edge to `EventLifecycle`
- Admin replay works in integration tests but domain rules allow invalid transitions

**Prevention:**
1. Add a new `ReplayFromDlq()` method to `EventEnvelope` that calls `EventLifecycle.EnsureTransition(FAILED_TERMINAL, QUEUED)` — this registers the edge
2. Write state machine unit tests for this transition *before* implementing replay
3. `TryTransitionStatusAsync` must validate through the domain, not bypass it

**Phase:** Phase 2 (DLQ routing + lifecycle) — lifecycle change must be tested before any replay code is written.

---

## Pitfall 3 — Redis Publish Before PostgreSQL Commit on Replay

**Risk level:** HIGH

**What goes wrong:** The Admin replay endpoint publishes to the main Redis stream before committing the `FAILED_TERMINAL → QUEUED` status update. If the DB commit fails after the Redis publish, the Worker picks up and processes the event while PostgreSQL still shows `FAILED_TERMINAL`. The event is double-counted in metrics and the lifecycle is corrupted.

**Warning signs:**
- Replay returns 500 but event appears in Redis stream
- Event processed by Worker shows `FAILED_TERMINAL` status in DB
- Inconsistent metric counts between DB state and processing counters

**Prevention:** Follow the DB-first pattern from `RetrySchedulerService`:
1. `TryTransitionStatusAsync(eventId, FAILED_TERMINAL, QUEUED)` — commit to PostgreSQL first
2. `IDlqRepository.MarkReplayedAsync(dlqEntryId)` — update DLQ record
3. `IEventPublisher.PublishAsync(envelope)` — publish to Redis last

If Redis publish fails after the DB commit, the event is in `QUEUED` status. The `OutboxPublisherService` or retry mechanism will recover it on the next cycle.

**Phase:** Phase 5 (Admin API) — enforce DB-first ordering in the replay handler from the start.

---

## Pitfall 4 — Prometheus Cardinality Explosion

**Risk level:** HIGH

**What goes wrong:** A developer uses `tenant_id`, `event_id`, `correlation_id`, or raw error message strings as metric label values. With thousands of tenants and millions of events, this creates millions of unique time series, overwhelming Prometheus memory and making dashboards unusable.

**Warning signs:**
- Metric labels include `event_id` (UUID — unbounded)
- Metric labels include `correlation_id` (UUID — unbounded)
- Error messages used as label values (`failure_reason` raw text)
- Label values not validated against a bounded set

**Prevention:** Only use **bounded** values as `TagList` entries:
- ✓ `event_type` — bounded by `AllowedEventTypes` config
- ✓ `error_category` — enum: `transient` / `validation` / `invariant` / `unknown`
- ✓ `stream` — `events:ingress` / `events:dlq`
- ✗ `tenant_id` — can be unbounded; exclude or limit to known top tenants
- ✗ `event_id` — UUID, never use as label
- ✗ `correlation_id` — UUID, never use as label

**Phase:** Phase 3 (Prometheus metrics) — establish label conventions before any metric is defined.

---

## Pitfall 5 — Health Check False Positives from Outbox Lag

**Risk level:** MEDIUM

**What goes wrong:** The `/health/ready` endpoint queries `SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL`. If the OutboxPublisherService is momentarily behind (common under load), the health check returns `Unhealthy`, causing Kubernetes to restart healthy pods. This creates a cascade failure.

**Warning signs:**
- Health check flaps under moderate load
- Pod restarts correlated with outbox processing spikes
- Liveness probe failing when system is otherwise functional

**Prevention:**
- Use **configurable thresholds** with hysteresis: return `Degraded` for lag > 30s, `Unhealthy` only for lag > 5 minutes (not zero)
- Expose outbox lag as a **Prometheus gauge** (`outbox_pending_total`) for alerting in Grafana — where operators can tune thresholds
- Health check should reflect: "is this service able to accept requests?" not "is the outbox perfectly empty?"
- Tag the outbox lag check as `["ready"]` not `["live"]` — readiness, not liveness

**Phase:** Phase 4 (deep health checks) — set thresholds during design, not after first false positive.

---

## Pitfall 6 — Admin Auth Middleware Tight Coupling

**Risk level:** MEDIUM

**What goes wrong:** The static key check is inlined directly in `AdminAuthFilter.InvokeAsync` with no interface abstraction. When JWT is needed, the entire middleware class must be rewritten and the DI registration refactored across multiple files.

**Warning signs:**
- `IConfiguration` injected directly into the filter with `GetValue<string>("AdminApi:Key")`
- No `IAdminAuthPolicy` interface — the policy is the implementation
- JWT upgrade requires modifying `AdminAuthFilter` internals

**Prevention:**
```csharp
public interface IAdminAuthPolicy
{
    bool IsAuthorized(HttpContext context);
}

public class StaticKeyAuthPolicy(IOptions<AdminAuthOptions> opts) : IAdminAuthPolicy
{
    public bool IsAuthorized(HttpContext ctx) =>
        ctx.Request.Headers.TryGetValue("X-Admin-Key", out var key) && key == opts.Value.ApiKey;
}

// JWT upgrade: register JwtAuthPolicy instead — zero changes to AdminAuthFilter
```

**Phase:** Phase 5 (Admin API) — design the interface before implementing the static key policy.

---

## Pitfall 7 — Replay Reusing Exhausted Attempt Count

**Risk level:** HIGH

**What goes wrong:** An event replayed from DLQ has `Attempts = 5` (the MaxAttempts threshold). The Worker picks it up, begins processing, encounters any error, calls `IncrementAttemptsAsync`, raising it to 6. The retry check (`attempts >= MaxAttempts`) is immediately true — the event is terminal-failed again and routed back to DLQ. The operator's replay produces an instant re-dead-letter.

**Warning signs:**
- Replayed events immediately re-appear in DLQ
- Worker logs show FAILED_TERMINAL immediately after replay
- Attempts count on `dlq_events` entry is already at MaxAttempts

**Prevention:** `ReplayFromDlq()` domain method must reset `Attempts = 0`. The `TryTransitionStatusAsync` SQL for replay must include `attempts = 0` in the UPDATE:
```sql
UPDATE events
SET status = 'QUEUED', attempts = 0, updated_at = now()
WHERE id = @eventId AND status = 'FAILED_TERMINAL'
```

This gives the replayed event a clean retry budget.

**Phase:** Phase 5 (Admin API replay) — the attempts reset must be in the transition SQL from day one.

---

## Pitfall 8 — DLQ Redis Stream Growing Without Bound

**Risk level:** MEDIUM

**What goes wrong:** `StreamAddAsync("events:dlq", ...)` is called without specifying `maxLength`. The DLQ stream accumulates every terminal failure forever. On a long-running system this consumes unbounded Redis memory and slows `XLEN` operations.

**Warning signs:**
- `XLEN events:dlq` growing unbounded in Redis
- Redis memory increasing monotonically
- No `MAXLEN` clause in `StreamAddAsync` calls

**Prevention:** Use approximate trimming on every DLQ write:
```csharp
await database.StreamAddAsync(
    key: "events:dlq",
    streamPairs: fields,
    maxLength: 10_000,        // approximate trim: ~ keeps last 10k entries
    useApproximateMaxLength: true);
```

Since `dlq_events` in PostgreSQL is the durable source of truth, Redis stream trimming is safe — operators query DLQ state from PostgreSQL, not from the stream. The stream is only for routing/consumption, not for archival.

**Phase:** Phase 2 (DLQ routing in Worker) — add `maxLength` to the initial `StreamAddAsync` call.
