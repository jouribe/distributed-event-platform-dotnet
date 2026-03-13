# Phase 1: DLQ Schema and Persistence - Context

**Gathered:** 2026-03-12
**Status:** Ready for planning

<domain>
## Phase Boundary

Establish the `dlq_events` PostgreSQL table, `DlqEvent` domain model, `IDlqRepository` interface, and `DlqRepository` implementation. This phase delivers the persistence layer that all subsequent DLQ work (Phase 2 routing, Phase 4 gauge metrics, Phase 6 Admin API) depends on. It does NOT wire the Worker to call any new methods — that is Phase 2.

</domain>

<decisions>
## Implementation Decisions

### Atomic write ownership
- `EventRepository` owns the FAILED_TERMINAL + dlq_events transaction
- New method: `MarkAsTerminalFailedWithDlqAsync(Guid eventId, DlqEvent dlqEvent, CancellationToken ct)`
- Pattern: follows `InsertWithOutboxAsync` exactly — single `CreateConnection()`, explicit `BeginTransaction()`, two INSERTs inside, `CommitAsync()` on success, rolled back on failure
- `IDlqRepository` does NOT own the atomic transaction; its `InsertAsync` is called from within the EventRepository transaction via a shared connection/transaction parameter

### replayed_by semantics
- `replayed_by` column is a nullable string
- In Phase 6 (Admin API), the replay endpoint sets it to `"system:admin_api"` for v1 traceability
- The column is reserved for future JWT/OAuth2 upgrade (would store caller identity)
- Phase 1 just defines the column as `text NULL` — no default value at the DB level

### DlqEvent model placement
- `DlqEvent` lives in `EventPlatform.Domain` alongside `OutboxEvent` — consistent precedent
- It is a sealed record (immutable value object), not an aggregate with lifecycle transitions
- Phase 1: no state machine on DlqEvent; it is a snapshot of failure metadata at the moment of terminal failure

### Pagination style for ListAsync
- Offset-based (skip + pageSize) — consistent with `RetryableEventsPage` pattern already established
- Return type: dedicated `DlqEventsPage` record with `Items`, `TotalCount`, `Skip`, `PageSize` fields

### IDlqRepository placement
- Lives in `EventPlatform.Infrastructure` (alongside `IEventRepository`, `IOutboxRepository`)
- Application layer (`EventPlatform.Application`) remains interfaces-only for external ports (`IEventPublisher`) — IDlqRepository is an infrastructure-internal contract

### GitHub issues
- No dedicated Phase 1 issue — it is foundational infrastructure
- Issue #67 (Phase 2) defines the DLQ message metadata; `DlqEvent` in Phase 1 must carry: `event_id`, `tenant_id`, `event_type`, `payload` (JSONB), `failure_reason`, `attempt_count`, `correlation_id`, `dead_lettered_at`, `replayed_at` (nullable), `replayed_by` (nullable)

</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- `IDbConnectionFactory` / `DbConnectionFactory`: inject to create Npgsql connections — same pattern for `DlqRepository`
- `InsertWithOutboxAsync` (EventRepository): exact transaction pattern to replicate for `MarkAsTerminalFailedWithDlqAsync`
- `RetryableEventsPage`: offset pagination model to mirror as `DlqEventsPage`
- `EventRepositoryException`: existing domain exception — reuse or create `DlqRepositoryException` following same shape
- `EventQueries.cs` / `OutboxQueries.cs` in `Persistence/Internal/`: new `DlqQueries.cs` should live there

### Established Patterns
- All SQL in static internal query classes (not inline in repository methods)
- Dapper `CommandDefinition` with `commandTimeout: 30` and `cancellationToken` passed explicitly
- `ArgumentNullException` guard at top of each public method
- `cancellationToken.ThrowIfCancellationRequested()` immediately after null checks
- Connection opened synchronously (`connection.Open()`), not `OpenAsync` — follow same convention

### Integration Points
- `migrations/postgres/` — next migration file is `009_create_dlq_events_table.sql` (sequential numbering)
- `ServiceCollectionExtensions.cs` — register `DlqRepository` as `IDlqRepository` (singleton or scoped, matching existing registrations)
- `EventRepository.MarkTerminalFailureAsync` — Phase 1 adds `MarkAsTerminalFailedWithDlqAsync` alongside it (does NOT replace or modify the existing method — Worker still calls `MarkTerminalFailureAsync` until Phase 2)

</code_context>

<specifics>
## Specific Ideas

- Method name is `MarkAsTerminalFailedWithDlqAsync` (user's exact phrasing, not `MarkTerminalWithDlqAsync`)
- `replayed_by` default for Admin API calls: `"system:admin_api"` — literal string value
- DlqEvent constructor should be a static factory method `DlqEvent.CreateFromTerminalFailure(...)` consistent with `EventEnvelope.CreateNew(...)` convention
- Review issue #67 before Phase 2 planning to confirm DLQ Redis message fields align with `DlqEvent` columns

</specifics>

<deferred>
## Deferred Ideas

- None — discussion stayed within phase scope

</deferred>

---

*Phase: 01-dlq-schema-and-persistence*
*Context gathered: 2026-03-12*
