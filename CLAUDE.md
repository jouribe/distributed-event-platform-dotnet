# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Infrastructure (required before running anything)
```bash
docker compose -f deployments/docker-compose.yml up -d
```
PostgreSQL maps to `localhost:54320`; Redis maps to `localhost:63790`.

### Database migrations
```bash
# PowerShell
$env:EVENTPLATFORM_DB="Host=localhost;Port=54320;Database=event_platform;Username=event_platform;Password=event_platform"
dotnet run --project src/EventPlatform.DbMigrator

# Bash
EVENTPLATFORM_DB="Host=localhost;Port=54320;Database=event_platform;Username=event_platform;Password=event_platform" dotnet run --project src/EventPlatform.DbMigrator
```

### Run services
```bash
dotnet run --project src/EventIngestion.Api
dotnet run --project src/EventWorker
```

### Tests
```bash
# Unit tests (no infrastructure needed)
dotnet test tests/UnitTests/EventPlatform.UnitTests.csproj

# Integration tests (Testcontainers spins up PostgreSQL 16-alpine + Redis 7-alpine automatically)
dotnet test tests/IntegrationTests/EventPlatform.IntegrationTests.csproj

# Single test
dotnet test tests/UnitTests/EventPlatform.UnitTests.csproj --filter "FullyQualifiedName~EventEnvelopeTests"

# With coverage (70% line coverage gate enforced via Directory.Build.props)
dotnet test tests/UnitTests/EventPlatform.UnitTests.csproj /p:CollectCoverage=true
```

## Architecture

This is a **distributed event processing platform** following Clean Architecture. The flow is:

```
HTTP POST /events → EventIngestion.Api → PostgreSQL (events + outbox) → OutboxPublisherService → Redis Stream → EventWorker → PostgreSQL (status update)
```

### Project layout

| Project | Role |
|---|---|
| `EventPlatform.Domain` | `EventEnvelope` aggregate, `EventLifecycle` state machine, `EventStatus` enum |
| `EventPlatform.Application` | Interfaces only: `IEventPublisher`, `IEventRepository`, `IOutboxRepository` |
| `EventPlatform.Infrastructure` | Dapper/PostgreSQL repositories, Redis publisher, `OutboxPublisherService` background service |
| `EventIngestion.Api` | Single Minimal API endpoint `POST /events`, idempotency enforcement, validation |
| `EventWorker` | `BackgroundService` consuming Redis Stream via Consumer Groups |
| `EventPlatform.DbMigrator` | Runs sequential SQL migrations from `migrations/postgres/` |

### Domain: EventEnvelope

`EventEnvelope` is a sealed record with a private constructor. The only valid entry points are:
- `EventEnvelope.CreateNew(...)` — creates with status `RECEIVED`
- `EventEnvelope.RehydrateFromPersistence(...)` — used only by the repository layer

State transitions are enforced by `EventLifecycle.EnsureTransition()` and exposed via domain methods: `MarkQueued()`, `MarkProcessing()`, `MarkSucceeded()`, `MarkRetryableFailure()`, `RequeueAfterRetry()`, `MarkTerminalFailure()`.

Valid lifecycle: `RECEIVED → QUEUED → PROCESSING → SUCCEEDED`
Retry path: `PROCESSING → FAILED_RETRYABLE → QUEUED`
Terminal: `PROCESSING → FAILED_TERMINAL`

### Outbox pattern

Ingestion writes the event envelope and an `OutboxEvent` row atomically in a single transaction (`InsertWithOutboxAsync`). The `OutboxPublisherService` (hosted in the API process) polls the outbox table and publishes unpublished entries to Redis, then marks them as published. This guarantees events are not lost if Redis is temporarily unavailable at ingestion time.

### Worker: Redis Consumer Groups

`Worker.cs` uses XREADGROUP (`>`) for new messages and XAUTOCLAIM (with XPENDING+XCLAIM fallback for older Redis) for reclaiming stale pending messages. On startup it:
1. Ensures the consumer group exists via `RedisConsumerGroupBootstrapper`
2. Drains own pending messages (entries read but not yet ACKed)
3. Reclaims stale messages from crashed consumers

To add new event handling logic, implement `IWorkerEventHandler` and register it in `EventWorker/Program.cs` (replacing `NoopWorkerEventHandler`).

### Configuration

Both services read connection strings from environment variables first, then fall back to `appsettings.json`:
- `EVENTPLATFORM_DB` — PostgreSQL connection string
- `EVENTPLATFORM_REDIS` — Redis connection string (API only; worker uses `RedisConsumer:ConnectionString`)

The API also requires `Ingestion:RedisStreamName` and `Ingestion:AllowedEventTypes` in config.

## Testing conventions

- Unit test structure mirrors source: `src/EventIngestion.Api/...` → `tests/UnitTests/Api/...`
- Namespaces follow directory path: `EventPlatform.UnitTests.{Layer}.{Component}`
- Use `EventEnvelopeBuilder` (in `tests/UnitTests/Fixtures/`) to construct test domain objects:
  ```csharp
  var envelope = new EventEnvelopeBuilder().WithEventType("order.created").Build();
  var queued = new EventEnvelopeBuilder().BuildQueued();
  ```
- Integration tests use `CustomWebApplicationFactory` which spins up real containers and provides `ResetStateAsync()` to truncate tables between tests

## Commit & Pull Request Standard

* **Commits**: Use Conventional Commits (`feat:`, `fix:`, `docs:`, `test:`, `chore:`).
* **PR Creation**: When asked to draft a Pull Request:
   1. **Read** `.github/pull_request_template.md` first.
   2. **Follow** the template's structure exactly.
   3. **Map changes** to the specific sections: Summary, Type of Change, Testing Notes, and Architectural Boundaries.
   4. Ensure the PR title matches the Conventional Commit format.
`
