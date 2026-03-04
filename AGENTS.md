# Repository Guidelines: Distributed Event Platform

## System Context & Flow
High-throughput distributed event processing following **Clean Architecture** and **Event-Driven Architecture (EDA)**.
- **Data Flow**: `HTTP POST /events` → `Api` → `PostgreSQL (Outbox)` → `OutboxPublisherService` → `Redis Stream` → `EventWorker` → `PostgreSQL (Final Status)`.

## Project Layout & Roles
| Project | Responsibility |
|---|---|
| `EventPlatform.Domain` | Core logic: `EventEnvelope` (Aggregate), `EventLifecycle` (State Machine). |
| `EventPlatform.Application` | Use Cases & Interfaces: `IEventPublisher`, `IEventRepository`. |
| `EventPlatform.Infrastructure` | Dapper/PostgreSQL Repos, Redis Pub/Sub, `OutboxPublisherService`. |
| `EventIngestion.Api` | Minimal API, Idempotency, Validation, Outbox entry point. |
| `EventWorker` | Background service consuming Redis Streams via Consumer Groups. |
| `EventPlatform.DbMigrator` | Sequential SQL migration runner from `migrations/postgres/`. |

## Domain Invariants & Business Rules (CRITICAL)
- **EventEnvelope Construction**: It is a `sealed record` with a **private constructor**.
  - **DO NOT** use `new`. Use `EventEnvelope.CreateNew(...)` for new events.
  - **DO NOT** use for persistence unless inside the Repository layer via `RehydrateFromPersistence`.
- **State Machine**: Transitions are strictly enforced by `EventLifecycle.EnsureTransition()`.
  - Valid Path: `RECEIVED` → `QUEUED` → `PROCESSING` → `SUCCEEDED`.
  - Retry: `PROCESSING` → `FAILED_RETRYABLE` → `QUEUED`.
- **Outbox Pattern**: Ingestion must use `InsertWithOutboxAsync` to ensure atomicity between DB write and eventual Redis publishing.

## Infrastructure & Local Dev
- **Docker**: `docker compose -f deployments/docker-compose.yml up -d` (PostgreSQL: 54320, Redis: 63790).
- **Environment**: Use `EVENTPLATFORM_DB` for connection strings.
- **Worker Logic**: Uses `XREADGROUP` and `XAUTOCLAIM` for reliability. To add handlers, implement `IWorkerEventHandler`.

## Build & Test Commands
- **Restore/Build**: `dotnet restore` | `dotnet build --configuration Release`.
- **Unit Tests**: `dotnet test tests/UnitTests/EventPlatform.UnitTests.csproj`.
- **Integration Tests**: `dotnet test tests/IntegrationTests/EventPlatform.IntegrationTests.csproj` (Uses Testcontainers).
- **Coverage**: 70% line coverage enforced via `Directory.Build.props`.

## Testing Conventions
- **Fixtures**: Use `EventEnvelopeBuilder` in `tests/UnitTests/Fixtures/` for constructing test objects.
- **Integration**: Use `CustomWebApplicationFactory` for real container flow. Call `ResetStateAsync()` between tests.

## Coding Style & Patterns
- **Idiomatic C#**: 4-space indent, file-scoped namespaces, nullable reference types.
- **Architectural Boundaries**: Infrastructure -> Application -> Domain. Domain must have zero external dependencies.
- **Naming**: PascalCase for types/methods, camelCase for parameters/locals. Tests must end in `Tests`.

## DevOps & Workflow
- **Git**: Conventional Commits only (`feat:`, `fix:`, `docs:`, `test:`, `chore:`).
- **Pull Requests**:
  - **MUST** follow the template in `.github/pull_request_template.md`.
  - PR titles must be Conventional Commits.
  - Verification: Confirm no architectural leaks and that tests cover the new behavior.

## Troubleshooting
- **Redis**: Verify `StackExchange.Redis` config matches container names.
- **Migrations**: Ensure `EVENTPLATFORM_DB` is set before running `DbMigrator`.
- **Testcontainers**: Ensure Docker Engine is running if integration tests fail on startup.