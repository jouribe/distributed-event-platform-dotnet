# Repository Guidelines

## Project Structure & Module Organization
- `src/` contains runtime services and core layers:
  - `EventIngestion.Api` (HTTP ingestion endpoints)
  - `EventWorker` (Redis stream consumers/background processing)
  - `EventPlatform.Domain`, `EventPlatform.Application`, `EventPlatform.Infrastructure` (Clean Architecture layers)
  - `EventPlatform.DbMigrator` (schema migration runner)
- `tests/` is split by scope:
  - `UnitTests/` for isolated domain/application/worker logic
  - `IntegrationTests/` for API + Redis/PostgreSQL/Testcontainers flows
- `migrations/` stores SQL migration artifacts; `deployments/` stores Docker Compose and deployment assets; `docs/` contains ADRs and architecture notes.

## Build, Test, and Development Commands
- `dotnet restore` - restore solution dependencies.
- `dotnet build --configuration Release` - compile all projects.
- `dotnet test --configuration Release` - run unit + integration tests with coverage gate.
- `docker compose -f deployments/docker-compose.yml up -d` - start PostgreSQL and Redis locally.
- `dotnet run --project src/EventPlatform.DbMigrator` - apply migrations (set `EVENTPLATFORM_DB` first).
- `dotnet run --project src/EventIngestion.Api` and `dotnet run --project src/EventWorker` - start API and worker.

## Coding Style & Naming Conventions
- Follow C# defaults: 4-space indentation, file-scoped namespaces where practical, nullable reference types enabled.
- Keep boundaries strict: Domain/Application should not depend on transport or hosting concerns.
- Use descriptive PascalCase for types/methods and camelCase for locals/parameters.
- Name tests by behavior, ending with `Tests` (example: `WorkerRetryTests`).

## Testing Guidelines
- Test stack: xUnit + Moq for unit tests; Testcontainers for integration tests.
- Coverage is enforced via `Directory.Build.props`: line coverage threshold is **70% total**.
- Add/adjust tests for any behavior change, especially around idempotency, retries, and delivery semantics.

## Commit & Pull Request Guidelines
- Use Conventional Commits (`feat`, `fix`, `docs`, `test`, `ci`, etc.).
  - Example: `feat(worker): implement retry backoff`
- PR titles must also follow Conventional Commit format (validated in CI).
- PRs should include: concise summary, change type, testing notes, linked issue/ADR when architectural, and checklist confirmation for tests/docs/boundaries.
