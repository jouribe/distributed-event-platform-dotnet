# Distributed Event Platform (.NET 10)

![.NET](https://img.shields.io/badge/.NET-10.0-blueviolet)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue)
![Redis](https://img.shields.io/badge/Redis-Streams-red)
![Architecture](https://img.shields.io/badge/Architecture-Clean%20Architecture-black)
![License](https://img.shields.io/badge/license-MIT-green)

A production-oriented distributed event processing platform built with
**.NET 10**, **Redis Streams**, and **PostgreSQL**.

## Current Release Scope

This release covers:
- Idempotent ingestion
- Async background processing
- Retry with exponential backoff
- Event lifecycle tracking
- Correlation and traceability fields
- Baseline observability signals

## Architecture Overview

- **EventIngestion.Api**
  - Receives events via HTTP
  - Enforces idempotency
  - Persists event envelope and outbox row atomically in PostgreSQL
- **OutboxPublisherService** (hosted inside `EventIngestion.Api`)
  - Polls unpublished outbox rows
  - Publishes to Redis Streams and transitions event from `RECEIVED → QUEUED`
- **EventWorker**
  - Consumes from Redis Streams via Consumer Groups (`XREADGROUP`)
  - Reclaims stale PEL messages via `XAUTOCLAIM` (with `XPENDING`+`XCLAIM` fallback for older Redis)
  - Processes events asynchronously
  - Applies retry policies
  - Updates event lifecycle state
  - Acknowledges messages after durable status transition
- **PostgreSQL**
  - Source of truth
  - Stores event envelope + status transitions
- **Redis Streams**
  - Transport layer
  - At-least-once delivery

## Event Lifecycle

RECEIVED
-> QUEUED
-> PROCESSING
-> SUCCEEDED

On retryable failure:

PROCESSING
-> FAILED_RETRYABLE
-> QUEUED (after delay)

On terminal failure:

PROCESSING
-> FAILED_TERMINAL

## Known Limitations

- DLQ workflow is not implemented yet.
- Manual intervention is required for `FAILED_TERMINAL` events.

## Out of Scope (This Release)

- Complete operational runbooks.
- Post-release reliability hardening and platform improvements.

## Local Development

### 1. Start infrastructure

```bash
docker compose -f deployments/docker-compose.yml up -d
```

### 2. Apply database migrations

PowerShell:

```powershell
$env:EVENTPLATFORM_DB="Host=localhost;Port=54320;Database=event_platform;Username=event_platform;Password=event_platform"
dotnet run --project src/EventPlatform.DbMigrator
```

### 3. Run API

```bash
dotnet run --project src/EventIngestion.Api
```

### 4. Run Worker

```bash
dotnet run --project src/EventWorker
```

## Tech Stack

- .NET 10
- ASP.NET Core (Minimal APIs)
- Redis Streams
- PostgreSQL
- Dapper
- BackgroundService
- Clean Architecture

## Documentation

See `/docs` folder for:
- Architecture decisions (ADR)
- Event contract
- Processing semantics
- Retry strategy
- Failure scenarios
- Observability design

## Testing

- Unit tests
- Integration tests with Testcontainers
- End-to-end ingestion -> processing flow

## Roadmap

- Dead Letter Queue (DLQ)
- Expanded metrics and dashboards
- Full operational runbooks

## License

MIT
