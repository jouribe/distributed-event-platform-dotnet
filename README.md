# Distributed Event Platform (.NET 10)

![.NET](https://img.shields.io/badge/.NET-10.0-blueviolet)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue)
![Redis](https://img.shields.io/badge/Redis-Streams-red)
![Architecture](https://img.shields.io/badge/Architecture-Clean%20Architecture-black)
![License](https://img.shields.io/badge/license-MIT-green)

A production-oriented distributed event processing platform built with
**.NET 10**, **Redis Streams**, and **PostgreSQL**.

This project demonstrates real-world distributed systems concepts
including:

-   Idempotent ingestion
-   At-least-once delivery semantics
-   Async background processing
-   Retry with exponential backoff
-   Event state tracking
-   Correlation ID propagation
-   Clean Architecture boundaries

------------------------------------------------------------------------

## 🧠 Why This Project?

Modern systems must handle:

-   Network failures
-   Duplicate requests
-   Process crashes
-   Message redelivery
-   Eventual consistency

This platform is designed to model those realities and implement safe,
resilient processing patterns.

It is not a CRUD app.

It is infrastructure.

------------------------------------------------------------------------

## 🏗 Architecture Overview

-   **EventIngestion.Api**
    -   Receives events via HTTP
    -   Enforces idempotency
    -   Persists event state in PostgreSQL
    -   Publishes to Redis Streams
-   **EventWorker**
    -   Consumes from Redis Streams (Consumer Groups)
    -   Processes events asynchronously
    -   Applies retry policies
    -   Updates event lifecycle state
    -   Acknowledges messages safely
-   **PostgreSQL**
    -   Source of truth
    -   Stores event envelope + status transitions
-   **Redis Streams**
    -   Transport layer
    -   At-least-once delivery

------------------------------------------------------------------------

## 🔄 Event Lifecycle

RECEIVED\
→ QUEUED\
→ PROCESSING\
→ SUCCEEDED

On failure:

PROCESSING\
→ FAILED_RETRYABLE\
→ QUEUED (after delay)

or

→ FAILED_TERMINAL

------------------------------------------------------------------------

## 🚀 Local Development

### 1️⃣ Start infrastructure

docker compose -f deployments/docker-compose.yml up -d

### 2️⃣ Run API

dotnet run --project src/EventIngestion.Api

### 3️⃣ Run Worker

dotnet run --project src/EventWorker

------------------------------------------------------------------------

## 📦 Tech Stack

-   .NET 10
-   ASP.NET Core (Minimal APIs)
-   Redis Streams
-   PostgreSQL
-   Dapper
-   BackgroundService
-   Clean Architecture

------------------------------------------------------------------------

## 📚 Documentation

See `/docs` folder for:

-   Architecture decisions (ADR)
-   Event contract
-   Processing semantics
-   Retry strategy
-   Failure scenarios
-   Observability design

------------------------------------------------------------------------

## 🧪 Testing

-   Unit tests
-   Integration tests with Testcontainers
-   End-to-end ingestion → processing flow

------------------------------------------------------------------------

## 🔮 Roadmap

-   Dead Letter Queue (DLQ)
-   Outbox pattern
-   Metrics (Prometheus)
-   Dashboard (real-time monitoring)
-   Modular extraction as reusable packages

------------------------------------------------------------------------

## 📄 License

MIT
