# Purpose

This project implements a distributed event processing platform using:

- .NET 10
- Redis Streams
- PostgreSQL

## What this project demonstrates

The platform demonstrates production-grade distributed systems patterns:

- Idempotent ingestion
- At-least-once processing
- Async event handling
- Retry with backoff
- Event state tracking
- Structured logging
- Correlation IDs

## Scope

- Reliable intake of events from HTTP API
- Event transport with Redis Streams
- Durable event lifecycle in PostgreSQL
- Worker processing with retry and deterministic status transitions

## Out of scope

- Business-specific domain logic
- Exactly-once delivery guarantees
- Multi-region replication concerns
- Full operational runbooks (planned)

## Product principle

This is infrastructure and reference architecture, not a business application.
