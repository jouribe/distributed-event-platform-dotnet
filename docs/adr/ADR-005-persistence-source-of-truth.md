# ADR-005: PostgreSQL as Source of Truth

## Status
Accepted

## Context
Redis is an in-memory system and should not be treated as the system of record.
We need reliable status tracking, retries scheduling, and auditing.

## Decision
PostgreSQL is the source of truth for:
- event envelope storage
- status transitions
- attempts + errors
- retry scheduling (next_attempt_at)

Redis is used only as a transport/broker.

## Consequences
- The worker can fetch payload from Postgres by event_id when needed.
- Redis messages stay small and stable.
