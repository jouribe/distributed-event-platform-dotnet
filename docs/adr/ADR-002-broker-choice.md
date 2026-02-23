# ADR-002: Broker Choice (Redis Streams)

## Status
Accepted

## Context
We need a lightweight message broker for async processing with:
- consumer groups
- acknowledgements
- pending messages tracking
- redelivery support

## Decision
Use Redis Streams with Consumer Groups.

Conventions:
- Stream: `events:stream`
- Consumer Group: `events:workers`
- Consumer Name: `{machine}-{pid}`

## Consequences
- Delivery is at-least-once.
- Workers must implement idempotent handlers.
- A later phase should implement pending reclaim (PEL + XCLAIM) for crashed consumers.
