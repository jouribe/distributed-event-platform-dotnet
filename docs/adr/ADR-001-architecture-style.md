# ADR-001: Architecture Style

## Status
Accepted

## Context
The platform must support independent evolution of ingestion and worker concerns while preserving clear boundaries.

## Decision
Use Clean Architecture combined with Vertical Slice organization.

## Consequences

- Clear separation of domain/application/infrastructure concerns.
- Features can evolve with low coupling.
- Later modular extraction is simpler.
