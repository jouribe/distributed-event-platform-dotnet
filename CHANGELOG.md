# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog.
This project adheres to Semantic Versioning.

---

## [Unreleased]

### Added
- Initial distributed architecture
- Redis Streams publisher
- PostgreSQL event persistence
- Idempotent ingestion endpoint
- Async worker processing
- Retry mechanism with exponential backoff
- Persist and rehydrate `source` field for improved traceability (#36)

### Changed
- Documentation updated to reflect the current release scope:
  - In scope: ingestion, async processing, idempotency, retry, event tracking, and baseline observability.
  - Known limitations: no mandatory outbox flow, no DLQ flow, and no PEL reclaim (`XAUTOCLAIM`) yet.
  - Out of scope: full operational runbooks and next-phase reliability improvements.

---

## [0.1.0] - 2025-02-22

### Added
- Clean Architecture scaffolding
- Event envelope model
- Event status state machine
- Docker compose for Postgres and Redis
- Initial documentation and ADRs
- Repository governance files
