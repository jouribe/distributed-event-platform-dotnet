## Summary

- **What**: Describe the changes.
- **Why**: Explain the business or technical motivation (reference ADRs if applicable).

## Type of Change

- [ ] `feat`: A new feature
- [ ] `fix`: A bug fix
- [ ] `docs`: Documentation only changes
- [ ] `refactor`: A code change that neither fixes a bug nor adds a feature
- [ ] `test`: Adding missing tests or correcting existing tests
- [ ] `chore`: Changes to the build process or auxiliary tools

## Related Issue

Closes #<issue-number>

## Architectural Integrity

- [ ] Domain Invariants: Are `EventEnvelope` state transitions respected via `EventLifecycle`?
- [ ] Boundary Rules: Does the `Domain/Application` layer remain free of Infrastructure dependencies?
- [ ] Outbox Pattern: If ingesting events, is `InsertWithOutboxAsync` used to ensure atomicity?
- [ ] Idempotency: Do consumers in `EventWorker` handle redelivery safely?

## Testing & Coverage

- [ ] Unit Tests: Coverage added for core logic in `Application` or `Domain`.
- [ ] Integration Tests: Verified via `Testcontainers` (PostgreSQL/Redis) for real infrastructure flow.
- [ ] Coverage Gate: Total line coverage is >= 70% (verified via `Directory.Build.props`).

## Database & Migrations

- [ ] New migrations follow the **Expand and Contract** pattern (no destructive changes).
- [ ] `DbMigrator` has been executed and verified locally.

## Checklist

- [ ] PR title follows **Conventional Commits** format.
- [ ] Linked to an issue (Closes/Fixes #X).
- [ ] No hardcoded secrets or connection strings.
- [ ] Documentation/ADRs updated if architecture was modified.
