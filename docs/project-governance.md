# Definition of Done (DoD)

An issue is considered DONE only if all the following conditions are met:

## Code Quality
- Code compiles without warnings.
- No unused dependencies or dead code.
- Follows established architecture boundaries.

## Architecture
- Domain logic stays in Domain layer.
- Infrastructure concerns stay in Infrastructure.
- No cross-layer violations.

## Functionality
- Acceptance criteria fully satisfied.
- No partial implementation.

## Testing
- Unit tests added (if applicable).
- Integration tests updated if behavior changes.
- CI pipeline passes.

## Observability
- Relevant logs include correlation_id and event_id.
- No silent failures.

## Documentation
- CHANGELOG updated if behavior changed.
- README updated if public behavior changed.

## Merge Conditions
- PR reviewed (even if self-reviewed with checklist).
- All checks green.
- Squash merge preferred.
