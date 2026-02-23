# Unit Tests

Unit tests for the Event Platform domain and application layers.

## Structure

Tests are organized by the domain they target, with shared fixtures for test data builders:

```
UnitTests/
├── Domain/
│   └── Events/
│       ├── EventEnvelopeTests.cs      # Tests for EventEnvelope aggregate
│       └── EventLifecycleTests.cs     # Tests for EventLifecycle state machine
├── Fixtures/
│   └── EventEnvelopeBuilder.cs        # Test fixture builder for EventEnvelope
└── EventPlatform.UnitTests.csproj
```

## Running Tests

From the workspace root:

```bash
dotnet test tests/UnitTests/EventPlatform.UnitTests.csproj
```

Or with coverage:

```bash
dotnet test tests/UnitTests/EventPlatform.UnitTests.csproj /p:CollectCoverage=true
```

## Adding New Tests

When adding tests for a new component/service, create a subdirectory matching the source structure:

- Tests for `src/EventPlatform.Application/...` → `tests/UnitTests/Application/...`
- Tests for `src/EventPlatform.Infrastructure/...` → `tests/UnitTests/Infrastructure/...`
- Tests for `src/EventIngestion.Api/...` → `tests/UnitTests/Api/...`

Update the namespace to reflect the directory path: `EventPlatform.UnitTests.{Layer}.{Component}`.

## Test Fixtures

Reusable test data builders and helpers are located in `Fixtures/`:

- **EventEnvelopeBuilder**: Fluent builder for constructing test EventEnvelope instances with customizable properties.

Example:
```csharp
var envelope = new EventEnvelopeBuilder()
    .WithEventType("UserCreated")
    .WithTenantId("tenant-xyz")
    .Build();

var queued = new EventEnvelopeBuilder().BuildQueued();
```
