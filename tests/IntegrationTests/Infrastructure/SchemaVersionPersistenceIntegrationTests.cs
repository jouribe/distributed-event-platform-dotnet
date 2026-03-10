using System.Text.Json;
using Dapper;
using EventPlatform.Domain.Events;
using EventPlatform.Infrastructure.Persistence.DataAccess;
using EventPlatform.Infrastructure.Persistence.Repositories;
using Npgsql;
using Testcontainers.PostgreSql;

namespace EventPlatform.IntegrationTests.Infrastructure;

/// <summary>
/// Integration tests that verify schema_version is correctly written and read back
/// through the full repository stack against a real PostgreSQL instance.
/// </summary>
public sealed class SchemaVersionPersistenceIntegrationTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgresContainer = new PostgreSqlBuilder("postgres:16-alpine")
        .WithDatabase("event_platform")
        .WithUsername("event_platform")
        .WithPassword("event_platform")
        .Build();

    private string _connectionString = string.Empty;

    public async Task InitializeAsync()
    {
        await _postgresContainer.StartAsync();
        _connectionString = _postgresContainer.GetConnectionString();
        await ApplyMigrationsAsync();
    }

    public async Task DisposeAsync()
    {
        await _postgresContainer.DisposeAsync();
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    [Fact]
    public async Task InsertAsync_ThenGetById_PreservesDefaultSchemaVersion()
    {
        var repository = CreateRepository();
        var envelope = BuildEnvelope(schemaVersion: 1);

        await repository.InsertAsync(envelope);

        var retrieved = await repository.GetByIdAsync(envelope.Id);

        Assert.NotNull(retrieved);
        Assert.Equal((short)1, retrieved.SchemaVersion);
    }

    [Fact]
    public async Task InsertAsync_ThenGetById_PreservesExplicitSchemaVersion()
    {
        var repository = CreateRepository();
        var envelope = BuildEnvelope(schemaVersion: 3);

        await repository.InsertAsync(envelope);

        var retrieved = await repository.GetByIdAsync(envelope.Id);

        Assert.NotNull(retrieved);
        Assert.Equal((short)3, retrieved.SchemaVersion);
    }

    [Fact]
    public async Task ExistingRow_WithoutSchemaVersionColumn_ReadsDefaultOne()
    {
        // Simulate a row that was written before migration 008 by inserting
        // directly without the schema_version column — the column DEFAULT 1 applies.
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();

        var id = Guid.NewGuid();
        await connection.ExecuteAsync(@"
            INSERT INTO event_platform.events (
                id, tenant_id, event_type, occurred_at, received_at,
                payload, idempotency_key, correlation_id, status, attempts, source
            ) VALUES (
                @Id, 'tenant-legacy', 'legacy.event', now(), now(),
                '{}', gen_random_uuid()::text, gen_random_uuid(), 'RECEIVED', 0, 'legacy-source'
            )", new { Id = id });

        var repository = CreateRepository();
        var retrieved = await repository.GetByIdAsync(id);

        Assert.NotNull(retrieved);
        Assert.Equal((short)1, retrieved.SchemaVersion);
    }

    [Fact]
    public async Task DatabaseConstraint_Rejects_SchemaVersionZero()
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();

        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await connection.ExecuteAsync(@"
                INSERT INTO event_platform.events (
                    id, tenant_id, event_type, occurred_at, received_at,
                    payload, idempotency_key, correlation_id, status, attempts, source, schema_version
                ) VALUES (
                    gen_random_uuid(), 'tenant-x', 'test.event', now(), now(),
                    '{}', gen_random_uuid()::text, gen_random_uuid(), 'RECEIVED', 0, 'test-source', 0
                )");
        });
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private IEventRepository CreateRepository()
    {
        var factory = new DbConnectionFactory(_connectionString);
        return new EventRepository(factory);
    }

    private static EventEnvelope BuildEnvelope(short schemaVersion = 1)
    {
        return EventEnvelope.CreateNew(
            id: Guid.NewGuid(),
            eventType: "order.created",
            occurredAt: DateTimeOffset.UtcNow.AddMinutes(-1),
            source: "schema-version-integration-test",
            tenantId: "tenant-schema-test",
            idempotencyKey: Guid.NewGuid().ToString("N"),
            correlationId: Guid.NewGuid(),
            payload: JsonDocument.Parse("""{"test": true}"""),
            schemaVersion: schemaVersion);
    }

    private async Task ApplyMigrationsAsync()
    {
        var repositoryRoot = ResolveRepositoryRoot();
        var migrationsDirectory = Path.Combine(repositoryRoot, "migrations", "postgres");
        var migrationFiles = Directory
            .GetFiles(migrationsDirectory, "*.sql")
            .OrderBy(Path.GetFileName, StringComparer.Ordinal)
            .ToArray();

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();

        foreach (var file in migrationFiles)
        {
            var sql = await File.ReadAllTextAsync(file);
            await using var command = connection.CreateCommand();
            command.CommandText = sql;
            await command.ExecuteNonQueryAsync();
        }
    }

    private static string ResolveRepositoryRoot()
    {
        var current = new DirectoryInfo(AppContext.BaseDirectory);

        while (current is not null)
        {
            if (File.Exists(Path.Combine(current.FullName, "EventPlatform.slnx")))
                return current.FullName;

            current = current.Parent;
        }

        throw new DirectoryNotFoundException("Could not resolve repository root from test execution directory.");
    }
}
