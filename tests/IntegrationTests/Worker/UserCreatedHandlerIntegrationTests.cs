using System.Text.Json;
using EventPlatform.Infrastructure.Persistence.DataAccess;
using EventWorker.Handlers;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using StackExchange.Redis;
using Testcontainers.PostgreSql;

namespace EventPlatform.IntegrationTests.Worker;

/// <summary>
/// Integration tests for <see cref="UserCreatedEventHandler"/> against a real PostgreSQL instance.
/// Verifies that user records are persisted correctly and that the handler is idempotent.
/// </summary>
public sealed class UserCreatedHandlerIntegrationTests : IAsyncLifetime
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

    public Task DisposeAsync() => _postgresContainer.DisposeAsync().AsTask();

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    [Fact]
    public async Task HandleAsync_CreatesUserRow_WhenValidPayload()
    {
        var handler = CreateHandler();
        var eventId = Guid.NewGuid();
        var entry   = BuildEntry("usr-001", "john@example.com", "John Doe", "tenant-1", eventId);

        await handler.HandleAsync(eventId, entry, "read-new", CancellationToken.None);

        var user = await GetUserAsync("usr-001");
        Assert.NotNull(user);
        Assert.Equal("john@example.com", user.Email);
        Assert.Equal("John Doe",         user.Name);
        Assert.Equal("tenant-1",         user.TenantId);
        Assert.Equal(eventId,            user.SourceEventId);
    }

    [Fact]
    public async Task HandleAsync_IsIdempotent_WhenDuplicateEventReceived()
    {
        var handler = CreateHandler();
        var eventId1 = Guid.NewGuid();
        var eventId2 = Guid.NewGuid();

        var entry1 = BuildEntry("usr-002", "jane@example.com",           "Jane Doe",       "tenant-1", eventId1);
        var entry2 = BuildEntry("usr-002", "jane.duplicate@example.com", "Jane Duplicate", "tenant-1", eventId2);

        await handler.HandleAsync(eventId1, entry1, "read-new",      CancellationToken.None);
        await handler.HandleAsync(eventId2, entry2, "drain-pending",  CancellationToken.None); // duplicate — must NOT throw

        var count = await CountUsersAsync("usr-002");
        Assert.Equal(1, count); // exactly one row persisted

        var user = await GetUserAsync("usr-002");
        Assert.Equal("jane@example.com", user!.Email); // first-write wins
    }

    [Fact]
    public async Task HandleAsync_CreatesDistinctRows_WhenDifferentExternalUserIds()
    {
        var handler = CreateHandler();

        await handler.HandleAsync(Guid.NewGuid(), BuildEntry("usr-003", "alice@example.com", "Alice", "tenant-1"), "read-new", CancellationToken.None);
        await handler.HandleAsync(Guid.NewGuid(), BuildEntry("usr-004", "bob@example.com",   "Bob",   "tenant-1"), "read-new", CancellationToken.None);

        Assert.Equal(1, await CountUsersAsync("usr-003"));
        Assert.Equal(1, await CountUsersAsync("usr-004"));
    }

    [Fact]
    public async Task HandleAsync_SetsSourceEventIdToNull_WhenEventIdIsEmpty()
    {
        var handler = CreateHandler();
        var entry   = BuildEntry("usr-005", "ghost@example.com", "Ghost", "tenant-1", eventId: null);

        // eventId = Guid.Empty signals "no persistence link"
        await handler.HandleAsync(Guid.Empty, entry, "reclaim-maintenance", CancellationToken.None);

        var user = await GetUserAsync("usr-005");
        Assert.NotNull(user);
        Assert.Null(user.SourceEventId);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private UserCreatedEventHandler CreateHandler()
        => new(new DbConnectionFactory(_connectionString), NullLogger<UserCreatedEventHandler>.Instance);

    private static StreamEntry BuildEntry(
        string userId,
        string email,
        string name,
        string tenantId = "tenant-1",
        Guid?  eventId  = null)
    {
        var id      = eventId ?? Guid.NewGuid();
        var message = JsonSerializer.Serialize(new
        {
            event_id   = id,
            event_type = "user.created",
            payload    = new { user_id = userId, email, name },
        });

        return new StreamEntry("1-0",
        [
            new NameValueEntry("event_id",   id.ToString()),
            new NameValueEntry("event_type", "user.created"),
            new NameValueEntry("tenant_id",  tenantId),
            new NameValueEntry("message",    message),
        ]);
    }

    private async Task<UserRow?> GetUserAsync(string externalUserId)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText =
            """
            SELECT external_user_id, tenant_id, email, name, source_event_id
            FROM event_platform.users
            WHERE external_user_id = @id
            LIMIT 1;
            """;
        cmd.Parameters.AddWithValue("id", externalUserId);

        await using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync()) return null;

        return new UserRow(
            reader.GetString(0),
            reader.GetString(1),
            reader.GetString(2),
            reader.GetString(3),
            reader.IsDBNull(4) ? null : reader.GetGuid(4));
    }

    private async Task<int> CountUsersAsync(string externalUserId)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM event_platform.users WHERE external_user_id = @id;";
        cmd.Parameters.AddWithValue("id", externalUserId);

        return Convert.ToInt32(await cmd.ExecuteScalarAsync());
    }

    private async Task ApplyMigrationsAsync()
    {
        var repositoryRoot    = ResolveRepositoryRoot();
        var migrationsDir     = Path.Combine(repositoryRoot, "migrations", "postgres");
        var migrationFiles    = Directory
            .GetFiles(migrationsDir, "*.sql")
            .OrderBy(Path.GetFileName, StringComparer.Ordinal)
            .ToArray();

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();

        foreach (var file in migrationFiles)
        {
            var sql = await File.ReadAllTextAsync(file);
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync();
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

    private sealed record UserRow(
        string ExternalUserId,
        string TenantId,
        string Email,
        string Name,
        Guid?  SourceEventId);
}
