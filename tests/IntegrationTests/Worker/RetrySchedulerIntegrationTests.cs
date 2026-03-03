using EventWorker;
using EventPlatform.Application.Abstractions;
using EventPlatform.Infrastructure.Messaging;
using EventPlatform.Infrastructure.Persistence.DataAccess;
using EventPlatform.Infrastructure.Persistence.Repositories;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using StackExchange.Redis;
using Testcontainers.PostgreSql;
using Testcontainers.Redis;

namespace EventPlatform.IntegrationTests.Worker;

/// <summary>
/// End-to-end test verifying that <see cref="RetrySchedulerService"/> picks up a
/// FAILED_RETRYABLE event, transitions it to QUEUED, and publishes it to the Redis stream.
/// </summary>
public sealed class RetrySchedulerIntegrationTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgresContainer = new PostgreSqlBuilder("postgres:16-alpine")
        .WithDatabase("event_platform")
        .WithUsername("event_platform")
        .WithPassword("event_platform")
        .Build();

    private readonly RedisContainer _redisContainer = new RedisBuilder("redis:7-alpine").Build();

    private string _connectionString = string.Empty;
    private IConnectionMultiplexer _multiplexer = default!;

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    [Fact]
    public async Task RetryScheduler_ReEnqueues_FailedRetryableEventToQueuedAndStream()
    {
        const string streamName = "events:ingress:retry-integration-test";

        // Seed the event directly with raw SQL so both received_at and next_attempt_at
        // are well in the past, satisfying the check constraint (next_attempt_at >= received_at)
        // and ensuring the scheduler considers the event eligible immediately.
        var eventId = Guid.NewGuid();
        await SeedFailedRetryableEventAsync(eventId);

        // Build DI with the real EventRepository.
        var dbFactory = new DbConnectionFactory(_connectionString);
        var services = new ServiceCollection();
        services.AddScoped<IEventRepository>(_ => new EventRepository(dbFactory));
        var scopeFactory = services.BuildServiceProvider().GetRequiredService<IServiceScopeFactory>();

        var publisher = new RedisEventPublisher(
            _multiplexer,
            new RedisPublisherOptions { StreamName = streamName });

        var service = new TestableRetrySchedulerService(
            NullLogger<RetrySchedulerService>.Instance,
            scopeFactory,
            publisher,
            Options.Create(new RetryOptions { MaxAttempts = 5, MaxBackoffSeconds = 60, PollingIntervalSeconds = 0 }),
            Options.Create(new RedisConsumerOptions { StreamName = streamName }));

        // Act: run the scheduler until the event transitions to QUEUED.
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var runTask = service.RunAsync(cts.Token);

        await WaitUntilAsync(async () =>
        {
            var status = await GetEventStatusAsync(eventId);
            return status == "QUEUED";
        }, TimeSpan.FromSeconds(8));

        cts.Cancel();
        await runTask;

        // Assert DB: event is QUEUED.
        Assert.Equal("QUEUED", await GetEventStatusAsync(eventId));

        // Assert Redis stream: entry with matching event_id was published.
        var database = _multiplexer.GetDatabase();
        var entries = await database.StreamRangeAsync(streamName, "-", "+");
        var found = entries.Any(e =>
            e.Values.Any(v => v.Name == "event_id" && v.Value == eventId.ToString()));
        Assert.True(found, $"Expected a stream entry with event_id = {eventId}.");
    }

    // -------------------------------------------------------------------------
    // IAsyncLifetime
    // -------------------------------------------------------------------------

    public async Task InitializeAsync()
    {
        await Task.WhenAll(
            _postgresContainer.StartAsync(),
            _redisContainer.StartAsync());

        _connectionString = _postgresContainer.GetConnectionString();
        _multiplexer = await ConnectionMultiplexer.ConnectAsync(_redisContainer.GetConnectionString());

        await ApplyMigrationsAsync();
    }

    public async Task DisposeAsync()
    {
        if (_multiplexer is not null)
        {
            await _multiplexer.CloseAsync();
            await _multiplexer.DisposeAsync();
        }

        await Task.WhenAll(
            _postgresContainer.DisposeAsync().AsTask(),
            _redisContainer.DisposeAsync().AsTask());
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /// <summary>
    /// Inserts a FAILED_RETRYABLE event row directly, with received_at and next_attempt_at
    /// both set to one hour ago so the check constraint is satisfied and the scheduler
    /// treats the event as immediately eligible.
    /// </summary>
    private async Task SeedFailedRetryableEventAsync(Guid eventId)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            INSERT INTO event_platform.events (
                id, tenant_id, event_type,
                occurred_at, received_at,
                payload, idempotency_key, correlation_id,
                status, attempts,
                next_attempt_at, last_error, source
            )
            VALUES (
                @id, 'tenant-1', 'order.created',
                NOW() - INTERVAL '2 hours', NOW() - INTERVAL '2 hours',
                '{"orderId":"test-123"}'::jsonb, @idempotencyKey::text, @correlationId,
                'FAILED_RETRYABLE', 1,
                NOW() - INTERVAL '1 hour', 'simulated transient error', 'integration-test'
            );
            """;

        cmd.Parameters.AddWithValue("id", eventId);
        cmd.Parameters.AddWithValue("idempotencyKey", Guid.NewGuid().ToString());
        cmd.Parameters.AddWithValue("correlationId", Guid.NewGuid());

        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<string?> GetEventStatusAsync(Guid eventId)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT status FROM event_platform.events WHERE id = @id LIMIT 1;";
        cmd.Parameters.AddWithValue("id", eventId);

        var result = await cmd.ExecuteScalarAsync();
        return result?.ToString();
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition, TimeSpan timeout)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;

        while (DateTimeOffset.UtcNow < deadline)
        {
            if (await condition())
                return;

            await Task.Delay(50);
        }

        throw new TimeoutException($"Condition not met within {timeout.TotalSeconds:F1}s.");
    }

    private async Task ApplyMigrationsAsync()
    {
        var repositoryRoot = ResolveRepositoryRoot();
        var migrationsDir = Path.Combine(repositoryRoot, "migrations", "postgres");
        var migrationFiles = Directory
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

        throw new InvalidOperationException("Could not locate the repository root (EventPlatform.slnx not found).");
    }

    private sealed class TestableRetrySchedulerService : RetrySchedulerService
    {
        public TestableRetrySchedulerService(
            Microsoft.Extensions.Logging.ILogger<RetrySchedulerService> logger,
            IServiceScopeFactory scopeFactory,
            IEventPublisher eventPublisher,
            IOptions<RetryOptions> retryOptions,
            IOptions<RedisConsumerOptions> consumerOptions)
            : base(logger, scopeFactory, eventPublisher, retryOptions, consumerOptions)
        {
        }

        public Task RunAsync(CancellationToken cancellationToken)
            => ExecuteAsync(cancellationToken);
    }
}
