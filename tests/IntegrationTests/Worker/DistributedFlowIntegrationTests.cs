using System.Net;
using System.Net.Http.Json;
using EventPlatform.Infrastructure;
using EventPlatform.IntegrationTests.Fixtures;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using StackExchange.Redis;
using Testcontainers.PostgreSql;
using Testcontainers.Redis;

// Keep DistributedFlowIntegrationTests in the ApiTestsCollection so it runs sequentially
// with EventIngestionApiTests, preventing race conditions from concurrent global environment
// variable mutation in CustomWebApplicationFactory.InitializeAsync.
namespace EventPlatform.IntegrationTests.Worker;

/// <summary>
/// End-to-end tests validating the full distributed flow:
/// API ingestion → outbox publish → Redis stream → Worker processing → SUCCEEDED.
/// Also validates that duplicate ingestion results in exactly one processing pass.
/// </summary>
[Collection(nameof(ApiTestsCollection))]
public sealed class DistributedFlowIntegrationTests : IClassFixture<CustomWebApplicationFactory>
{
    private const string StreamName = "events:ingress";
    private const string GroupName = "event-worker-dist-test";
    private const string ConsumerName = "consumer-1";

    private readonly CustomWebApplicationFactory _factory;

    public DistributedFlowIntegrationTests(CustomWebApplicationFactory factory)
    {
        _factory = factory;
    }

    [Fact]
    public async Task IngestEvent_WorkerProcesses_EventReachesSucceeded()
    {
        await _factory.ResetStateAsync();

        const string tenantId = "tenant-dist-flow-1";
        var idempotencyKey = $"dist-flow-succeeded-{Guid.NewGuid():N}";

        var client = _factory.CreateClient(new WebApplicationFactoryClientOptions
        {
            AllowAutoRedirect = false,
            BaseAddress = new Uri("https://localhost")
        });

        var requestBody = new
        {
            event_type = "order.created",
            source = "distributed-flow-tests",
            tenant_id = tenantId,
            payload = new { order_id = "ord-001" }
        };

        var message = new HttpRequestMessage(HttpMethod.Post, "/events")
        {
            Content = JsonContent.Create(requestBody)
        };
        message.Headers.Add("Idempotency-Key", idempotencyKey);

        var response = await client.SendAsync(message);
        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);

        // Wait for the outbox to publish the event to the Redis stream.
        await _factory.WaitForStreamLengthAsync(expectedLength: 1, timeout: TimeSpan.FromSeconds(10));

        // Create the consumer group starting at position "0" so the Worker picks up
        // the already-published message when it performs the startup drain.
        var multiplexer = await ConnectionMultiplexer.ConnectAsync(_factory.RedisConnectionString);
        try
        {
            var db = multiplexer.GetDatabase();
            await db.StreamCreateConsumerGroupAsync(StreamName, GroupName, "0", createStream: false);

            var scopeFactory = DistributedFlowTestHelpers.BuildScopeFactory(
                _factory.PostgresConnectionString, new DistributedFlowTestHelpers.NoopHandler());
            var worker = DistributedFlowTestHelpers.CreateWorker(multiplexer, scopeFactory, StreamName, GroupName, ConsumerName);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            var workerTask = worker.RunAsync(cts.Token);

            await DistributedFlowTestHelpers.WaitUntilAsync(
                async () => await GetEventStatusByIdempotencyKeyAsync(tenantId, idempotencyKey) == "SUCCEEDED",
                TimeSpan.FromSeconds(10));

            cts.Cancel();
            await workerTask;
        }
        finally
        {
            await multiplexer.CloseAsync();
            multiplexer.Dispose();
        }

        Assert.Equal("SUCCEEDED", await GetEventStatusByIdempotencyKeyAsync(tenantId, idempotencyKey));
        Assert.Equal(1, await GetEventAttemptsByIdempotencyKeyAsync(tenantId, idempotencyKey));
    }

    [Fact]
    public async Task DuplicateIngest_WorkerProcessesExactlyOnce_AttemptsIsOne()
    {
        await _factory.ResetStateAsync();

        const string tenantId = "tenant-dist-flow-2";
        var idempotencyKey = $"dist-flow-dup-{Guid.NewGuid():N}";

        var client = _factory.CreateClient(new WebApplicationFactoryClientOptions
        {
            AllowAutoRedirect = false,
            BaseAddress = new Uri("https://localhost")
        });

        var requestBody = new
        {
            event_type = "order.created",
            source = "distributed-flow-tests",
            tenant_id = tenantId,
            payload = new { order_id = "ord-002" }
        };

        // First POST — accepted, event stored and queued.
        var first = new HttpRequestMessage(HttpMethod.Post, "/events") { Content = JsonContent.Create(requestBody) };
        first.Headers.Add("Idempotency-Key", idempotencyKey);
        var firstResponse = await client.SendAsync(first);
        Assert.Equal(HttpStatusCode.Accepted, firstResponse.StatusCode);

        // Wait for the single stream entry from the first POST.
        await _factory.WaitForStreamLengthAsync(expectedLength: 1, timeout: TimeSpan.FromSeconds(10));

        // Second POST with the same idempotency key — replayed, no new stream entry.
        var second = new HttpRequestMessage(HttpMethod.Post, "/events") { Content = JsonContent.Create(requestBody) };
        second.Headers.Add("Idempotency-Key", idempotencyKey);
        var secondResponse = await client.SendAsync(second);
        Assert.Equal(HttpStatusCode.OK, secondResponse.StatusCode);

        // Confirm the stream holds exactly one entry and stays there long enough
        // for the outbox publisher (100 ms poll) to have processed any potential duplicate.
        await _factory.EnsureStreamLengthStaysAtAsync(1, TimeSpan.FromMilliseconds(300));

        // Run the Worker against the single stream entry; it must process exactly once.
        const string groupName = GroupName + "-dup";
        var multiplexer = await ConnectionMultiplexer.ConnectAsync(_factory.RedisConnectionString);
        try
        {
            var db = multiplexer.GetDatabase();
            await db.StreamCreateConsumerGroupAsync(StreamName, groupName, "0", createStream: false);

            var scopeFactory = DistributedFlowTestHelpers.BuildScopeFactory(
                _factory.PostgresConnectionString, new DistributedFlowTestHelpers.NoopHandler());
            var worker = DistributedFlowTestHelpers.CreateWorker(multiplexer, scopeFactory, StreamName, groupName, ConsumerName);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            var workerTask = worker.RunAsync(cts.Token);

            await DistributedFlowTestHelpers.WaitUntilAsync(
                async () => await GetEventStatusByIdempotencyKeyAsync(tenantId, idempotencyKey) == "SUCCEEDED",
                TimeSpan.FromSeconds(10));

            cts.Cancel();
            await workerTask;
        }
        finally
        {
            await multiplexer.CloseAsync();
            multiplexer.Dispose();
        }

        // The event must have been processed exactly once despite two ingestion requests.
        Assert.Equal("SUCCEEDED", await GetEventStatusByIdempotencyKeyAsync(tenantId, idempotencyKey));
        Assert.Equal(1, await GetEventAttemptsByIdempotencyKeyAsync(tenantId, idempotencyKey));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private async Task<string?> GetEventStatusByIdempotencyKeyAsync(string tenantId, string idempotencyKey)
    {
        await using var connection = new NpgsqlConnection(_factory.PostgresConnectionString);
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText =
            "SELECT status FROM event_platform.events WHERE tenant_id = @tenant_id AND idempotency_key = @idempotency_key LIMIT 1;";
        cmd.Parameters.AddWithValue("tenant_id", tenantId);
        cmd.Parameters.AddWithValue("idempotency_key", idempotencyKey);
        return (await cmd.ExecuteScalarAsync())?.ToString();
    }

    private async Task<int> GetEventAttemptsByIdempotencyKeyAsync(string tenantId, string idempotencyKey)
    {
        await using var connection = new NpgsqlConnection(_factory.PostgresConnectionString);
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText =
            "SELECT attempts FROM event_platform.events WHERE tenant_id = @tenant_id AND idempotency_key = @idempotency_key LIMIT 1;";
        cmd.Parameters.AddWithValue("tenant_id", tenantId);
        cmd.Parameters.AddWithValue("idempotency_key", idempotencyKey);
        return Convert.ToInt32(await cmd.ExecuteScalarAsync());
    }
}

/// <summary>
/// Tests validating Worker retry mechanics: a handler failure increments the attempts counter
/// and transitions the event to FAILED_RETRYABLE.
/// </summary>
public sealed class WorkerRetryFlowIntegrationTests : IAsyncLifetime
{
    private const string StreamName = "events:ingress:retry-flow-test";
    private const string GroupName = "event-worker-retry-flow-test";
    private const string ConsumerName = "consumer-1";

    private readonly PostgreSqlContainer _postgresContainer = new PostgreSqlBuilder("postgres:16-alpine")
        .WithDatabase("event_platform")
        .WithUsername("event_platform")
        .WithPassword("event_platform")
        .Build();

    private readonly RedisContainer _redisContainer = new RedisBuilder("redis:7-alpine").Build();

    private string _connectionString = string.Empty;
    private IConnectionMultiplexer _multiplexer = default!;

    [Fact]
    public async Task Worker_IncrementsAttempts_AndTransitionsToFailedRetryable_WhenHandlerThrows()
    {
        var eventId = Guid.NewGuid();
        await SeedQueuedEventAsync(eventId);

        var db = _multiplexer.GetDatabase();
        await db.StreamCreateConsumerGroupAsync(StreamName, GroupName, "$", createStream: true);
        await PublishEventToStreamAsync(db, eventId);

        var services = new ServiceCollection();
        services.AddInfrastructurePersistence(_connectionString);
        services.AddSingleton<EventWorker.IWorkerEventHandler>(new DistributedFlowTestHelpers.FailingHandler());
        var scopeFactory = services.BuildServiceProvider().GetRequiredService<IServiceScopeFactory>();

        var worker = DistributedFlowTestHelpers.CreateWorker(
            _multiplexer, scopeFactory, StreamName, GroupName, ConsumerName,
            retryOptions: Options.Create(new EventWorker.RetryOptions { MaxAttempts = 5, MaxBackoffSeconds = 60 }));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var workerTask = worker.RunAsync(cts.Token);

        await DistributedFlowTestHelpers.WaitUntilAsync(
            async () => await GetEventStatusAsync(eventId) == "FAILED_RETRYABLE",
            TimeSpan.FromSeconds(10));

        cts.Cancel();
        await workerTask;

        Assert.Equal("FAILED_RETRYABLE", await GetEventStatusAsync(eventId));
        Assert.Equal(1, await GetEventAttemptsAsync(eventId));
    }

    // -------------------------------------------------------------------------
    // IAsyncLifetime
    // -------------------------------------------------------------------------

    public async Task InitializeAsync()
    {
        await Task.WhenAll(_postgresContainer.StartAsync(), _redisContainer.StartAsync());
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

    private async Task SeedQueuedEventAsync(Guid eventId)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            INSERT INTO event_platform.events (
                id, tenant_id, event_type,
                occurred_at, received_at,
                payload, idempotency_key, correlation_id,
                status, attempts, source
            )
            VALUES (
                @id, 'tenant-retry', 'order.created',
                NOW() - INTERVAL '1 minute', NOW() - INTERVAL '1 minute',
                '{"orderId":"test-retry-001"}'::jsonb, @idempotencyKey::text, @correlationId,
                'QUEUED', 0, 'retry-flow-test'
            );
            """;
        cmd.Parameters.AddWithValue("id", eventId);
        cmd.Parameters.AddWithValue("idempotencyKey", Guid.NewGuid().ToString());
        cmd.Parameters.AddWithValue("correlationId", Guid.NewGuid());
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task PublishEventToStreamAsync(IDatabase db, Guid eventId)
    {
        await db.StreamAddAsync(StreamName,
        [
            new NameValueEntry("event_id",   eventId.ToString()),
            new NameValueEntry("event_type", "order.created"),
            new NameValueEntry("tenant_id",  "tenant-retry"),
            new NameValueEntry("message",
                $$$"""{"event_id":"{{{eventId}}}","event_type":"order.created","tenant_id":"tenant-retry","payload":{"orderId":"test-retry-001"}}""")
        ]);
    }

    private async Task<string?> GetEventStatusAsync(Guid eventId)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT status FROM event_platform.events WHERE id = @id LIMIT 1;";
        cmd.Parameters.AddWithValue("id", eventId);
        return (await cmd.ExecuteScalarAsync())?.ToString();
    }

    private async Task<int> GetEventAttemptsAsync(Guid eventId)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT attempts FROM event_platform.events WHERE id = @id LIMIT 1;";
        cmd.Parameters.AddWithValue("id", eventId);
        return Convert.ToInt32(await cmd.ExecuteScalarAsync());
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
}

/// <summary>
/// Shared test helpers used by both <see cref="DistributedFlowIntegrationTests"/>
/// and <see cref="WorkerRetryFlowIntegrationTests"/>.
/// </summary>
internal static class DistributedFlowTestHelpers
{
    internal static IServiceScopeFactory BuildScopeFactory(
        string connectionString, EventWorker.IWorkerEventHandler handler)
    {
        var services = new ServiceCollection();
        services.AddInfrastructurePersistence(connectionString);
        services.AddSingleton(handler);
        return services.BuildServiceProvider().GetRequiredService<IServiceScopeFactory>();
    }

    internal static DistributedFlowTestWorker CreateWorker(
        IConnectionMultiplexer multiplexer,
        IServiceScopeFactory scopeFactory,
        string streamName,
        string groupName,
        string consumerName,
        IOptions<EventWorker.RetryOptions>? retryOptions = null)
    {
        return new DistributedFlowTestWorker(
            NullLogger<EventWorker.Worker>.Instance,
            multiplexer,
            new NoopBootstrapper(),
            scopeFactory,
            Options.Create(new EventWorker.RedisConsumerOptions
            {
                StreamName = streamName,
                GroupName = groupName,
                ConsumerName = consumerName,
                EmptyReadDelayMilliseconds = 50,
                ErrorDelayMilliseconds = 50
            }),
            retryOptions ?? Options.Create(new EventWorker.RetryOptions { MaxAttempts = 3, MaxBackoffSeconds = 60 }));
    }

    internal static async Task WaitUntilAsync(Func<Task<bool>> condition, TimeSpan timeout)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;
        while (DateTimeOffset.UtcNow < deadline)
        {
            if (await condition()) return;
            await Task.Delay(50);
        }

        throw new TimeoutException($"Condition not met within {timeout.TotalSeconds:F1}s.");
    }

    internal sealed class NoopHandler : EventWorker.IWorkerEventHandler
    {
        public Task HandleAsync(Guid eventId, StreamEntry entry, string phase, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }
    }

    internal sealed class FailingHandler : EventWorker.IWorkerEventHandler
    {
        public Task HandleAsync(Guid eventId, StreamEntry entry, string phase, CancellationToken cancellationToken)
            => throw new InvalidOperationException("Simulated handler failure for retry test.");
    }

    private sealed class NoopBootstrapper : EventWorker.IRedisConsumerGroupBootstrapper
    {
        public Task EnsureConsumerGroupAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}

/// <summary>
/// A <see cref="EventWorker.Worker"/> subclass that exposes <see cref="ExecuteAsync"/>
/// as a public method for use in integration tests.
/// </summary>
internal sealed class DistributedFlowTestWorker : EventWorker.Worker
{
    public DistributedFlowTestWorker(
        ILogger<EventWorker.Worker> logger,
        IConnectionMultiplexer multiplexer,
        EventWorker.IRedisConsumerGroupBootstrapper bootstrapper,
        IServiceScopeFactory scopeFactory,
        IOptions<EventWorker.RedisConsumerOptions> options,
        IOptions<EventWorker.RetryOptions> retryOptions)
        : base(logger, multiplexer, bootstrapper, scopeFactory, options, retryOptions)
    {
    }

    public Task RunAsync(CancellationToken ct) => ExecuteAsync(ct);
}
