using DotNet.Testcontainers.Containers;
using EventPlatform.Infrastructure;
using Microsoft.AspNetCore.Hosting;
using EventPlatform.Infrastructure.Messaging;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Npgsql;
using StackExchange.Redis;
using Testcontainers.PostgreSql;
using Testcontainers.Redis;
using Xunit;

namespace EventPlatform.IntegrationTests.Fixtures;

public sealed class CustomWebApplicationFactory : WebApplicationFactory<Program>, IAsyncLifetime
{
    private const string StreamName = "events:ingress";
    private readonly PostgreSqlContainer _postgresContainer = new PostgreSqlBuilder("postgres:16-alpine")
        .WithDatabase("event_platform")
        .WithUsername("event_platform")
        .WithPassword("event_platform")
        .Build();
    private readonly RedisContainer _redisContainer = new RedisBuilder("redis:7-alpine")
        .Build();

    private IConnectionMultiplexer? _redisMultiplexer;
    private string? _previousDbEnvironmentValue;
    private string? _previousRedisEnvironmentValue;

    public string PostgresConnectionString { get; private set; } = string.Empty;
    public string RedisConnectionString { get; private set; } = string.Empty;

    public async Task ResetStateAsync()
    {
        await using var connection = new NpgsqlConnection(PostgresConnectionString);
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = @"
TRUNCATE TABLE event_platform.outbox_events, event_platform.events RESTART IDENTITY CASCADE;";
        await command.ExecuteNonQueryAsync();

        var redis = _redisMultiplexer?.GetDatabase()
            ?? throw new InvalidOperationException("Redis multiplexer is not initialized.");

        await redis.ExecuteAsync("FLUSHDB");
    }

    public async Task<int> CountEventsByTenantAndIdempotencyKeyAsync(string tenantId, string idempotencyKey)
    {
        await using var connection = new NpgsqlConnection(PostgresConnectionString);
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT COUNT(*)
FROM event_platform.events
WHERE tenant_id = @tenant_id AND idempotency_key = @idempotency_key;";
        command.Parameters.AddWithValue("tenant_id", tenantId);
        command.Parameters.AddWithValue("idempotency_key", idempotencyKey);

        var result = await command.ExecuteScalarAsync();
        return Convert.ToInt32(result);
    }

    public async Task<Guid> GetCorrelationIdByTenantAndIdempotencyKeyAsync(string tenantId, string idempotencyKey)
    {
        await using var connection = new NpgsqlConnection(PostgresConnectionString);
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT correlation_id
FROM event_platform.events
WHERE tenant_id = @tenant_id AND idempotency_key = @idempotency_key
LIMIT 1;";
        command.Parameters.AddWithValue("tenant_id", tenantId);
        command.Parameters.AddWithValue("idempotency_key", idempotencyKey);

        var result = await command.ExecuteScalarAsync();
        if (result is null || result is DBNull)
        {
            throw new InvalidOperationException("Expected event row was not found for tenant/idempotency key.");
        }

        return (Guid)result;
    }

    public async Task<long> GetStreamLengthAsync()
    {
        var redis = _redisMultiplexer?.GetDatabase()
            ?? throw new InvalidOperationException("Redis multiplexer is not initialized.");

        return await redis.StreamLengthAsync(StreamName);
    }

    public async Task WaitForStreamLengthAsync(long expectedLength, TimeSpan timeout)
    {
        var start = DateTimeOffset.UtcNow;

        while (DateTimeOffset.UtcNow - start < timeout)
        {
            var currentLength = await GetStreamLengthAsync();
            if (currentLength == expectedLength)
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(100));
        }

        var finalLength = await GetStreamLengthAsync();
        throw new TimeoutException($"Timed out waiting for stream '{StreamName}' length {expectedLength}. Current length: {finalLength}.");
    }

    async Task IAsyncLifetime.InitializeAsync()
    {
        await Task.WhenAll(_postgresContainer.StartAsync(), _redisContainer.StartAsync());

        PostgresConnectionString = _postgresContainer.GetConnectionString();
        RedisConnectionString = _redisContainer.GetConnectionString();

        _previousDbEnvironmentValue = Environment.GetEnvironmentVariable("EVENTPLATFORM_DB");
        _previousRedisEnvironmentValue = Environment.GetEnvironmentVariable("EVENTPLATFORM_REDIS");
        Environment.SetEnvironmentVariable("EVENTPLATFORM_DB", PostgresConnectionString);
        Environment.SetEnvironmentVariable("EVENTPLATFORM_REDIS", RedisConnectionString);

        _redisMultiplexer = await ConnectionMultiplexer.ConnectAsync(RedisConnectionString);

        await ApplyMigrationsAsync();
    }

    async Task IAsyncLifetime.DisposeAsync()
    {
        base.Dispose();

        if (_redisMultiplexer is not null)
        {
            await _redisMultiplexer.CloseAsync();
            await _redisMultiplexer.DisposeAsync();
        }

        Environment.SetEnvironmentVariable("EVENTPLATFORM_DB", _previousDbEnvironmentValue);
        Environment.SetEnvironmentVariable("EVENTPLATFORM_REDIS", _previousRedisEnvironmentValue);

        await _redisContainer.DisposeAsync();
        await _postgresContainer.DisposeAsync();
    }

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Production");

        builder.ConfigureAppConfiguration((_, configBuilder) =>
        {
            configBuilder.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ConnectionStrings:EventPlatformDb"] = PostgresConnectionString,
                ["ConnectionStrings:EventPlatformRedis"] = RedisConnectionString,
                ["Ingestion:RedisStreamName"] = StreamName
            });
        });

        builder.ConfigureServices(services =>
        {
            var outboxHostedServices = services
                .Where(descriptor =>
                    descriptor.ServiceType == typeof(IHostedService)
                    && descriptor.ImplementationType == typeof(OutboxPublisherService))
                .ToList();

            foreach (var descriptor in outboxHostedServices)
            {
                services.Remove(descriptor);
            }

            services.RemoveAll<IConfigureOptions<OutboxPublisherOptions>>();
            services.AddOutboxPublisher(options =>
            {
                options.PollIntervalMilliseconds = 100;
                options.MaxBatchSize = 100;
            });
        });
    }

    private async Task ApplyMigrationsAsync()
    {
        var repositoryRoot = ResolveRepositoryRoot();
        var migrationsDirectory = Path.Combine(repositoryRoot, "migrations", "postgres");
        var migrationFiles = Directory
            .GetFiles(migrationsDirectory, "*.sql")
            .OrderBy(Path.GetFileName, StringComparer.Ordinal)
            .ToArray();

        await using var connection = new NpgsqlConnection(PostgresConnectionString);
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
            var solutionPath = Path.Combine(current.FullName, "EventPlatform.slnx");
            if (File.Exists(solutionPath))
            {
                return current.FullName;
            }

            current = current.Parent;
        }

        throw new DirectoryNotFoundException("Could not resolve repository root from test execution directory.");
    }
}
