using System.Text.Json;
using EventPlatform.Infrastructure.Messaging;
using StackExchange.Redis;
using Testcontainers.Redis;

namespace EventPlatform.IntegrationTests.Infrastructure;

/// <summary>
/// Integration tests for <see cref="RedisEventPublisher"/> against a real Redis instance.
/// Satisfies the Definition of Done for issue #6: "Events visible via Redis CLI".
/// </summary>
public sealed class RedisEventPublisherIntegrationTests : IAsyncLifetime
{
    private readonly RedisContainer _redisContainer = new RedisBuilder("redis:7-alpine").Build();
    private IConnectionMultiplexer _multiplexer = default!;

    public async Task InitializeAsync()
    {
        await _redisContainer.StartAsync();
        _multiplexer = await ConnectionMultiplexer.ConnectAsync(_redisContainer.GetConnectionString());
    }

    public async Task DisposeAsync()
    {
        await _multiplexer.CloseAsync();
        _multiplexer.Dispose();
        await _redisContainer.DisposeAsync();
    }

    // -------------------------------------------------------------------------
    // PublishAsync
    // -------------------------------------------------------------------------

    [Fact]
    public async Task PublishAsync_WritesRequiredFields_ToRedisStream()
    {
        const string streamName = "test:events:publish-async";
        var publisher = new RedisEventPublisher(
            _multiplexer,
            new RedisPublisherOptions { StreamName = streamName });

        var eventId = Guid.NewGuid();
        var correlationId = Guid.NewGuid();
        var envelope = EventEnvelopeTestFactory.Create(
            eventId: eventId,
            eventType: "order.created",
            tenantId: "tenant-integration-1",
            correlationId: correlationId);

        await publisher.PublishAsync(envelope);

        var db = _multiplexer.GetDatabase();
        var entries = await db.StreamRangeAsync(streamName);

        var entry = Assert.Single(entries);
        Assert.Equal(eventId.ToString(), (string?)entry["event_id"]);
        Assert.Equal("order.created", (string?)entry["event_type"]);
        Assert.Equal("tenant-integration-1", (string?)entry["tenant_id"]);
        Assert.Equal(correlationId.ToString(), (string?)entry["correlation_id"]);
        Assert.False(string.IsNullOrEmpty((string?)entry["message"]), "message field should contain the full event JSON");
    }

    [Fact]
    public async Task PublishAsync_MessageField_ContainsValidJson_WithAllEventFields()
    {
        const string streamName = "test:events:message-json";
        var publisher = new RedisEventPublisher(
            _multiplexer,
            new RedisPublisherOptions { StreamName = streamName });

        var eventId = Guid.NewGuid();
        var envelope = EventEnvelopeTestFactory.Create(
            eventId: eventId,
            eventType: "user.registered",
            tenantId: "tenant-integration-2");

        await publisher.PublishAsync(envelope);

        var db = _multiplexer.GetDatabase();
        var entries = await db.StreamRangeAsync(streamName);
        var entry = Assert.Single(entries);

        var messageJson = (string?)entry["message"];
        Assert.NotNull(messageJson);

        using var doc = JsonDocument.Parse(messageJson);
        var root = doc.RootElement;

        Assert.Equal(eventId.ToString(), root.GetProperty("event_id").GetString());
        Assert.Equal("user.registered", root.GetProperty("event_type").GetString());
        Assert.Equal("tenant-integration-2", root.GetProperty("tenant_id").GetString());
        Assert.True(root.TryGetProperty("occurred_at", out _), "message JSON should include occurred_at");
        Assert.True(root.TryGetProperty("payload", out _), "message JSON should include payload");
    }

    [Fact]
    public async Task PublishAsync_MultipleEvents_AllWrittenToStream()
    {
        const string streamName = "test:events:multi";
        var publisher = new RedisEventPublisher(
            _multiplexer,
            new RedisPublisherOptions { StreamName = streamName });

        var envelopes = Enumerable.Range(0, 3)
            .Select(i => EventEnvelopeTestFactory.Create(
                eventType: "order.created",
                tenantId: $"tenant-{i}"))
            .ToList();

        foreach (var env in envelopes)
            await publisher.PublishAsync(env);

        var db = _multiplexer.GetDatabase();
        var entries = await db.StreamRangeAsync(streamName);

        Assert.Equal(3, entries.Length);
        var writtenTenants = entries
            .Select(e => (string?)e["tenant_id"])
            .ToHashSet();

        Assert.Contains("tenant-0", writtenTenants);
        Assert.Contains("tenant-1", writtenTenants);
        Assert.Contains("tenant-2", writtenTenants);
    }

    // -------------------------------------------------------------------------
    // PublishToStreamAsync
    // -------------------------------------------------------------------------

    [Fact]
    public async Task PublishToStreamAsync_WritesRequiredFields_FromJsonPayload()
    {
        const string streamName = "test:events:publish-to-stream";
        var publisher = new RedisEventPublisher(
            _multiplexer,
            new RedisPublisherOptions { StreamName = "events:ingress" });

        var eventId = Guid.NewGuid();
        var correlationId = Guid.NewGuid();
        var payload = JsonDocument.Parse($$"""
            {
                "event_id": "{{eventId}}",
                "event_type": "payment.processed",
                "tenant_id": "tenant-integration-3",
                "correlation_id": "{{correlationId}}",
                "payload": {"amount": 99.99}
            }
            """);

        await publisher.PublishToStreamAsync(streamName, payload);

        var db = _multiplexer.GetDatabase();
        var entries = await db.StreamRangeAsync(streamName);

        var entry = Assert.Single(entries);
        Assert.Equal(eventId.ToString(), (string?)entry["event_id"]);
        Assert.Equal("payment.processed", (string?)entry["event_type"]);
        Assert.Equal("tenant-integration-3", (string?)entry["tenant_id"]);
        Assert.Equal(correlationId.ToString(), (string?)entry["correlation_id"]);
        Assert.False(string.IsNullOrEmpty((string?)entry["message"]), "message field should contain the full JSON payload");
    }

    [Fact]
    public async Task PublishToStreamAsync_UsesProvidedStreamName_NotDefaultStreamName()
    {
        const string customStream = "test:events:custom-target";
        var publisher = new RedisEventPublisher(
            _multiplexer,
            new RedisPublisherOptions { StreamName = "events:ingress" });

        var payload = JsonDocument.Parse("""{"event_id": "aaa", "event_type": "x"}""");
        await publisher.PublishToStreamAsync(customStream, payload);

        var db = _multiplexer.GetDatabase();

        var customEntries = await db.StreamRangeAsync(customStream);
        Assert.Single(customEntries);

        var defaultEntries = await db.StreamRangeAsync("events:ingress");
        Assert.Empty(defaultEntries);
    }
}

/// <summary>
/// Minimal factory for building <see cref="EventPlatform.Domain.Events.EventEnvelope"/> instances
/// in integration tests without pulling in the full unit-test fixture assembly.
/// </summary>
file static class EventEnvelopeTestFactory
{
    public static EventPlatform.Domain.Events.EventEnvelope Create(
        Guid? eventId = null,
        string eventType = "order.created",
        string tenantId = "tenant-default",
        Guid? correlationId = null)
    {
        return EventPlatform.Domain.Events.EventEnvelope.CreateNew(
            id: eventId ?? Guid.NewGuid(),
            eventType: eventType,
            occurredAt: DateTimeOffset.UtcNow.AddMinutes(-1),
            source: "integration-test",
            tenantId: tenantId,
            idempotencyKey: Guid.NewGuid().ToString("N"),
            correlationId: correlationId ?? Guid.NewGuid(),
            payload: JsonDocument.Parse("""{"test": true}"""));
    }
}
