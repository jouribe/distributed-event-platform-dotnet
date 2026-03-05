using System.Text.Json;
using EventPlatform.Infrastructure.Messaging;
using EventPlatform.UnitTests.Fixtures;
using Moq;
using StackExchange.Redis;

namespace EventPlatform.UnitTests.Infrastructure.Messaging;

public sealed class RedisEventPublisherTests
{
    // -------------------------------------------------------------------------
    // Constructor validation
    // -------------------------------------------------------------------------

    [Fact]
    public void Constructor_ThrowsArgumentNullException_WhenMultiplexerIsNull()
    {
        var act = () => new RedisEventPublisher(null!, new RedisPublisherOptions { StreamName = "s" });
        Assert.Throws<ArgumentNullException>(act);
    }

    [Fact]
    public void Constructor_ThrowsArgumentNullException_WhenOptionsIsNull()
    {
        var multiplexer = new Mock<IConnectionMultiplexer>(MockBehavior.Loose).Object;
        var act = () => new RedisEventPublisher(multiplexer, null!);
        Assert.Throws<ArgumentNullException>(act);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void Constructor_ThrowsArgumentException_WhenStreamNameIsNullOrWhitespace(string streamName)
    {
        var multiplexer = new Mock<IConnectionMultiplexer>(MockBehavior.Loose).Object;
        var act = () => new RedisEventPublisher(multiplexer, new RedisPublisherOptions { StreamName = streamName });
        Assert.Throws<ArgumentException>(act);
    }

    // -------------------------------------------------------------------------
    // PublishAsync — argument guards
    // -------------------------------------------------------------------------

    [Fact]
    public async Task PublishAsync_ThrowsArgumentNullException_WhenEnvelopeIsNull()
    {
        var publisher = CreatePublisher("events:ingress", out _, out _);
        await Assert.ThrowsAsync<ArgumentNullException>(() => publisher.PublishAsync(null!));
    }

    [Fact]
    public async Task PublishAsync_ThrowsOperationCanceledException_WhenTokenAlreadyCancelled()
    {
        var publisher = CreatePublisher("events:ingress", out _, out _);
        var envelope = new EventEnvelopeBuilder().Build();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => publisher.PublishAsync(envelope, cts.Token));
    }

    // -------------------------------------------------------------------------
    // PublishAsync — stream fields
    // -------------------------------------------------------------------------

    [Fact]
    public async Task PublishAsync_WritesRequiredFields_ToConfiguredStream()
    {
        var publisher = CreatePublisher("events:ingress", out _, out var dbMock);
        NameValueEntry[]? captured = null;
        SetupStreamAdd(dbMock, (_, entries) => captured = entries);

        var envelope = new EventEnvelopeBuilder()
            .WithEventType("order.created")
            .WithTenantId("tenant-123")
            .Build();

        await publisher.PublishAsync(envelope);

        Assert.NotNull(captured);
        AssertField(captured, "event_id", envelope.Id.ToString());
        AssertField(captured, "event_type", "order.created");
        AssertField(captured, "tenant_id", "tenant-123");
        AssertField(captured, "correlation_id", envelope.CorrelationId.ToString());
        Assert.Contains(captured, e => e.Name == "message");
    }

    [Fact]
    public async Task PublishAsync_UsesConfiguredStreamName()
    {
        const string expectedStream = "my:custom:stream";
        var publisher = CreatePublisher(expectedStream, out _, out var dbMock);
        RedisKey? capturedKey = null;
        SetupStreamAdd(dbMock, (key, _) => capturedKey = key);

        await publisher.PublishAsync(new EventEnvelopeBuilder().Build());

        Assert.NotNull(capturedKey);
        Assert.Equal(expectedStream, (string?)capturedKey.Value);
    }

    // -------------------------------------------------------------------------
    // PublishToStreamAsync — argument guards
    // -------------------------------------------------------------------------

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task PublishToStreamAsync_ThrowsArgumentException_WhenStreamNameIsInvalid(string? streamName)
    {
        var publisher = CreatePublisher("events:ingress", out _, out _);
        var payload = JsonDocument.Parse("{}");
        await Assert.ThrowsAsync<ArgumentException>(() => publisher.PublishToStreamAsync(streamName!, payload));
    }

    [Fact]
    public async Task PublishToStreamAsync_ThrowsArgumentNullException_WhenPayloadIsNull()
    {
        var publisher = CreatePublisher("events:ingress", out _, out _);
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => publisher.PublishToStreamAsync("events:test", null!));
    }

    [Fact]
    public async Task PublishToStreamAsync_ThrowsOperationCanceledException_WhenTokenAlreadyCancelled()
    {
        var publisher = CreatePublisher("events:ingress", out _, out _);
        var payload = JsonDocument.Parse("{}");
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => publisher.PublishToStreamAsync("events:test", payload, cts.Token));
    }

    // -------------------------------------------------------------------------
    // PublishToStreamAsync — stream fields
    // -------------------------------------------------------------------------

    [Fact]
    public async Task PublishToStreamAsync_WritesRequiredFields_FromJsonPayload()
    {
        var publisher = CreatePublisher("events:ingress", out _, out var dbMock);
        NameValueEntry[]? captured = null;
        SetupStreamAdd(dbMock, (_, entries) => captured = entries);

        var eventId = Guid.NewGuid();
        var correlationId = Guid.NewGuid();
        var payload = JsonDocument.Parse($$"""
            {
                "event_id": "{{eventId}}",
                "event_type": "user.registered",
                "tenant_id": "tenant-456",
                "correlation_id": "{{correlationId}}"
            }
            """);

        await publisher.PublishToStreamAsync("events:test", payload);

        Assert.NotNull(captured);
        AssertField(captured, "event_id", eventId.ToString());
        AssertField(captured, "event_type", "user.registered");
        AssertField(captured, "tenant_id", "tenant-456");
        AssertField(captured, "correlation_id", correlationId.ToString());
        Assert.Contains(captured, e => e.Name == "message");
    }

    [Fact]
    public async Task PublishToStreamAsync_StillWritesMessage_WhenMetadataFieldsAreMissing()
    {
        var publisher = CreatePublisher("events:ingress", out _, out var dbMock);
        NameValueEntry[]? captured = null;
        SetupStreamAdd(dbMock, (_, entries) => captured = entries);

        var payload = JsonDocument.Parse("""{"payload": "minimal"}""");
        await publisher.PublishToStreamAsync("events:test", payload);

        Assert.NotNull(captured);
        Assert.Contains(captured, e => e.Name == "message");
        Assert.DoesNotContain(captured, e => e.Name == "event_id");
        Assert.DoesNotContain(captured, e => e.Name == "event_type");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static RedisEventPublisher CreatePublisher(
        string streamName,
        out Mock<IConnectionMultiplexer> multiplexerMock,
        out Mock<IDatabase> dbMock)
    {
        dbMock = new Mock<IDatabase>(MockBehavior.Loose);
        multiplexerMock = new Mock<IConnectionMultiplexer>(MockBehavior.Loose);
        multiplexerMock
            .Setup(x => x.GetDatabase(It.IsAny<int>(), It.IsAny<object>()))
            .Returns(dbMock.Object);

        return new RedisEventPublisher(
            multiplexerMock.Object,
            new RedisPublisherOptions { StreamName = streamName });
    }

    /// <summary>
    /// Registers callbacks on both StreamAddAsync overloads present in StackExchange.Redis 2.x:
    /// - 6-param: (key, entries, messageId, maxLength:int?, useApprox, flags)
    /// - 8-param: (key, entries, messageId, maxLength:long?, useApprox, minId:long?, trimMode, flags)
    /// This ensures the callback fires regardless of which overload the compiler resolves to.
    /// </summary>
    private static void SetupStreamAdd(Mock<IDatabase> dbMock, Action<RedisKey, NameValueEntry[]> callback)
    {
        dbMock
            .Setup(x => x.StreamAddAsync(
                It.IsAny<RedisKey>(),
                It.IsAny<NameValueEntry[]>(),
                It.IsAny<RedisValue?>(),
                It.IsAny<int?>(),
                It.IsAny<bool>(),
                It.IsAny<CommandFlags>()))
            .Callback<RedisKey, NameValueEntry[], RedisValue?, int?, bool, CommandFlags>(
                (key, entries, _, _, _, _) => callback(key, entries))
            .ReturnsAsync(RedisValue.Null);

        dbMock
            .Setup(x => x.StreamAddAsync(
                It.IsAny<RedisKey>(),
                It.IsAny<NameValueEntry[]>(),
                It.IsAny<RedisValue?>(),
                It.IsAny<long?>(),
                It.IsAny<bool>(),
                It.IsAny<long?>(),
                It.IsAny<StreamTrimMode>(),
                It.IsAny<CommandFlags>()))
            .Callback<RedisKey, NameValueEntry[], RedisValue?, long?, bool, long?, StreamTrimMode, CommandFlags>(
                (key, entries, _, _, _, _, _, _) => callback(key, entries))
            .ReturnsAsync(RedisValue.Null);
    }

    private static void AssertField(NameValueEntry[] entries, string name, string expectedValue)
    {
        var entry = entries.FirstOrDefault(e => e.Name == name);
        Assert.True(entry.Name.HasValue, $"Expected field '{name}' not found in stream entries.");
        Assert.Equal(expectedValue, (string?)entry.Value);
    }
}
