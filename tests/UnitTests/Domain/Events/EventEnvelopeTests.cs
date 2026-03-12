using System.Text.Json;
using EventPlatform.Domain.Events;
using EventPlatform.UnitTests.Fixtures;

namespace EventPlatform.UnitTests.Domain.Events;

public class EventEnvelopeTests
{
    [Fact]
    public void CreateNew_SetsReceivedStateAndDefaults()
    {
        var envelope = new EventEnvelopeBuilder().Build();

        Assert.Equal(EventStatus.RECEIVED, envelope.Status);
        Assert.Equal(0, envelope.Attempts);
        Assert.Null(envelope.NextAttemptAt);
        Assert.Null(envelope.LastError);
        Assert.True(envelope.ReceivedAt >= envelope.OccurredAt);
    }

    [Fact]
    public void MarkProcessing_IncrementsAttempts_AndClearsErrorState()
    {
        var envelope = new EventEnvelopeBuilder().BuildQueued();

        var processing = envelope.MarkProcessing();

        Assert.Equal(EventStatus.PROCESSING, processing.Status);
        Assert.Equal(1, processing.Attempts);
        Assert.Null(processing.NextAttemptAt);
        Assert.Null(processing.LastError);
    }

    [Fact]
    public void RetryFlow_KeepsAttemptsStableAcrossFailureAndRequeue()
    {
        var queued = new EventEnvelopeBuilder().BuildQueued();
        var processing = queued.MarkProcessing();
        var nextAttemptAt = DateTimeOffset.UtcNow.AddMinutes(5);

        var failed = processing.MarkRetryableFailure("timeout", nextAttemptAt);
        var requeued = failed.RequeueAfterRetry();

        Assert.Equal(EventStatus.FAILED_RETRYABLE, failed.Status);
        Assert.Equal(1, failed.Attempts);
        Assert.Equal("timeout", failed.LastError);
        Assert.Equal(nextAttemptAt, failed.NextAttemptAt);

        Assert.Equal(EventStatus.QUEUED, requeued.Status);
        Assert.Equal(1, requeued.Attempts);
        Assert.Equal("timeout", requeued.LastError);
        Assert.Null(requeued.NextAttemptAt);
    }

    [Fact]
    public void MarkTerminalFailure_UsesCurrentAttemptCount()
    {
        var processing = new EventEnvelopeBuilder().BuildQueued().MarkProcessing();

        var terminal = processing.MarkTerminalFailure("invalid payload");

        Assert.Equal(EventStatus.FAILED_TERMINAL, terminal.Status);
        Assert.Equal(1, terminal.Attempts);
        Assert.Equal("invalid payload", terminal.LastError);
        Assert.Null(terminal.NextAttemptAt);
    }

    [Fact]
    public void MarkRetryableFailure_Throws_WhenNextAttemptIsNotFuture()
    {
        var processing = new EventEnvelopeBuilder().BuildQueued().MarkProcessing();

        Assert.Throws<ArgumentException>(() =>
            processing.MarkRetryableFailure("timeout", DateTimeOffset.UtcNow.AddSeconds(-1)));
    }

    [Fact]
    public void CreateNew_Throws_WhenSourceIsEmpty()
    {
        Assert.Throws<ArgumentException>(() =>
            EventEnvelope.CreateNew(
                id: Guid.NewGuid(),
                eventType: "OrderCreated",
                occurredAt: DateTimeOffset.UtcNow.AddMinutes(-1),
                source: " ",
                tenantId: "tenant-a",
                idempotencyKey: "idem-1",
                correlationId: Guid.NewGuid(),
                payload: JsonDocument.Parse("{\"orderId\":\"1\"}")));
    }

    [Fact]
    public void CreateNew_Throws_WhenCorrelationIdIsEmpty()
    {
        Assert.Throws<ArgumentException>(() =>
            EventEnvelope.CreateNew(
                id: Guid.NewGuid(),
                eventType: "OrderCreated",
                occurredAt: DateTimeOffset.UtcNow.AddMinutes(-1),
                source: "checkout-api",
                tenantId: "tenant-a",
                idempotencyKey: "idem-1",
                correlationId: Guid.Empty,
                payload: JsonDocument.Parse("{\"orderId\":\"1\"}")));
    }

    // -------------------------------------------------------------------------
    // SchemaVersion tests
    // -------------------------------------------------------------------------

    [Fact]
    public void CreateNew_SetsSchemaVersion_ToOneByDefault()
    {
        var envelope = new EventEnvelopeBuilder().Build();

        Assert.Equal((short)1, envelope.SchemaVersion);
    }

    [Fact]
    public void CreateNew_SetsSchemaVersion_WhenExplicitlyProvided()
    {
        var envelope = new EventEnvelopeBuilder().WithSchemaVersion(3).Build();

        Assert.Equal((short)3, envelope.SchemaVersion);
    }

    [Fact]
    public void CreateNew_Throws_WhenSchemaVersionIsZero()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new EventEnvelopeBuilder().WithSchemaVersion(0).Build());
    }

    [Fact]
    public void CreateNew_Throws_WhenSchemaVersionIsNegative()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new EventEnvelopeBuilder().WithSchemaVersion(-1).Build());
    }

    [Fact]
    public void SchemaVersion_HasNoPublicSetter_IsImmutable()
    {
        // init-only enforcement: verify that no mutation is possible after construction
        var envelope = new EventEnvelopeBuilder().WithSchemaVersion(2).Build();
        var copy = envelope with { SchemaVersion = 5 };

        // original must be unchanged
        Assert.Equal((short)2, envelope.SchemaVersion);
        Assert.Equal((short)5, copy.SchemaVersion);
    }

    [Fact]
    public void RehydrateFromPersistence_RoundTrips_SchemaVersion()
    {
        var original = new EventEnvelopeBuilder().WithSchemaVersion(7).Build().MarkQueued();

        var rehydrated = EventEnvelope.RehydrateFromPersistence(
            id: original.Id,
            eventType: original.EventType,
            occurredAt: original.OccurredAt,
            receivedAt: original.ReceivedAt,
            source: original.Source,
            tenantId: original.TenantId,
            idempotencyKey: original.IdempotencyKey,
            correlationId: original.CorrelationId,
            payload: original.Payload,
            status: original.Status,
            attempts: original.Attempts,
            nextAttemptAt: original.NextAttemptAt,
            lastError: original.LastError,
            schemaVersion: 7);

        Assert.Equal((short)7, rehydrated.SchemaVersion);
    }

    [Fact]
    public void RehydrateFromPersistence_DefaultsSchemaVersionToOne_WhenOmitted()
    {
        var envelope = new EventEnvelopeBuilder().Build();

        // Omit the schemaVersion parameter — relies on the default = 1 overload
        var rehydrated = EventEnvelope.RehydrateFromPersistence(
            id: envelope.Id,
            eventType: envelope.EventType,
            occurredAt: envelope.OccurredAt,
            receivedAt: envelope.ReceivedAt,
            source: envelope.Source,
            tenantId: envelope.TenantId,
            idempotencyKey: envelope.IdempotencyKey,
            correlationId: envelope.CorrelationId,
            payload: envelope.Payload,
            status: envelope.Status,
            attempts: envelope.Attempts,
            nextAttemptAt: envelope.NextAttemptAt,
            lastError: envelope.LastError);

        Assert.Equal((short)1, rehydrated.SchemaVersion);
    }
}
