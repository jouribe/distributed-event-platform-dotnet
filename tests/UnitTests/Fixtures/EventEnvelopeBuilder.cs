using System.Text.Json;
using EventPlatform.Domain.Events;

namespace EventPlatform.UnitTests.Fixtures;

/// <summary>
/// Test fixture builder for EventEnvelope instances.
/// </summary>
public class EventEnvelopeBuilder
{
    private Guid _id = Guid.NewGuid();
    private string _eventType = "OrderCreated";
    private DateTimeOffset _occurredAt = DateTimeOffset.UtcNow.AddMinutes(-1);
    private string _source = "checkout-api";
    private string _tenantId = "tenant-a";
    private string? _idempotencyKey = "idem-1";
    private Guid _correlationId = Guid.NewGuid();
    private JsonDocument _payload = JsonDocument.Parse("{\"orderId\":\"1\"}");

    public EventEnvelopeBuilder WithId(Guid id)
    {
        _id = id;
        return this;
    }

    public EventEnvelopeBuilder WithEventType(string eventType)
    {
        _eventType = eventType;
        return this;
    }

    public EventEnvelopeBuilder WithOccurredAt(DateTimeOffset occurredAt)
    {
        _occurredAt = occurredAt;
        return this;
    }

    public EventEnvelopeBuilder WithSource(string source)
    {
        _source = source;
        return this;
    }

    public EventEnvelopeBuilder WithTenantId(string tenantId)
    {
        _tenantId = tenantId;
        return this;
    }

    public EventEnvelopeBuilder WithIdempotencyKey(string? idempotencyKey)
    {
        _idempotencyKey = idempotencyKey;
        return this;
    }

    public EventEnvelopeBuilder WithCorrelationId(Guid correlationId)
    {
        _correlationId = correlationId;
        return this;
    }

    public EventEnvelopeBuilder WithPayload(string json)
    {
        _payload = JsonDocument.Parse(json);
        return this;
    }

    public EventEnvelope Build() =>
        EventEnvelope.CreateNew(
            id: _id,
            eventType: _eventType,
            occurredAt: _occurredAt,
            source: _source,
            tenantId: _tenantId,
            idempotencyKey: _idempotencyKey,
            correlationId: _correlationId,
            payload: _payload);

    /// <summary>
    /// Builds and transitions the envelope to QUEUED state.
    /// </summary>
    public EventEnvelope BuildQueued() => Build().MarkQueued();
}
