using System.Text.Json;
using EventPlatform.Application.Abstractions;
using EventPlatform.Domain.Events;
using StackExchange.Redis;

namespace EventPlatform.Infrastructure.Messaging;

public sealed class RedisEventPublisher : IEventPublisher
{
    private readonly IConnectionMultiplexer _connectionMultiplexer;
    private readonly RedisPublisherOptions _options;

    public RedisEventPublisher(
        IConnectionMultiplexer connectionMultiplexer,
        RedisPublisherOptions options)
    {
        _connectionMultiplexer = connectionMultiplexer ?? throw new ArgumentNullException(nameof(connectionMultiplexer));
        _options = options ?? throw new ArgumentNullException(nameof(options));

        if (string.IsNullOrWhiteSpace(_options.StreamName))
            throw new ArgumentException("StreamName cannot be null or empty.", nameof(options));
    }

    public async Task PublishAsync(EventEnvelope envelope, CancellationToken cancellationToken = default)
    {
        if (envelope is null)
            throw new ArgumentNullException(nameof(envelope));

        cancellationToken.ThrowIfCancellationRequested();

        var database = _connectionMultiplexer.GetDatabase();
        var message = JsonSerializer.Serialize(new
        {
            event_id = envelope.Id,
            tenant_id = envelope.TenantId,
            event_type = envelope.EventType,
            correlation_id = envelope.CorrelationId,
            occurred_at = envelope.OccurredAt,
            received_at = envelope.ReceivedAt,
            idempotency_key = envelope.IdempotencyKey,
            payload = envelope.Payload.RootElement,
            status = envelope.Status.ToString()
        });

        var entries = new[]
        {
            new NameValueEntry("event_id", envelope.Id.ToString()),
            new NameValueEntry("tenant_id", envelope.TenantId),
            new NameValueEntry("event_type", envelope.EventType),
            new NameValueEntry("correlation_id", envelope.CorrelationId.ToString()),
            new NameValueEntry("message", message)
        };

        await database.StreamAddAsync(_options.StreamName, entries).ConfigureAwait(false);
    }
}
