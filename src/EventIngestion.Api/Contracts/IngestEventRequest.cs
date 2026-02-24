using System.Text.Json;
using System.Text.Json.Serialization;

namespace EventIngestion.Api.Contracts;

public sealed class IngestEventRequest
{
    [JsonPropertyName("event_id")]
    public Guid? EventId { get; init; }

    [JsonPropertyName("event_type")]
    public string? EventType { get; init; }

    [JsonPropertyName("occurred_at")]
    public DateTimeOffset? OccurredAt { get; init; }

    [JsonPropertyName("source")]
    public string? Source { get; init; }

    [JsonPropertyName("tenant_id")]
    public string? TenantId { get; init; }

    [JsonPropertyName("idempotency_key")]
    public string? IdempotencyKey { get; init; }

    [JsonPropertyName("correlation_id")]
    public Guid? CorrelationId { get; init; }

    [JsonPropertyName("payload")]
    public JsonElement Payload { get; init; }
}
