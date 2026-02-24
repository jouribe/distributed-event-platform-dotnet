using System.Text.Json.Serialization;

namespace EventPlatform.IntegrationTests.Contracts;

public sealed class IngestResponseModel
{
    [JsonPropertyName("event_id")]
    public Guid EventId { get; init; }

    [JsonPropertyName("status")]
    public string Status { get; init; } = string.Empty;

    [JsonPropertyName("idempotency_replayed")]
    public bool IdempotencyReplayed { get; init; }
}
