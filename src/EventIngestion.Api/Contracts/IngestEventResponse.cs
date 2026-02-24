using System.Text.Json.Serialization;

namespace EventIngestion.Api.Contracts;

public sealed record IngestEventResponse(
    [property: JsonPropertyName("event_id")] Guid EventId,
    [property: JsonPropertyName("status")] string Status,
    [property: JsonPropertyName("idempotency_replayed")] bool IdempotencyReplayed);
