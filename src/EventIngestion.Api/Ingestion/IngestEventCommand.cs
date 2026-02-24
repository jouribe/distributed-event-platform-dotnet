using System.Text.Json;

namespace EventIngestion.Api.Ingestion;

public sealed record IngestEventCommand(
    Guid EventId,
    string EventType,
    DateTimeOffset OccurredAt,
    string Source,
    string TenantId,
    string IdempotencyKey,
    Guid CorrelationId,
    JsonElement Payload);
