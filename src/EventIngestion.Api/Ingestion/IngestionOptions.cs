namespace EventIngestion.Api.Ingestion;

public sealed class IngestionOptions
{
    public string[] AllowedEventTypes { get; init; } = Array.Empty<string>();
    public string RedisStreamName { get; init; } = string.Empty;
}
