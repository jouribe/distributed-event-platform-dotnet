using System.Diagnostics.Metrics;

namespace EventIngestion.Api.Ingestion;

public static class IngestionMetrics
{
    private static readonly Meter Meter = new("EventPlatform.Ingestion", "1.0.0");

    public static readonly Counter<long> RequestsTotal =
        Meter.CreateCounter<long>("ingestion_requests_total");

    public static readonly Counter<long> IdempotentReplayCount =
        Meter.CreateCounter<long>("ingestion_idempotent_replay_total");

    public static readonly Counter<long> PublishFailures =
        Meter.CreateCounter<long>("ingestion_publish_failures_total");
}
