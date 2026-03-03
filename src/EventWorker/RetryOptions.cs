namespace EventWorker;

public sealed class RetryOptions
{
    public const string SectionName = "Retry";

    public int MaxAttempts { get; init; } = 5;
    public int MaxBackoffSeconds { get; init; } = 60;
    public int PollingIntervalSeconds { get; init; } = 10;
}
