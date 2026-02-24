namespace EventWorker;

public sealed class RedisConsumerOptions
{
    public const string SectionName = "RedisConsumer";

    public string ConnectionString { get; init; } = "localhost:63790";

    public string StreamName { get; init; } = "events:ingress";

    public string GroupName { get; init; } = "event-worker";

    public string ConsumerName { get; init; } = $"{Environment.MachineName}-{Environment.ProcessId}";

    public int ReadCount { get; init; } = 10;

    public int EmptyReadDelayMilliseconds { get; init; } = 250;

    public int ErrorDelayMilliseconds { get; init; } = 1000;
}
