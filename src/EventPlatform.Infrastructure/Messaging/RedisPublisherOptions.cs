namespace EventPlatform.Infrastructure.Messaging;

public sealed class RedisPublisherOptions
{
    public string StreamName { get; init; } = "events:ingress";
}
