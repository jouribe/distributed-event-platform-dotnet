using EventPlatform.Application.Abstractions;
using EventPlatform.Domain.Events;

namespace EventPlatform.IntegrationTests.Fakes;

public sealed class InMemoryEventPublisher : IEventPublisher
{
    public int PublishedCount { get; private set; }
    public int PublishAttempts { get; private set; }
    public bool FailNextPublish { get; set; }

    public Task PublishAsync(EventEnvelope envelope, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        PublishAttempts++;

        if (FailNextPublish)
        {
            FailNextPublish = false;
            throw new InvalidOperationException("Simulated publish failure.");
        }

        PublishedCount++;
        return Task.CompletedTask;
    }

    public void Clear()
    {
        PublishedCount = 0;
        PublishAttempts = 0;
        FailNextPublish = false;
    }
}
