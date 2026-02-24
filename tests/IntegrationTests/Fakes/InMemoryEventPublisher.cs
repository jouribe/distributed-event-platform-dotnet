using EventPlatform.Application.Abstractions;
using EventPlatform.Domain.Events;

namespace EventPlatform.IntegrationTests.Fakes;

public sealed class InMemoryEventPublisher : IEventPublisher
{
    public int PublishedCount { get; private set; }

    public Task PublishAsync(EventEnvelope envelope, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        PublishedCount++;
        return Task.CompletedTask;
    }

    public void Clear()
    {
        PublishedCount = 0;
    }
}
