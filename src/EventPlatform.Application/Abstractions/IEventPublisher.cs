using EventPlatform.Domain.Events;

namespace EventPlatform.Application.Abstractions;

public interface IEventPublisher
{
    Task PublishAsync(EventEnvelope envelope, CancellationToken cancellationToken = default);
}
