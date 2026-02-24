using EventPlatform.Domain.Events;
using System.Text.Json;

namespace EventPlatform.Application.Abstractions;

public interface IEventPublisher
{
    Task PublishAsync(EventEnvelope envelope, CancellationToken cancellationToken = default);

    /// <summary>
    /// Publishes a pre-serialized event payload to a specific Redis stream.
    /// Used by the outbox publisher to recover from failures.
    /// </summary>
    /// <param name="streamName">The name of the Redis stream.</param>
    /// <param name="payload">The event payload as a JsonDocument.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task PublishToStreamAsync(string streamName, JsonDocument payload, CancellationToken cancellationToken = default);
}
