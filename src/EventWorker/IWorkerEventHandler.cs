using StackExchange.Redis;

namespace EventWorker;

public interface IWorkerEventHandler
{
    Task HandleAsync(Guid eventId, StreamEntry entry, string phase, CancellationToken cancellationToken);
}

public sealed class NoopWorkerEventHandler : IWorkerEventHandler
{
    private readonly ILogger<NoopWorkerEventHandler> _logger;

    public NoopWorkerEventHandler(ILogger<NoopWorkerEventHandler> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public Task HandleAsync(Guid eventId, StreamEntry entry, string phase, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        _logger.LogInformation(
            "Processed event {EventId} from stream entry {EntryId} (phase: {Phase})",
            eventId,
            entry.Id,
            phase);

        return Task.CompletedTask;
    }
}
