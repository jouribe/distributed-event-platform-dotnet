using EventPlatform.Domain.Events;
using EventPlatform.Infrastructure.Persistence.Repositories;

namespace EventPlatform.IntegrationTests.Fakes;

/// <summary>
/// In-memory implementation of IOutboxRepository for testing purposes.
/// </summary>
public sealed class InMemoryOutboxRepository : IOutboxRepository
{
    private readonly Dictionary<Guid, OutboxEvent> _outboxes = new();
    private readonly object _gate = new();

    public Task InsertAsync(OutboxEvent outboxEvent, CancellationToken cancellationToken = default)
        => InsertIfMissingAsync(outboxEvent, cancellationToken);

    public Task<bool> InsertIfMissingAsync(OutboxEvent outboxEvent, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            if (_outboxes.Values.Any(x => x.EventId == outboxEvent.EventId))
                return Task.FromResult(false);

            _outboxes[outboxEvent.Id] = outboxEvent;
            return Task.FromResult(true);
        }
    }

    public Task<IReadOnlyList<OutboxEvent>> GetUnpublishedAsync(int limit = 100, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            var unpublished = _outboxes.Values
                .Where(o => !o.IsPublished)
                .OrderBy(o => o.CreatedAt)
                .Take(limit)
                .ToList();

            return Task.FromResult<IReadOnlyList<OutboxEvent>>(unpublished.AsReadOnly());
        }
    }

    public Task MarkPublishedAsync(Guid outboxId, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            if (_outboxes.TryGetValue(outboxId, out var outbox))
            {
                _outboxes[outboxId] = outbox.MarkPublished();
            }
        }

        return Task.CompletedTask;
    }

    public async Task<bool> MarkPublishedAndQueueEventAsync(
        Guid outboxId,
        Guid eventId,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await MarkPublishedAsync(outboxId, cancellationToken);
        return true;
    }

    public Task RecordPublishAttemptAsync(Guid outboxId, string? error = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            if (_outboxes.TryGetValue(outboxId, out var outbox))
            {
                _outboxes[outboxId] = outbox.WithPublishAttempt(outbox.PublishAttempts + 1, error);
            }
        }

        return Task.CompletedTask;
    }

    public Task<long> DeletePublishedAsync(DateTimeOffset olderThan, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            var toDelete = _outboxes.Values
                .Where(o => o.IsPublished && o.PublishedAt < olderThan)
                .Select(o => o.Id)
                .ToList();

            foreach (var id in toDelete)
            {
                _outboxes.Remove(id);
            }

            return Task.FromResult((long)toDelete.Count);
        }
    }

    public Task<long> CountPendingAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            return Task.FromResult((long)_outboxes.Values.Count(o => !o.IsPublished));
        }
    }

    public void Clear()
    {
        lock (_gate)
        {
            _outboxes.Clear();
        }
    }
}
