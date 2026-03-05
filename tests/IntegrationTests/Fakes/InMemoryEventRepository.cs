using EventPlatform.Domain.Events;
using EventPlatform.Infrastructure.Persistence.Exceptions;
using EventPlatform.Infrastructure.Persistence.Repositories;

namespace EventPlatform.IntegrationTests.Fakes;

public sealed class InMemoryEventRepository : IEventRepository
{
    private readonly Dictionary<string, EventEnvelope> _events = new(StringComparer.Ordinal);
    private readonly object _gate = new();
    public IOutboxRepository? OutboxRepository { get; set; }

    public Task InsertAsync(EventEnvelope envelope, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var key = ComposeKey(envelope.TenantId, envelope.IdempotencyKey ?? string.Empty);

        lock (_gate)
        {
            if (_events.ContainsKey(key))
            {
                throw new EventRepositoryConflictException(
                    "duplicate",
                    "ux_events_tenant_idempotency_key",
                    new InvalidOperationException("duplicate"));
            }

            _events[key] = envelope;
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Inserts both event and outbox in memory (simulating transactional behavior).
    /// </summary>
    public async Task InsertWithOutboxAsync(EventEnvelope envelope, OutboxEvent outboxEvent, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        await InsertAsync(envelope, cancellationToken);

        if (OutboxRepository != null)
        {
            await OutboxRepository.InsertIfMissingAsync(outboxEvent, cancellationToken);
        }
    }

    public async Task EnsureOutboxEntryAsync(OutboxEvent outboxEvent, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (OutboxRepository != null)
        {
            await OutboxRepository.InsertIfMissingAsync(outboxEvent, cancellationToken);
        }
    }

    public Task<BatchInsertResult> BatchInsertAsync(IEnumerable<EventEnvelope> envelopes, CancellationToken cancellationToken = default)
        => Task.FromResult(new BatchInsertResult(0, 0, 0, 0, Array.Empty<EventInsertDetail>()));

    public Task UpdateStatusAsync(Guid eventId, EventStatus newStatus, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            var key = _events.FirstOrDefault(pair => pair.Value.Id == eventId).Key;
            if (!string.IsNullOrWhiteSpace(key))
            {
                var current = _events[key];
                _events[key] = ApplyTransition(current, newStatus);
            }
        }

        return Task.CompletedTask;
    }

    public Task<bool> TryTransitionStatusAsync(
        Guid eventId,
        EventStatus expectedCurrentStatus,
        EventStatus newStatus,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            var pair = _events.FirstOrDefault(x => x.Value.Id == eventId);
            if (string.IsNullOrWhiteSpace(pair.Key))
                return Task.FromResult(false);

            if (pair.Value.Status != expectedCurrentStatus)
                return Task.FromResult(false);

            _events[pair.Key] = ApplyTransition(pair.Value, newStatus);
            return Task.FromResult(true);
        }
    }

    public Task<int> IncrementAttemptsAsync(Guid eventId, CancellationToken cancellationToken = default) => Task.FromResult(1);
    public Task MarkRetryableFailureAsync(Guid eventId, DateTimeOffset nextAttemptAt, string lastError, CancellationToken cancellationToken = default) => Task.CompletedTask;
    public Task MarkTerminalFailureAsync(Guid eventId, string lastError, CancellationToken cancellationToken = default) => Task.CompletedTask;
    public Task RequeueForRetryAsync(Guid eventId, CancellationToken cancellationToken = default) => Task.CompletedTask;
    public Task<EventEnvelope?> GetByIdAsync(Guid eventId, CancellationToken cancellationToken = default) => Task.FromResult<EventEnvelope?>(null);

    public Task<EventEnvelope?> GetByTenantAndIdempotencyKeyAsync(string tenantId, string idempotencyKey, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            _events.TryGetValue(ComposeKey(tenantId, idempotencyKey), out var value);
            return Task.FromResult<EventEnvelope?>(value);
        }
    }

    public Task<RetryableEventsPage> GetRetryableEventsAsync(DateTimeOffset now, int pageSize = 1000, int skip = 0, CancellationToken cancellationToken = default)
        => Task.FromResult(new RetryableEventsPage(Array.Empty<EventEnvelope>(), hasMore: false, skip: 0, pageSize: 0));

    public Task<long> GetCountAsync(EventStatus status, string? tenantId = null, CancellationToken cancellationToken = default)
        => Task.FromResult(0L);

    public Task<IReadOnlyList<EventEnvelope>> GetByCorrelationIdAsync(Guid correlationId, CancellationToken cancellationToken = default)
        => Task.FromResult<IReadOnlyList<EventEnvelope>>(Array.Empty<EventEnvelope>());

    public Task<IReadOnlyList<EventEnvelope>> GetByTenantIdAsync(string tenantId, int pageSize = 100, int skip = 0, CancellationToken cancellationToken = default)
        => Task.FromResult<IReadOnlyList<EventEnvelope>>(Array.Empty<EventEnvelope>());

    public Task<EventEnvelope?> GetOldestRetryableAsync(DateTimeOffset now, CancellationToken cancellationToken = default)
        => Task.FromResult<EventEnvelope?>(null);

    public void Clear()
    {
        lock (_gate)
        {
            _events.Clear();
        }
    }

    private static EventEnvelope ApplyTransition(EventEnvelope current, EventStatus newStatus)
    {
        return newStatus switch
        {
            EventStatus.QUEUED when current.Status == EventStatus.RECEIVED => current.MarkQueued(),
            EventStatus.PROCESSING when current.Status == EventStatus.QUEUED => current.MarkProcessing(),
            EventStatus.SUCCEEDED when current.Status == EventStatus.PROCESSING => current.MarkSucceeded(),
            _ => current
        };
    }

    private static string ComposeKey(string tenantId, string idempotencyKey) => $"{tenantId}::{idempotencyKey}";
}
