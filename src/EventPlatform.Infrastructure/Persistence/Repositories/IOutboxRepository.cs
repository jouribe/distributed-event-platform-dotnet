using EventPlatform.Domain.Events;

namespace EventPlatform.Infrastructure.Persistence.Repositories;

/// <summary>
/// Repository interface for persisting and querying outbox events.
/// The outbox pattern ensures event publishing reliability.
/// </summary>
public interface IOutboxRepository
{
    /// <summary>
    /// Inserts a new outbox event into the database.
    /// </summary>
    /// <param name="outboxEvent">The outbox event to insert.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="outboxEvent"/> is null.</exception>
    Task InsertAsync(OutboxEvent outboxEvent, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves unpublished outbox events with pagination.
    /// </summary>
    /// <param name="limit">Maximum number of events to return.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A read-only list of unpublished outbox events.</returns>
    Task<IReadOnlyList<OutboxEvent>> GetUnpublishedAsync(
        int limit = 100,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Updates the published timestamp and clears errors for a successfully published outbox event.
    /// </summary>
    /// <param name="outboxId">The outbox event id.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task MarkPublishedAsync(Guid outboxId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Records a failed publish attempt for an outbox event.
    /// </summary>
    /// <param name="outboxId">The outbox event id.</param>
    /// <param name="error">Optional error message from the failed attempt.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task RecordPublishAttemptAsync(
        Guid outboxId,
        string? error = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes published outbox events older than the specified age.
    /// </summary>
    /// <param name="olderThan">Delete entries published before this time.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The number of deleted rows.</returns>
    Task<long> DeletePublishedAsync(DateTimeOffset olderThan, CancellationToken cancellationToken = default);
}
