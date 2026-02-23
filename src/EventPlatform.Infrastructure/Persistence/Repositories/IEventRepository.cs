using EventPlatform.Domain.Events;

namespace EventPlatform.Infrastructure.Persistence.Repositories;

/// <summary>
/// Repository interface for persisting and querying event envelopes.
/// </summary>
public interface IEventRepository
{
    /// <summary>
    /// Inserts a new event envelope into the database.
    /// </summary>
    /// <param name="envelope">The event envelope to insert.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="envelope"/> is null.</exception>
    Task InsertAsync(EventEnvelope envelope, CancellationToken cancellationToken = default);

    /// <summary>
    /// Updates the status of an event by its ID.
    /// </summary>
    /// <param name="eventId">The event ID.</param>
    /// <param name="newStatus">The new event status.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task UpdateStatusAsync(Guid eventId, EventStatus newStatus, CancellationToken cancellationToken = default);

    /// <summary>
    /// Increments the attempt counter for an event by its ID.
    /// </summary>
    /// <param name="eventId">The event ID.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task IncrementAttemptsAsync(Guid eventId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves an event envelope by its ID.
    /// </summary>
    /// <param name="eventId">The event ID.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The event envelope, or null if not found.</returns>
    Task<EventEnvelope?> GetByIdAsync(Guid eventId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves retryable events with pagination metadata.
    /// </summary>
    /// <param name="now">The current time reference.</param>
    /// <param name="pageSize">The maximum number of events to return. Defaults to 1000.</param>
    /// <param name="skip">The number of events to skip. Defaults to 0.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A paginated retryable event result with metadata.</returns>
    Task<RetryableEventsPage> GetRetryableEventsAsync(
        DateTimeOffset now,
        int pageSize = 1000,
        int skip = 0,
        CancellationToken cancellationToken = default);
}
