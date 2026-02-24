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
    /// Inserts a new event envelope and its outbox record in a single atomic transaction.
    /// Uses the Outbox pattern to ensure reliable event publishing: if the API crashes after inserting
    /// the event but before publishing to Redis, the outbox will eventually publish the event.
    /// </summary>
    /// <param name="envelope">The event envelope to insert.</param>
    /// <param name="outboxEvent">The outbox event to insert (should reference the envelope).</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="envelope"/> or <paramref name="outboxEvent"/> is null.</exception>
    Task InsertWithOutboxAsync(EventEnvelope envelope, OutboxEvent outboxEvent, CancellationToken cancellationToken = default);

    /// <summary>
    /// Inserts multiple event envelopes into the database in a single batch operation.
    /// </summary>
    /// <param name="envelopes">The collection of event envelopes to insert.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A batch insert result containing success/conflict/error counts and per-event details.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="envelopes"/> is null.</exception>
    /// <remarks>
    /// This method provides high-throughput event ingestion by inserting multiple events in a single database round-trip.
    /// Partial failures are supported: events with idempotency conflicts are reported as conflicts without failing the batch.
    /// The order of details in the result matches the order of input envelopes.
    /// </remarks>
    Task<BatchInsertResult> BatchInsertAsync(IEnumerable<EventEnvelope> envelopes, CancellationToken cancellationToken = default);

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

    Task<EventEnvelope?> GetByTenantAndIdempotencyKeyAsync(
        string tenantId,
        string idempotencyKey,
        CancellationToken cancellationToken = default);

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

    /// <summary>
    /// Counts events by status, optionally filtered by tenant.
    /// </summary>
    /// <param name="status">The event status to count.</param>
    /// <param name="tenantId">Optional tenant filter.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The number of events matching the criteria.</returns>
    Task<long> GetCountAsync(
        EventStatus status,
        string? tenantId = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves all events with the specified correlation ID.
    /// </summary>
    /// <param name="correlationId">The correlation ID.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The events associated with the correlation ID.</returns>
    Task<IReadOnlyList<EventEnvelope>> GetByCorrelationIdAsync(
        Guid correlationId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves events for a tenant with pagination.
    /// </summary>
    /// <param name="tenantId">The tenant ID.</param>
    /// <param name="pageSize">The maximum number of events to return. Defaults to 100.</param>
    /// <param name="skip">The number of events to skip. Defaults to 0.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The events for the tenant.</returns>
    Task<IReadOnlyList<EventEnvelope>> GetByTenantIdAsync(
        string tenantId,
        int pageSize = 100,
        int skip = 0,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the single oldest retryable event eligible for processing.
    /// </summary>
    /// <param name="now">The current time reference.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The oldest retryable event, or null if none exist.</returns>
    Task<EventEnvelope?> GetOldestRetryableAsync(
        DateTimeOffset now,
        CancellationToken cancellationToken = default);
}
