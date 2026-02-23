using System.Text.Json;
using Dapper;
using EventPlatform.Domain.Events;
using EventPlatform.Infrastructure.Persistence.DataAccess;
using EventPlatform.Infrastructure.Persistence.Internal;

namespace EventPlatform.Infrastructure.Persistence.Repositories;

/// <summary>
/// Implementation of <see cref="IEventRepository"/> using Dapper for PostgreSQL.
/// </summary>
public sealed class EventRepository : IEventRepository
{
    private readonly IDbConnectionFactory _connectionFactory;

    /// <summary>
    /// Initializes a new instance of the <see cref="EventRepository"/> class.
    /// </summary>
    /// <param name="connectionFactory">The database connection factory.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="connectionFactory"/> is null.</exception>
    public EventRepository(IDbConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
    }

    /// <summary>
    /// Inserts a new event envelope into the database.
    /// </summary>
    /// <param name="envelope">The event envelope to insert.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="envelope"/> is null.</exception>
    public async Task InsertAsync(EventEnvelope envelope, CancellationToken cancellationToken = default)
    {
        if (envelope == null)
            throw new ArgumentNullException(nameof(envelope));

        using var connection = _connectionFactory.CreateConnection();
        connection.Open();

        var parameters = new
        {
            envelope.Id,
            envelope.TenantId,
            envelope.EventType,
            envelope.OccurredAt,
            envelope.ReceivedAt,
            Payload = envelope.Payload.RootElement.ToString(), // Serialize JsonDocument to string
            envelope.IdempotencyKey,
            envelope.CorrelationId,
            Status = envelope.Status.ToString(),
            envelope.Attempts,
            envelope.NextAttemptAt,
            envelope.LastError
        };

        await connection.ExecuteAsync(EventQueries.InsertEvent, parameters, commandTimeout: 30);
    }

    /// <summary>
    /// Updates the status of an event by its ID.
    /// </summary>
    /// <param name="eventId">The event ID.</param>
    /// <param name="newStatus">The new event status.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task UpdateStatusAsync(Guid eventId, EventStatus newStatus, CancellationToken cancellationToken = default)
    {
        using var connection = _connectionFactory.CreateConnection();
        connection.Open();

        var parameters = new
        {
            Status = newStatus.ToString(),
            EventId = eventId
        };

        await connection.ExecuteAsync(EventQueries.UpdateStatus, parameters, commandTimeout: 30);
    }

    /// <summary>
    /// Increments the attempt counter for an event by its ID.
    /// </summary>
    /// <param name="eventId">The event ID.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task IncrementAttemptsAsync(Guid eventId, CancellationToken cancellationToken = default)
    {
        using var connection = _connectionFactory.CreateConnection();
        connection.Open();

        var parameters = new { EventId = eventId };

        await connection.ExecuteAsync(EventQueries.IncrementAttempts, parameters, commandTimeout: 30);
    }

    /// <summary>
    /// Retrieves an event envelope by its ID.
    /// </summary>
    /// <param name="eventId">The event ID.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The event envelope, or null if not found.</returns>
    public async Task<EventEnvelope?> GetByIdAsync(Guid eventId, CancellationToken cancellationToken = default)
    {
        using var connection = _connectionFactory.CreateConnection();
        connection.Open();

        var parameters = new { EventId = eventId };

        var result = await connection.QuerySingleOrDefaultAsync<EventEnvelopeDto>(
            EventQueries.GetById,
            parameters,
            commandTimeout: 30);

        return result == null ? null : result.ToEventEnvelope();
    }

    /// <summary>
    /// Retrieves all events with retryable status and a scheduled retry time <= the current time.
    /// </summary>
    /// <param name="now">The current time reference.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A collection of retryable event envelopes.</returns>
    public async Task<IEnumerable<EventEnvelope>> GetRetryableEventsAsync(DateTimeOffset now, CancellationToken cancellationToken = default)
    {
        using var connection = _connectionFactory.CreateConnection();
        connection.Open();

        var parameters = new
        {
            Status = EventStatus.FAILED_RETRYABLE.ToString(),
            Now = now
        };

        var results = await connection.QueryAsync<EventEnvelopeDto>(
            EventQueries.GetRetryableEvents,
            parameters,
            commandTimeout: 30);

        return results.Select(dto => dto.ToEventEnvelope()).ToList();
    }

    /// <summary>
    /// Data transfer object for mapping database rows to EventEnvelope entities.
    /// </summary>
    private sealed class EventEnvelopeDto
    {
        public Guid Id { get; set; }
        public string EventType { get; set; } = null!;
        public DateTimeOffset OccurredAt { get; set; }
        public DateTimeOffset ReceivedAt { get; set; }
        public string Payload { get; set; } = null!;
        public string? IdempotencyKey { get; set; }
        public string TenantId { get; set; } = null!;
        public Guid CorrelationId { get; set; }
        public string Status { get; set; } = null!;
        public int Attempts { get; set; }
        public DateTimeOffset? NextAttemptAt { get; set; }
        public string? LastError { get; set; }

        /// <summary>
        /// Converts this DTO to an EventEnvelope domain entity.
        /// </summary>
        /// <returns>An EventEnvelope instance.</returns>
        public EventEnvelope ToEventEnvelope()
        {
            var payloadDocument = JsonDocument.Parse(Payload);
            var status = Enum.Parse<EventStatus>(Status);

            return EventEnvelope.RehydrateFromPersistence(
                id: Id,
                eventType: EventType,
                occurredAt: OccurredAt,
                receivedAt: ReceivedAt,
                source: "PERSISTED", // Note: 'source' is not persisted; we use a marker value
                tenantId: TenantId,
                idempotencyKey: IdempotencyKey,
                correlationId: CorrelationId,
                payload: payloadDocument,
                status: status,
                attempts: Attempts,
                nextAttemptAt: NextAttemptAt,
                lastError: LastError);
        }
    }
}
