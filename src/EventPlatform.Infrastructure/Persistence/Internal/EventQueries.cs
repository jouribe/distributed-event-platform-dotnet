namespace EventPlatform.Infrastructure.Persistence.Internal;

/// <summary>
/// Compiled SQL queries for event persistence operations.
/// </summary>
internal static class EventQueries
{
    /// <summary>
    /// SQL query to insert a new event into the events table.
    /// </summary>
    public const string InsertEvent = @"
        INSERT INTO events (
            id, tenant_id, event_type, occurred_at, received_at,
            payload, idempotency_key, correlation_id, status, attempts,
            next_attempt_at, last_error
        )
        VALUES (
            @Id, @TenantId, @EventType, @OccurredAt, @ReceivedAt,
            @Payload, @IdempotencyKey, @CorrelationId, @Status, @Attempts,
            @NextAttemptAt, @LastError
        )";

    /// <summary>
    /// SQL query to update only the status of an event.
    /// </summary>
    public const string UpdateStatus = @"
        UPDATE events
        SET status = @Status
        WHERE id = @EventId";

    /// <summary>
    /// SQL query to increment the attempts counter for an event.
    /// </summary>
    public const string IncrementAttempts = @"
        UPDATE events
        SET attempts = attempts + 1
        WHERE id = @EventId";

    /// <summary>
    /// SQL query to retrieve a single event by ID.
    /// </summary>
    public const string GetById = @"
        SELECT
            id, tenant_id, event_type, occurred_at, received_at,
            payload, idempotency_key, correlation_id, status, attempts,
            next_attempt_at, last_error
        FROM events
        WHERE id = @EventId
        LIMIT 1";

    /// <summary>
    /// SQL query to retrieve all events with FAILED_RETRYABLE status
    /// and next_attempt_at <= current time.
    /// </summary>
    public const string GetRetryableEvents = @"
        SELECT
            id, tenant_id, event_type, occurred_at, received_at,
            payload, idempotency_key, correlation_id, status, attempts,
            next_attempt_at, last_error
        FROM events
        WHERE status = @Status
          AND next_attempt_at <= @Now
        ORDER BY next_attempt_at ASC";
}
