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
            next_attempt_at, last_error, source
        )
        VALUES (
            @Id, @TenantId, @EventType, @OccurredAt, @ReceivedAt,
            @Payload, @IdempotencyKey, @CorrelationId, @Status, @Attempts,
            @NextAttemptAt, @LastError, @Source
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
            next_attempt_at, last_error, source
        FROM events
        WHERE id = @EventId
        LIMIT 1";

    public const string GetByTenantAndIdempotencyKey = @"
        SELECT
            id, tenant_id, event_type, occurred_at, received_at,
            payload, idempotency_key, correlation_id, status, attempts,
            next_attempt_at, last_error, source
        FROM events
        WHERE tenant_id = @TenantId
          AND idempotency_key = @IdempotencyKey
        ORDER BY received_at ASC
        LIMIT 1";

    /// <summary>
    /// SQL query to retrieve paged events with FAILED_RETRYABLE status
    /// and next_attempt_at <= current time.
    /// </summary>
    public const string GetRetryableEventsPage = @"
        SELECT
            id, tenant_id, event_type, occurred_at, received_at,
            payload, idempotency_key, correlation_id, status, attempts,
            next_attempt_at, last_error, source
        FROM events
        WHERE status = @Status
          AND next_attempt_at <= @Now
        ORDER BY next_attempt_at ASC, id ASC
        LIMIT @Take
        OFFSET @Skip";

    /// <summary>
    /// SQL query to count events by status with optional tenant filter.
    /// </summary>
    public const string GetCountByStatus = @"
        SELECT COUNT(*)
        FROM events
        WHERE status = @Status
          AND (@TenantId IS NULL OR tenant_id = @TenantId)";

    /// <summary>
    /// SQL query to retrieve events by correlation ID ordered chronologically.
    /// </summary>
    public const string GetByCorrelationId = @"
        SELECT
            id, tenant_id, event_type, occurred_at, received_at,
            payload, idempotency_key, correlation_id, status, attempts,
            next_attempt_at, last_error, source
        FROM events
        WHERE correlation_id = @CorrelationId
        ORDER BY received_at ASC, id ASC";

    /// <summary>
    /// SQL query to retrieve paged events for a tenant ordered by latest first.
    /// </summary>
    public const string GetByTenantIdPage = @"
        SELECT
            id, tenant_id, event_type, occurred_at, received_at,
            payload, idempotency_key, correlation_id, status, attempts,
            next_attempt_at, last_error, source
        FROM events
        WHERE tenant_id = @TenantId
        ORDER BY received_at DESC, id DESC
        LIMIT @Take
        OFFSET @Skip";

    /// <summary>
    /// SQL query to retrieve the oldest retryable event eligible for processing.
    /// </summary>
    public const string GetOldestRetryable = @"
        SELECT
            id, tenant_id, event_type, occurred_at, received_at,
            payload, idempotency_key, correlation_id, status, attempts,
            next_attempt_at, last_error, source
        FROM events
        WHERE status = @Status
          AND next_attempt_at <= @Now
        ORDER BY next_attempt_at ASC, id ASC
        LIMIT 1";

    /// <summary>
    /// SQL query for batch insert with conflict detection and detailed results.
    /// Uses PostgreSQL UNNEST for efficient multi-row insertion.
    /// Returns the outcome for each input event (Success or Conflict).
    /// </summary>
    public const string BatchInsertWithResults = @"
        WITH input_data AS (
            SELECT
                t.*,
                row_number() OVER () as input_order
            FROM UNNEST(
                @Ids::uuid[],
                @TenantIds::text[],
                @EventTypes::text[],
                @OccurredAts::timestamptz[],
                @ReceivedAts::timestamptz[],
                @Payloads::text[],
                @IdempotencyKeys::text[],
                @CorrelationIds::uuid[],
                @Statuses::text[],
                @AttemptsArray::integer[],
                @NextAttemptAts::timestamptz[],
                @LastErrors::text[],
                @Sources::text[]
            ) AS t(
                id, tenant_id, event_type, occurred_at, received_at,
                payload, idempotency_key, correlation_id, status, attempts,
                next_attempt_at, last_error, source
            )
        ),
        normalized_data AS (
            SELECT
                id, tenant_id, event_type, occurred_at, received_at,
                payload,
                NULLIF(idempotency_key, '') as idempotency_key,
                correlation_id, status, attempts,
                NULLIF(next_attempt_at, '-infinity'::timestamptz) as next_attempt_at,
                NULLIF(last_error, '') as last_error,
                source,
                input_order
            FROM input_data
        ),
        inserted AS (
            INSERT INTO events (
                id, tenant_id, event_type, occurred_at, received_at,
                payload, idempotency_key, correlation_id, status, attempts,
                next_attempt_at, last_error, source
            )
            SELECT
                id, tenant_id, event_type, occurred_at, received_at,
                payload::jsonb, idempotency_key, correlation_id, status, attempts,
                next_attempt_at, last_error, source
            FROM normalized_data
            ON CONFLICT (tenant_id, idempotency_key)
            WHERE idempotency_key IS NOT NULL
            DO NOTHING
            RETURNING id
        )
        SELECT
            n.id,
            CASE
                WHEN ins.id IS NOT NULL THEN 'Success'
                WHEN n.idempotency_key IS NOT NULL THEN 'Conflict'
                ELSE 'Success'
            END as outcome
        FROM normalized_data n
        LEFT JOIN inserted ins ON n.id = ins.id
        ORDER BY n.input_order;";
}
