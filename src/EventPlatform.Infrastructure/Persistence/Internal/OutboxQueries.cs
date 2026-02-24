namespace EventPlatform.Infrastructure.Persistence.Internal;

/// <summary>
/// Compiled SQL queries for outbox event persistence operations.
/// </summary>
internal static class OutboxQueries
{
    /// <summary>
    /// SQL query to insert a new outbox event.
    /// </summary>
    public const string InsertOutboxEvent = @"
        INSERT INTO event_platform.outbox_events (
            id, event_id, stream_name, payload, created_at, published_at,
            publish_attempts, last_error
        )
        VALUES (
            @Id, @EventId, @StreamName, @Payload, @CreatedAt, @PublishedAt,
            @PublishAttempts, @LastError
        )";

    /// <summary>
    /// SQL query to retrieve unpublished outbox events ordered by creation time.
    /// </summary>
    public const string GetUnpublished = @"
        SELECT
            id, event_id, stream_name, payload, created_at, published_at,
            publish_attempts, last_error
        FROM event_platform.outbox_events
        WHERE published_at IS NULL
        ORDER BY created_at ASC
        LIMIT @Limit";

    /// <summary>
    /// SQL query to mark an outbox event as published and clear errors.
    /// </summary>
    public const string MarkPublished = @"
        UPDATE event_platform.outbox_events
        SET published_at = @PublishedAt, last_error = NULL
        WHERE id = @Id";

    /// <summary>
    /// SQL query to record a failed publish attempt.
    /// </summary>
    public const string RecordPublishAttempt = @"
        UPDATE event_platform.outbox_events
        SET publish_attempts = publish_attempts + 1, last_error = @Error
        WHERE id = @Id";

    /// <summary>
    /// SQL query to delete published outbox events older than a specified time.
    /// </summary>
    public const string DeletePublished = @"
        DELETE FROM event_platform.outbox_events
        WHERE published_at IS NOT NULL AND published_at < @OlderThan";
}
