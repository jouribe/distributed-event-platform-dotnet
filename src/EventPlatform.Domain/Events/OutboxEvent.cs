using System.Text.Json;

namespace EventPlatform.Domain.Events;

/// <summary>
/// Represents an unpublished event in the outbox, ensuring reliable publishing to Redis.
/// Uses the Outbox pattern to guarantee events are not lost if the API crashes after publishing but before status update.
/// </summary>
public sealed record OutboxEvent
{
    public Guid Id { get; init; }
    public Guid EventId { get; init; }
    public string StreamName { get; init; }
    public JsonDocument Payload { get; init; }
    public DateTimeOffset CreatedAt { get; init; }
    public DateTimeOffset? PublishedAt { get; init; }
    public int PublishAttempts { get; init; }
    public string? LastError { get; init; }

    public OutboxEvent(
        Guid id,
        Guid eventId,
        string streamName,
        JsonDocument payload,
        DateTimeOffset createdAt,
        DateTimeOffset? publishedAt,
        int publishAttempts,
        string? lastError)
    {
        if (id == Guid.Empty) throw new ArgumentException("Id cannot be empty", nameof(id));
        if (eventId == Guid.Empty) throw new ArgumentException("EventId cannot be empty", nameof(eventId));
        if (string.IsNullOrWhiteSpace(streamName)) throw new ArgumentException("StreamName is required", nameof(streamName));
        if (payload is null) throw new ArgumentNullException(nameof(payload));
        if (publishAttempts < 0) throw new ArgumentOutOfRangeException(nameof(publishAttempts), "PublishAttempts cannot be negative");

        Id = id;
        EventId = eventId;
        StreamName = streamName;
        Payload = payload;
        CreatedAt = createdAt;
        PublishedAt = publishedAt;
        PublishAttempts = publishAttempts;
        LastError = lastError;
    }

    /// <summary>
    /// Creates a new outbox entry for an event envelope.
    /// </summary>
    /// <param name="id">The outbox id (new unique id).</param>
    /// <param name="eventId">The event id this outbox entry references.</param>
    /// <param name="streamName">The Redis stream name.</param>
    /// <param name="payload">The event payload as JsonDocument.</param>
    /// <returns>A new OutboxEvent instance.</returns>
    public static OutboxEvent CreateNew(
        Guid id,
        Guid eventId,
        string streamName,
        JsonDocument payload)
    {
        return new OutboxEvent(
            id: id,
            eventId: eventId,
            streamName: streamName,
            payload: payload,
            createdAt: DateTimeOffset.UtcNow,
            publishedAt: null,
            publishAttempts: 0,
            lastError: null);
    }

    /// <summary>
    /// Records a publish attempt (whether it succeeded or failed).
    /// </summary>
    public OutboxEvent WithPublishAttempt(int newAttemptCount, string? error = null)
    {
        return this with
        {
            PublishAttempts = newAttemptCount,
            LastError = error
        };
    }

    /// <summary>
    /// Marks the outbox entry as published (no further attempts needed).
    /// </summary>
    public OutboxEvent MarkPublished()
    {
        return this with
        {
            PublishedAt = DateTimeOffset.UtcNow,
            LastError = null
        };
    }

    /// <summary>
    /// Indicates whether this outbox entry has been published.
    /// </summary>
    public bool IsPublished => PublishedAt.HasValue;
}
