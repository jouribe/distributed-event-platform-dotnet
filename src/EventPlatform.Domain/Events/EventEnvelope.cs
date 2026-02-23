using System.Text.Json;

namespace EventPlatform.Domain.Events;


public sealed record EventEnvelope
{
    public Guid Id { get; init; }
    public string EventType { get; init; }
    public DateTimeOffset OccurredAt { get; init; }
    public DateTimeOffset ReceivedAt { get; init; }
    public string Source { get; init; }
    public string TenantId { get; init; }
    public string? IdempotencyKey { get; init; }
    public Guid CorrelationId { get; init; }
    public JsonDocument Payload { get; init; }

    public EventStatus Status { get; private init; }
    public int Attempts { get; private init; }
    public DateTimeOffset? NextAttemptAt { get; private init; }
    public string? LastError { get; private init; }

    //private EventEnvelope() { } // optional (ORM)

    /// <summary>
    /// Initializes a validated event envelope instance. Use <see cref="CreateNew"/> for new events.
    /// </summary>
    private EventEnvelope(
        Guid id,
        string eventType,
        DateTimeOffset occurredAt,
        DateTimeOffset receivedAt,
        string source,
        string tenantId,
        string? idempotencyKey,
        Guid correlationId,
        JsonDocument payload,
        EventStatus status,
        int attempts,
        DateTimeOffset? nextAttemptAt,
        string? lastError)
    {
        if (id == Guid.Empty) throw new ArgumentException("Event id cannot be empty", nameof(id));
        if (string.IsNullOrWhiteSpace(eventType)) throw new ArgumentException("EventType is required", nameof(eventType));
        if (string.IsNullOrWhiteSpace(source)) throw new ArgumentException("Source is required", nameof(source));
        if (string.IsNullOrWhiteSpace(tenantId)) throw new ArgumentException("TenantId is required", nameof(tenantId));
        if (correlationId == Guid.Empty) throw new ArgumentException("CorrelationId cannot be empty", nameof(correlationId));
        if (payload is null) throw new ArgumentNullException(nameof(payload));
        if (occurredAt > receivedAt) throw new ArgumentException("OccurredAt cannot be later than ReceivedAt");
        if (attempts < 0) throw new ArgumentOutOfRangeException(nameof(attempts), "Attempts cannot be negative");

        if (status == EventStatus.FAILED_RETRYABLE && nextAttemptAt is null)
            throw new ArgumentException("NextAttemptAt is required when status is FAILED_RETRYABLE", nameof(nextAttemptAt));

        if (status != EventStatus.FAILED_RETRYABLE && nextAttemptAt is not null)
            throw new ArgumentException("NextAttemptAt must be null unless status is FAILED_RETRYABLE", nameof(nextAttemptAt));

        if (status == EventStatus.SUCCEEDED && !string.IsNullOrWhiteSpace(lastError))
            throw new ArgumentException("LastError must be null when status is SUCCEEDED", nameof(lastError));

        Id = id;
        EventType = eventType;
        OccurredAt = occurredAt;
        ReceivedAt = receivedAt;
        Source = source;
        TenantId = tenantId;
        IdempotencyKey = idempotencyKey;
        CorrelationId = correlationId;
        Payload = payload;

        Status = status;
        Attempts = attempts;
        NextAttemptAt = nextAttemptAt;
        LastError = lastError;
    }

    /// <summary>
    /// Creates a new envelope in <see cref="EventStatus.RECEIVED"/> with attempts initialized to 0.
    /// </summary>
    public static EventEnvelope CreateNew(
        Guid id,
        string eventType,
        DateTimeOffset occurredAt,
        string source,
        string tenantId,
        string? idempotencyKey,
        Guid correlationId,
        JsonDocument payload)
    {
        return new EventEnvelope(
            id: id,
            eventType: eventType,
            occurredAt: occurredAt,
            receivedAt: DateTimeOffset.UtcNow,
            source: source,
            tenantId: tenantId,
            idempotencyKey: idempotencyKey,
            correlationId: correlationId,
            payload: payload,
            status: EventStatus.RECEIVED,
            attempts: 0,
            nextAttemptAt: null,
            lastError: null);
    }

    /// <summary>
    /// Performs a lifecycle-validated state transition.
    /// </summary>
    private EventEnvelope TransitionTo(EventStatus newStatus)
    {
        EventLifecycle.EnsureTransition(Status, newStatus);
        return this with { Status = newStatus };
    }

    /// <summary>
    /// Marks the event as <see cref="EventStatus.QUEUED"/>.
    /// </summary>
    public EventEnvelope MarkQueued() => TransitionTo(EventStatus.QUEUED);

    /// <summary>
    /// Marks the event as <see cref="EventStatus.PROCESSING"/> and increments attempts.
    /// </summary>
    public EventEnvelope MarkProcessing() =>
        TransitionTo(EventStatus.PROCESSING) with
        {
            Attempts = Attempts + 1,
            LastError = null,
            NextAttemptAt = null
        };

    /// <summary>
    /// Marks the event as <see cref="EventStatus.SUCCEEDED"/> and clears retry/error metadata.
    /// </summary>
    public EventEnvelope MarkSucceeded() =>
        TransitionTo(EventStatus.SUCCEEDED) with
        {
            LastError = null,
            NextAttemptAt = null
        };

    /// <summary>
    /// Marks the event as <see cref="EventStatus.FAILED_RETRYABLE"/> with error details and next retry time.
    /// </summary>
    public EventEnvelope MarkRetryableFailure(string error, DateTimeOffset nextAttemptAt)
    {
        if (string.IsNullOrWhiteSpace(error))
            error = "Unknown error";

        if (nextAttemptAt <= DateTimeOffset.UtcNow)
            throw new ArgumentException("NextAttemptAt must be in the future", nameof(nextAttemptAt));

        // PROCESSING -> FAILED_RETRYABLE
        var failed = TransitionTo(EventStatus.FAILED_RETRYABLE);

        return failed with
        {
            LastError = error,
            NextAttemptAt = nextAttemptAt
        };
    }

    /// <summary>
    /// Re-queues a retryable failure and clears the scheduled retry timestamp.
    /// </summary>
    public EventEnvelope RequeueAfterRetry()
    {
        // FAILED_RETRYABLE -> QUEUED
        var queued = TransitionTo(EventStatus.QUEUED);

        return queued with
        {
            NextAttemptAt = null
        };
    }

    /// <summary>
    /// Marks the event as <see cref="EventStatus.FAILED_TERMINAL"/> and stores terminal error details.
    /// </summary>
    public EventEnvelope MarkTerminalFailure(string error)
    {
        if (string.IsNullOrWhiteSpace(error))
            error = "Unknown error";

        // PROCESSING -> FAILED_TERMINAL
        var failed = TransitionTo(EventStatus.FAILED_TERMINAL);

        return failed with
        {
            LastError = error,
            NextAttemptAt = null
        };
    }
}
