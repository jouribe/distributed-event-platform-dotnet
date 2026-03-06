using System.Diagnostics.Metrics;
using EventPlatform.Domain.Events;
using EventPlatform.Application.Abstractions;
using EventPlatform.Infrastructure.Diagnostics;
using EventPlatform.Infrastructure.Persistence.Exceptions;
using EventPlatform.Infrastructure.Persistence.Repositories;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventPlatform.Infrastructure.Messaging;

/// <summary>
/// Background service that publishes unpublished events from the outbox table to Redis.
/// Implements at-least-once delivery semantics: events will be published eventually even if
/// the API crashes or Redis becomes temporarily unavailable.
/// </summary>
public sealed class OutboxPublisherService : BackgroundService
{
    private readonly IOutboxRepository _outboxRepository;
    private readonly IEventPublisher _eventPublisher;
    private readonly OutboxPublisherOptions _options;
    private readonly ILogger<OutboxPublisherService> _logger;

    public OutboxPublisherService(
        IOutboxRepository outboxRepository,
        IEventPublisher eventPublisher,
        IOptionsMonitor<OutboxPublisherOptions> optionsMonitor,
        ILogger<OutboxPublisherService> logger)
    {
        _outboxRepository = outboxRepository ?? throw new ArgumentNullException(nameof(outboxRepository));
        _eventPublisher = eventPublisher ?? throw new ArgumentNullException(nameof(eventPublisher));
        _options = optionsMonitor.CurrentValue ?? throw new ArgumentNullException(nameof(optionsMonitor));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Executes the background service: continuously publishes unpublished outbox events and cleans up old published ones.
    /// </summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("OutboxPublisherService started with poll interval {Interval}ms and max batch {BatchSize}",
            _options.PollIntervalMilliseconds, _options.MaxBatchSize);

        var cycleCount = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await PublishUnpublishedEventsAsync(stoppingToken);
                cycleCount++;

                // Periodic cleanup of old published entries (every 10 publication cycles)
                if (cycleCount % 10 == 0)
                {
                    await CleanupOldPublishedEventsAsync(stoppingToken);
                }

                var pending = await _outboxRepository.CountPendingAsync(stoppingToken);
                OutboxMetrics.UpdatePendingCount(pending);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error in OutboxPublisherService, will retry after delay");
            }

            try
            {
                await Task.Delay(_options.PollIntervalMilliseconds, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        _logger.LogInformation("OutboxPublisherService stopped");
    }

    private async Task PublishUnpublishedEventsAsync(CancellationToken cancellationToken)
    {
        var unpublished = await _outboxRepository.GetUnpublishedAsync(_options.MaxBatchSize, cancellationToken);
        if (unpublished.Count == 0)
            return;

        _logger.LogDebug("Publishing {Count} unpublished outbox events", unpublished.Count);

        foreach (var outboxEvent in unpublished)
        {
            using var scope = BeginPublishScope(outboxEvent);

            try
            {
                await _eventPublisher.PublishToStreamAsync(
                    outboxEvent.StreamName,
                    outboxEvent.Payload,
                    cancellationToken);

                var marked = await _outboxRepository.MarkPublishedAndQueueEventAsync(
                    outboxEvent.Id,
                    outboxEvent.EventId,
                    cancellationToken);

                if (!marked)
                {
                    _logger.LogDebug(
                        "Outbox already published or missing (outbox_id: {OutboxId}, event_id: {EventId})",
                        outboxEvent.Id,
                        outboxEvent.EventId);
                    continue;
                }

                _logger.LogDebug(
                    "Published outbox event (outbox_id: {OutboxId}, event_id: {EventId}, attempt: {Attempt})",
                    outboxEvent.Id,
                    outboxEvent.EventId,
                    outboxEvent.PublishAttempts + 1);
            }
            catch (EventRepositoryTransientException ex)
            {
                OutboxMetrics.PublishFailures.Add(1);

                _logger.LogWarning(
                    ex,
                    "Transient repository error while publishing outbox (outbox_id: {OutboxId}, event_id: {EventId}, attempt: {Attempt}, last_error: {LastError})",
                    outboxEvent.Id,
                    outboxEvent.EventId,
                    outboxEvent.PublishAttempts + 1,
                    ex.Message);

                await SafeRecordPublishAttemptAsync(outboxEvent, $"Transient DB error: {ex.Message}", cancellationToken);
            }
            catch (Exception ex)
            {
                OutboxMetrics.PublishFailures.Add(1);

                _logger.LogWarning(
                    ex,
                    "Failed to publish outbox event (outbox_id: {OutboxId}, event_id: {EventId}, attempt: {Attempt}, last_error: {LastError})",
                    outboxEvent.Id,
                    outboxEvent.EventId,
                    outboxEvent.PublishAttempts + 1,
                    ex.Message);

                await SafeRecordPublishAttemptAsync(outboxEvent, $"Publish error: {ex.Message}", cancellationToken);
            }
        }
    }

    private IDisposable BeginPublishScope(OutboxEvent outboxEvent)
    {
        var scopeState = new Dictionary<string, object?>
        {
            ["event_id"] = outboxEvent.EventId
        };

        var correlationId = CorrelationIdMetadata.TryReadFromOutboxPayload(outboxEvent.Payload);
        if (correlationId.HasValue)
        {
            scopeState["correlation_id"] = correlationId.Value;
        }

        return _logger.BeginScope(scopeState)!;
    }
    private async Task SafeRecordPublishAttemptAsync(OutboxEvent outboxEvent, string error, CancellationToken cancellationToken)
    {
        try
        {
            await _outboxRepository.RecordPublishAttemptAsync(outboxEvent.Id, error, cancellationToken);
        }
        catch (Exception recordEx)
        {
            _logger.LogError(
                recordEx,
                "Failed to record publish attempt (outbox_id: {OutboxId}, event_id: {EventId}, attempt: {Attempt}, last_error: {LastError})",
                outboxEvent.Id,
                outboxEvent.EventId,
                outboxEvent.PublishAttempts + 1,
                error);
        }
    }

    private async Task CleanupOldPublishedEventsAsync(CancellationToken cancellationToken)
    {
        try
        {
            var cutoffTime = DateTimeOffset.UtcNow.AddHours(-24);
            var deletedCount = await _outboxRepository.DeletePublishedAsync(cutoffTime, cancellationToken);

            if (deletedCount > 0)
            {
                _logger.LogInformation("Cleaned up {DeletedCount} old published outbox events", deletedCount);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to cleanup old published outbox events");
        }
    }
}

public static class OutboxMetrics
{
    private static readonly Meter Meter = new("EventPlatform.Outbox", "1.0.0");
    private static long _pendingCount;

    public static readonly Counter<long> PublishFailures =
        Meter.CreateCounter<long>("outbox_publish_failures_total");

    private static readonly ObservableGauge<long> PendingGauge =
        Meter.CreateObservableGauge<long>(
            "outbox_pending_count",
            () => new Measurement<long>(Interlocked.Read(ref _pendingCount)));

    public static void UpdatePendingCount(long pendingCount)
        => Interlocked.Exchange(ref _pendingCount, pendingCount);
}

/// <summary>
/// Configuration options for the OutboxPublisherService.
/// </summary>
public sealed class OutboxPublisherOptions
{
    public const int DefaultPollIntervalMilliseconds = 1000;
    public const int DefaultMaxBatchSize = 100;

    public int PollIntervalMilliseconds { get; set; } = DefaultPollIntervalMilliseconds;
    public int MaxBatchSize { get; set; } = DefaultMaxBatchSize;
}
