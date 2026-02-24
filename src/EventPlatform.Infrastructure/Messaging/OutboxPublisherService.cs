using EventPlatform.Application.Abstractions;
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

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await PublishUnpublishedEventsAsync(stoppingToken);

                // Periodic cleanup of old published entries (every 10 publication cycles)
                if (DateTimeOffset.UtcNow.Ticks % 10 == 0)
                {
                    await CleanupOldPublishedEventsAsync(stoppingToken);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected when service is shutting down
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error in OutboxPublisherService, will retry after delay");
            }

            // Wait before next poll
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
            try
            {
                // The outbox stores the payload as JsonDocument, we pass it directly to the publisher
                await _eventPublisher.PublishToStreamAsync(
                    outboxEvent.StreamName,
                    outboxEvent.Payload,
                    cancellationToken);

                // Mark as published
                await _outboxRepository.MarkPublishedAsync(outboxEvent.Id, cancellationToken);

                _logger.LogDebug("Published outbox event {OutboxId} (event {EventId})",
                    outboxEvent.Id, outboxEvent.EventId);
            }
            catch (EventRepositoryTransientException ex)
            {
                // Transient database error - record attempt and retry later
                _logger.LogWarning(ex, "Transient error recording publish attempt for outbox {OutboxId}", outboxEvent.Id);

                try
                {
                    await _outboxRepository.RecordPublishAttemptAsync(
                        outboxEvent.Id,
                        $"Transient DB error: {ex.Message}",
                        cancellationToken);
                }
                catch (Exception recordEx)
                {
                    _logger.LogError(recordEx, "Failed to record publish attempt for outbox {OutboxId}", outboxEvent.Id);
                }
            }
            catch (Exception ex)
            {
                // Publish failed (network, Redis down, etc)
                _logger.LogWarning(ex, "Failed to publish outbox event {OutboxId}, attempt {Attempt}",
                    outboxEvent.Id, outboxEvent.PublishAttempts + 1);

                try
                {
                    await _outboxRepository.RecordPublishAttemptAsync(
                        outboxEvent.Id,
                        $"Publish error: {ex.Message}",
                        cancellationToken);
                }
                catch (Exception recordEx)
                {
                    _logger.LogError(recordEx, "Failed to record publish attempt for outbox {OutboxId}", outboxEvent.Id);
                }
            }
        }
    }

    private async Task CleanupOldPublishedEventsAsync(CancellationToken cancellationToken)
    {
        try
        {
            // Delete published events older than 24 hours
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
