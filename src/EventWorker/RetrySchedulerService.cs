using EventPlatform.Application.Abstractions;
using EventPlatform.Domain.Events;
using EventPlatform.Infrastructure.Persistence.Repositories;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace EventWorker;

public class RetrySchedulerService : BackgroundService
{
    private readonly ILogger<RetrySchedulerService> _logger;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IEventPublisher _eventPublisher;
    private readonly RetryOptions _retryOptions;
    private readonly string _streamName;

    public RetrySchedulerService(
        ILogger<RetrySchedulerService> logger,
        IServiceScopeFactory scopeFactory,
        IEventPublisher eventPublisher,
        IOptions<RetryOptions> retryOptions,
        IOptions<RedisConsumerOptions> consumerOptions)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));
        _eventPublisher = eventPublisher ?? throw new ArgumentNullException(nameof(eventPublisher));
        _retryOptions = retryOptions?.Value ?? throw new ArgumentNullException(nameof(retryOptions));
        _streamName = consumerOptions?.Value?.StreamName ?? throw new ArgumentNullException(nameof(consumerOptions));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "RetrySchedulerService started. Polling every {Interval}s for FAILED_RETRYABLE events on stream {Stream}.",
            _retryOptions.PollingIntervalSeconds,
            _streamName);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ProcessRetryBatchAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "RetrySchedulerService encountered an unexpected error during polling.");
            }

            try
            {
                await Task.Delay(
                    TimeSpan.FromSeconds(_retryOptions.PollingIntervalSeconds),
                    stoppingToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }

        _logger.LogInformation("RetrySchedulerService stopped.");
    }

    private async Task ProcessRetryBatchAsync(CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var eventRepository = scope.ServiceProvider.GetRequiredService<IEventRepository>();

        var page = await eventRepository
            .GetRetryableEventsAsync(DateTimeOffset.UtcNow, pageSize: 100, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        if (page.Items.Count == 0)
            return;

        _logger.LogInformation(
            "RetrySchedulerService found {Count} event(s) eligible for re-enqueue.",
            page.Items.Count);

        foreach (var envelope in page.Items)
        {
            await RequeueEventAsync(eventRepository, envelope, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task RequeueEventAsync(
        IEventRepository eventRepository,
        EventEnvelope envelope,
        CancellationToken cancellationToken)
    {
        // Phase 1 — mark QUEUED in the DB first (prevents double-scheduling).
        // RequeueForRetryAsync also clears next_attempt_at / last_error to satisfy
        // the domain invariant enforced by RehydrateFromPersistence.
        // If this fails the row is still FAILED_RETRYABLE, so it is safe to abort.
        try
        {
            await eventRepository
                .RequeueForRetryAsync(envelope.Id, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Failed to mark event {EventId} as QUEUED. It remains FAILED_RETRYABLE and will be retried on the next scheduler cycle.",
                envelope.Id);
            return;
        }

        // Phase 2 — publish to the Redis stream.
        // If this fails the row is already QUEUED but has no stream entry, so the
        // event would be permanently stranded (GetRetryableEventsAsync only selects
        // FAILED_RETRYABLE rows). Restore to FAILED_RETRYABLE with a fresh backoff
        // so the scheduler picks it up again. Use CancellationToken.None so the
        // restore always runs, even when the cancellation token triggered the failure.
        try
        {
            await _eventPublisher
                .PublishAsync(envelope, cancellationToken)
                .ConfigureAwait(false);

            _logger.LogInformation(
                "Event {EventId} re-enqueued to stream {Stream} (attempt {Attempts}).",
                envelope.Id,
                _streamName,
                envelope.Attempts);
        }
        catch (Exception publishEx)
        {
            _logger.LogWarning(
                publishEx,
                "Failed to publish event {EventId} to stream after marking QUEUED. Restoring to FAILED_RETRYABLE.",
                envelope.Id);

            var delaySec = Math.Min(Math.Pow(2, envelope.Attempts), _retryOptions.MaxBackoffSeconds);
            var nextAttemptAt = DateTimeOffset.UtcNow.AddSeconds(delaySec);

            try
            {
                await eventRepository
                    .MarkRetryableFailureAsync(envelope.Id, nextAttemptAt, publishEx.Message, CancellationToken.None)
                    .ConfigureAwait(false);
            }
            catch (Exception restoreEx)
            {
                _logger.LogError(
                    restoreEx,
                    "Failed to restore event {EventId} to FAILED_RETRYABLE after publish failure. Event may be stranded as QUEUED.",
                    envelope.Id);
            }
        }
    }
}
