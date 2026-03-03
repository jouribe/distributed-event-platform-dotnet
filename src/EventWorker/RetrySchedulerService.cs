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
        try
        {
            // Mark QUEUED first to prevent double-scheduling on the next poll cycle.
            // RequeueForRetryAsync clears next_attempt_at and last_error so the domain
            // invariant (NextAttemptAt must be null for non-FAILED_RETRYABLE status) is
            // satisfied and RehydrateFromPersistence does not throw on subsequent reads.
            // Trade-off: if we crash before publishing, the event is stuck in QUEUED
            // (acceptable MVP trade-off; a future improvement could use a second outbox).
            await eventRepository
                .RequeueForRetryAsync(envelope.Id, cancellationToken)
                .ConfigureAwait(false);

            await _eventPublisher
                .PublishAsync(envelope, cancellationToken)
                .ConfigureAwait(false);

            _logger.LogInformation(
                "Event {EventId} re-enqueued to stream {Stream} (attempt {Attempts}).",
                envelope.Id,
                _streamName,
                envelope.Attempts);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Failed to re-enqueue event {EventId}. It will be retried on the next scheduler cycle.",
                envelope.Id);
        }
    }
}
