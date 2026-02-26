using EventPlatform.Domain.Events;
using EventPlatform.Infrastructure.Persistence.Repositories;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using System.Diagnostics;
using System.Text.Json;

namespace EventWorker;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IConnectionMultiplexer _connectionMultiplexer;
    private readonly IRedisConsumerGroupBootstrapper _bootstrapper;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly RedisConsumerOptions _options;

    public Worker(
        ILogger<Worker> logger,
        IConnectionMultiplexer connectionMultiplexer,
        IRedisConsumerGroupBootstrapper bootstrapper,
        IServiceScopeFactory scopeFactory,
        IOptions<RedisConsumerOptions> options)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _connectionMultiplexer = connectionMultiplexer ?? throw new ArgumentNullException(nameof(connectionMultiplexer));
        _bootstrapper = bootstrapper ?? throw new ArgumentNullException(nameof(bootstrapper));
        _scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));

        if (string.IsNullOrWhiteSpace(_options.StreamName))
            throw new ArgumentException("StreamName cannot be null or empty.", nameof(options));

        if (string.IsNullOrWhiteSpace(_options.GroupName))
            throw new ArgumentException("GroupName cannot be null or empty.", nameof(options));

        if (string.IsNullOrWhiteSpace(_options.ConsumerName))
            throw new ArgumentException("ConsumerName cannot be null or empty.", nameof(options));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var database = _connectionMultiplexer.GetDatabase();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await _bootstrapper.EnsureConsumerGroupAsync(stoppingToken).ConfigureAwait(false);
                await DrainPendingMessagesOnStartupAsync(database, stoppingToken).ConfigureAwait(false);
                await ReclaimPendingMessagesAsync(database, "reclaim-startup", _options.DrainOnStartupMaxBatches, stoppingToken).ConfigureAwait(false);
                break;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during worker startup recovery phase.");

                if (!await DelayUnlessCancelledAsync(_options.ErrorDelayMilliseconds, stoppingToken).ConfigureAwait(false))
                {
                    return;
                }
            }
        }

        if (stoppingToken.IsCancellationRequested)
        {
            return;
        }

        var nextReclaimAt = DateTimeOffset.UtcNow.AddMilliseconds(_options.ReclaimIntervalMilliseconds);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (DateTimeOffset.UtcNow >= nextReclaimAt)
                {
                    await ReclaimPendingMessagesAsync(database, "reclaim-maintenance", 1, stoppingToken).ConfigureAwait(false);
                    nextReclaimAt = DateTimeOffset.UtcNow.AddMilliseconds(_options.ReclaimIntervalMilliseconds);
                }

                var entries = await database
                    .StreamReadGroupAsync(
                        _options.StreamName,
                        _options.GroupName,
                        _options.ConsumerName,
                        ">",
                        _options.EffectiveReadBatchSize,
                        noAck: false)
                    .ConfigureAwait(false);

                if (entries.Length == 0)
                {
                    await Task.Delay(_options.EffectiveEmptyReadDelayMilliseconds, stoppingToken).ConfigureAwait(false);
                    continue;
                }

                await ProcessEntriesAsync(database, entries, "read-new", stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while consuming Redis stream entries.");

                if (!await DelayUnlessCancelledAsync(_options.ErrorDelayMilliseconds, stoppingToken).ConfigureAwait(false))
                {
                    return;
                }
            }
        }
    }

    private static async Task<bool> DelayUnlessCancelledAsync(int delayMilliseconds, CancellationToken stoppingToken)
    {
        try
        {
            await Task.Delay(delayMilliseconds, stoppingToken).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            return false;
        }
    }

    private async Task DrainPendingMessagesOnStartupAsync(IDatabase database, CancellationToken stoppingToken)
    {
        var maxBatches = _options.DrainOnStartupMaxBatches;
        var maxMessages = _options.DrainOnStartupMaxMessages;

        var drainedTotal = 0;
        var stopwatch = Stopwatch.StartNew();

        for (var batchNumber = 1; batchNumber <= maxBatches && drainedTotal < maxMessages; batchNumber++)
        {
            stoppingToken.ThrowIfCancellationRequested();

            var remaining = maxMessages - drainedTotal;
            var batchSize = Math.Min(_options.EffectiveReadBatchSize, remaining);
            if (batchSize <= 0)
            {
                break;
            }

            var entries = await database
                .StreamReadGroupAsync(
                    _options.StreamName,
                    _options.GroupName,
                    _options.ConsumerName,
                    "0",
                    batchSize,
                    noAck: false)
                .ConfigureAwait(false);

            if (entries.Length == 0)
            {
                break;
            }

            var acked = await ProcessEntriesAsync(database, entries, "drain-pending", stoppingToken).ConfigureAwait(false);
            drainedTotal += acked;

            _logger.LogInformation(
                "Worker batch processed (phase: {Phase}, stream: {Stream}, group: {Group}, consumer: {Consumer}, batchSize: {BatchSize}, ackedCount: {AckedCount}, reclaimedCount: {ReclaimedCount}, durationMs: {DurationMs})",
                "drain-pending",
                _options.StreamName,
                _options.GroupName,
                _options.ConsumerName,
                entries.Length,
                acked,
                0,
                stopwatch.ElapsedMilliseconds);
        }

        _logger.LogInformation(
            "Worker startup drain completed (phase: {Phase}, stream: {Stream}, group: {Group}, consumer: {Consumer}, ackedCount: {AckedCount}, durationMs: {DurationMs})",
            "drain-pending",
            _options.StreamName,
            _options.GroupName,
            _options.ConsumerName,
            drainedTotal,
            stopwatch.ElapsedMilliseconds);
    }

    private async Task ReclaimPendingMessagesAsync(
        IDatabase database,
        string phase,
        int maxBatches,
        CancellationToken stoppingToken)
    {
        var stopwatch = Stopwatch.StartNew();
        var reclaimedTotal = 0;
        var ackedTotal = 0;
        var cursor = (RedisValue)"0-0";

        for (var batchNumber = 1; batchNumber <= maxBatches; batchNumber++)
        {
            stoppingToken.ThrowIfCancellationRequested();

            StreamEntry[] entries;
            RedisValue nextCursor;

            try
            {
                var result = await database
                    .StreamAutoClaimAsync(
                        _options.StreamName,
                        _options.GroupName,
                        _options.ConsumerName,
                        _options.ClaimMinIdleTimeMilliseconds,
                        cursor,
                        _options.ClaimBatchSize)
                    .ConfigureAwait(false);

                entries = result.ClaimedEntries ?? Array.Empty<StreamEntry>();
                nextCursor = result.NextStartId;
            }
            catch (RedisServerException ex) when (IsAutoClaimUnsupported(ex))
            {
                _logger.LogWarning(
                    ex,
                    "XAUTOCLAIM is not supported by Redis server. Falling back to XPENDING/XCLAIM strategy (phase: {Phase}, stream: {Stream}, group: {Group}, consumer: {Consumer}).",
                    phase,
                    _options.StreamName,
                    _options.GroupName,
                    _options.ConsumerName);

                var pending = await database
                    .StreamPendingMessagesAsync(
                        _options.StreamName,
                        _options.GroupName,
                        _options.ClaimBatchSize,
                        default,
                        null,
                        null,
                        _options.ClaimMinIdleTimeMilliseconds,
                        CommandFlags.None)
                    .ConfigureAwait(false);

                var pendingMessages = pending ?? Array.Empty<StreamPendingMessageInfo>();
                if (pendingMessages.Length == 0)
                {
                    break;
                }

                var idsToClaim = pendingMessages
                    .Select(message => message.MessageId)
                    .ToArray();

                entries = await database
                    .StreamClaimAsync(
                        _options.StreamName,
                        _options.GroupName,
                        _options.ConsumerName,
                        _options.ClaimMinIdleTimeMilliseconds,
                        idsToClaim,
                        CommandFlags.None)
                    .ConfigureAwait(false);

                nextCursor = RedisValue.Null;
            }

            if (entries.Length == 0)
            {
                break;
            }

            var acked = await ProcessEntriesAsync(database, entries, phase, stoppingToken).ConfigureAwait(false);
            reclaimedTotal += entries.Length;
            ackedTotal += acked;

            _logger.LogInformation(
                "Worker batch processed (phase: {Phase}, stream: {Stream}, group: {Group}, consumer: {Consumer}, batchSize: {BatchSize}, ackedCount: {AckedCount}, reclaimedCount: {ReclaimedCount}, durationMs: {DurationMs})",
                phase,
                _options.StreamName,
                _options.GroupName,
                _options.ConsumerName,
                entries.Length,
                acked,
                entries.Length,
                stopwatch.ElapsedMilliseconds);

            if (nextCursor == cursor || nextCursor.IsNull)
            {
                break;
            }

            cursor = nextCursor;
        }

        _logger.LogInformation(
            "Worker reclaim completed (phase: {Phase}, stream: {Stream}, group: {Group}, consumer: {Consumer}, ackedCount: {AckedCount}, reclaimedCount: {ReclaimedCount}, durationMs: {DurationMs})",
            phase,
            _options.StreamName,
            _options.GroupName,
            _options.ConsumerName,
            ackedTotal,
            reclaimedTotal,
            stopwatch.ElapsedMilliseconds);
    }

    private static bool IsAutoClaimUnsupported(RedisServerException ex)
        => ex.Message.Contains("unknown command", StringComparison.OrdinalIgnoreCase)
            || ex.Message.Contains("unsupported command", StringComparison.OrdinalIgnoreCase)
            || ex.Message.Contains("syntax error", StringComparison.OrdinalIgnoreCase);

    private async Task<int> ProcessEntriesAsync(
        IDatabase database,
        StreamEntry[] entries,
        string phase,
        CancellationToken stoppingToken)
    {
        var ackedCount = 0;

        foreach (var entry in entries)
        {
            stoppingToken.ThrowIfCancellationRequested();

            if (!await TryHandleEntryAsync(entry, phase, stoppingToken).ConfigureAwait(false))
            {
                continue;
            }

            await database
                .StreamAcknowledgeAsync(_options.StreamName, _options.GroupName, entry.Id)
                .ConfigureAwait(false);

            ackedCount++;
        }

        return ackedCount;
    }

    protected virtual Task<bool> TryHandleEntryAsync(StreamEntry entry, string phase, CancellationToken stoppingToken)
    {
        if (!TryResolveEventId(entry, out var eventId))
        {
            _logger.LogWarning(
                "Skipping stream entry {EntryId} because event_id is missing or invalid (phase: {Phase}, stream: {Stream}, group: {Group}, consumer: {Consumer})",
                entry.Id,
                phase,
                _options.StreamName,
                _options.GroupName,
                _options.ConsumerName);

            return Task.FromResult(false);
        }

        return HandleEntryWithPersistenceAsync(eventId, entry, phase, stoppingToken);
    }

    private async Task<bool> HandleEntryWithPersistenceAsync(
        Guid eventId,
        StreamEntry entry,
        string phase,
        CancellationToken stoppingToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var eventRepository = scope.ServiceProvider.GetRequiredService<IEventRepository>();
        var eventHandler = scope.ServiceProvider.GetRequiredService<IWorkerEventHandler>();

        try
        {
            await eventRepository
                .UpdateStatusAsync(eventId, EventStatus.PROCESSING, stoppingToken)
                .ConfigureAwait(false);

            await eventRepository
                .IncrementAttemptsAsync(eventId, stoppingToken)
                .ConfigureAwait(false);

            _logger.LogInformation(
                "Event {EventId} transitioned to PROCESSING (entry: {EntryId}, phase: {Phase}, stream: {Stream}, group: {Group}, consumer: {Consumer})",
                eventId,
                entry.Id,
                phase,
                _options.StreamName,
                _options.GroupName,
                _options.ConsumerName);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Failed to persist PROCESSING transition for event {EventId} (entry: {EntryId}, phase: {Phase}, stream: {Stream}, group: {Group}, consumer: {Consumer})",
                eventId,
                entry.Id,
                phase,
                _options.StreamName,
                _options.GroupName,
                _options.ConsumerName);

            return false;
        }

        try
        {
            await eventHandler
                .HandleAsync(eventId, entry, phase, stoppingToken)
                .ConfigureAwait(false);

            await eventRepository
                .UpdateStatusAsync(eventId, EventStatus.SUCCEEDED, stoppingToken)
                .ConfigureAwait(false);

            _logger.LogInformation(
                "Event {EventId} transitioned to SUCCEEDED (entry: {EntryId}, phase: {Phase}, stream: {Stream}, group: {Group}, consumer: {Consumer})",
                eventId,
                entry.Id,
                phase,
                _options.StreamName,
                _options.GroupName,
                _options.ConsumerName);

            return true;
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Handler execution failed for event {EventId}. Attempting to persist FAILED_RETRYABLE status (entry: {EntryId}, phase: {Phase})",
                eventId,
                entry.Id,
                phase);

            try
            {
                await eventRepository
                    .UpdateStatusAsync(eventId, EventStatus.FAILED_RETRYABLE, stoppingToken)
                    .ConfigureAwait(false);

                _logger.LogInformation(
                    "Event {EventId} transitioned to FAILED_RETRYABLE (entry: {EntryId}, phase: {Phase}, stream: {Stream}, group: {Group}, consumer: {Consumer})",
                    eventId,
                    entry.Id,
                    phase,
                    _options.StreamName,
                    _options.GroupName,
                    _options.ConsumerName);

                return true;
            }
            catch (Exception persistenceEx)
            {
                _logger.LogError(
                    persistenceEx,
                    "Failed to persist FAILED_RETRYABLE transition for event {EventId} after handler error (entry: {EntryId}, phase: {Phase}, stream: {Stream}, group: {Group}, consumer: {Consumer})",
                    eventId,
                    entry.Id,
                    phase,
                    _options.StreamName,
                    _options.GroupName,
                    _options.ConsumerName);

                return false;
            }
        }
    }

    private static bool TryResolveEventId(StreamEntry entry, out Guid eventId)
    {
        eventId = default;

        if (TryGetStringField(entry, "event_id", out var eventIdRaw)
            && Guid.TryParse(eventIdRaw, out eventId))
        {
            return true;
        }

        if (!TryGetStringField(entry, "message", out var messageRaw)
            || string.IsNullOrWhiteSpace(messageRaw))
        {
            return false;
        }

        try
        {
            using var json = JsonDocument.Parse(messageRaw);
            if (!json.RootElement.TryGetProperty("event_id", out var eventIdProperty))
            {
                return false;
            }

            if (eventIdProperty.ValueKind == JsonValueKind.String
                && Guid.TryParse(eventIdProperty.GetString(), out eventId))
            {
                return true;
            }

            return eventIdProperty.ValueKind == JsonValueKind.Object
                && Guid.TryParse(eventIdProperty.GetRawText(), out eventId);
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static bool TryGetStringField(StreamEntry entry, string fieldName, out string value)
    {
        value = string.Empty;

        foreach (var field in entry.Values)
        {
            if (!field.Name.IsNullOrEmpty
                && string.Equals(field.Name.ToString(), fieldName, StringComparison.OrdinalIgnoreCase))
            {
                if (field.Value.IsNull)
                {
                    return false;
                }

                value = field.Value.ToString();
                return true;
            }
        }

        return false;
    }
}
