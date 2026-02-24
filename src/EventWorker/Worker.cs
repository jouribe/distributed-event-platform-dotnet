using Microsoft.Extensions.Options;
using StackExchange.Redis;
using System.Diagnostics;

namespace EventWorker;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IConnectionMultiplexer _connectionMultiplexer;
    private readonly IRedisConsumerGroupBootstrapper _bootstrapper;
    private readonly RedisConsumerOptions _options;

    public Worker(
        ILogger<Worker> logger,
        IConnectionMultiplexer connectionMultiplexer,
        IRedisConsumerGroupBootstrapper bootstrapper,
        IOptions<RedisConsumerOptions> options)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _connectionMultiplexer = connectionMultiplexer ?? throw new ArgumentNullException(nameof(connectionMultiplexer));
        _bootstrapper = bootstrapper ?? throw new ArgumentNullException(nameof(bootstrapper));
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
        await _bootstrapper.EnsureConsumerGroupAsync(stoppingToken).ConfigureAwait(false);

        var database = _connectionMultiplexer.GetDatabase();
        await DrainPendingMessagesOnStartupAsync(database, stoppingToken).ConfigureAwait(false);
        await ReclaimPendingMessagesAsync(database, "reclaim-startup", _options.DrainOnStartupMaxBatches, stoppingToken).ConfigureAwait(false);

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
                await Task.Delay(_options.ErrorDelayMilliseconds, stoppingToken).ConfigureAwait(false);
            }
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
        _logger.LogInformation(
            "Consumed message {EntryId} from stream {Stream} (group: {Group}, consumer: {Consumer}, phase: {Phase})",
            entry.Id,
            _options.StreamName,
            _options.GroupName,
            _options.ConsumerName,
            phase);

        return Task.FromResult(true);
    }
}
