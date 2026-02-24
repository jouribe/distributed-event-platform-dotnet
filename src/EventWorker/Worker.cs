using Microsoft.Extensions.Options;
using StackExchange.Redis;

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

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var entries = await database
                    .StreamReadGroupAsync(
                        _options.StreamName,
                        _options.GroupName,
                        _options.ConsumerName,
                        ">",
                        _options.ReadCount,
                        noAck: false)
                    .ConfigureAwait(false);

                if (entries.Length == 0)
                {
                    await Task.Delay(_options.EmptyReadDelayMilliseconds, stoppingToken).ConfigureAwait(false);
                    continue;
                }

                foreach (var entry in entries)
                {
                    _logger.LogInformation(
                        "Consumed message {EntryId} from stream {Stream} (group: {Group}, consumer: {Consumer})",
                        entry.Id,
                        _options.StreamName,
                        _options.GroupName,
                        _options.ConsumerName);

                    await database
                        .StreamAcknowledgeAsync(_options.StreamName, _options.GroupName, entry.Id)
                        .ConfigureAwait(false);
                }
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
}
