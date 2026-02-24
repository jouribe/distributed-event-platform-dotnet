using EventWorker.Resilience;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace EventWorker;

public interface IRedisConsumerGroupBootstrapper
{
    Task EnsureConsumerGroupAsync(CancellationToken cancellationToken);
}

public sealed class RedisConsumerGroupBootstrapper : IRedisConsumerGroupBootstrapper
{
    private readonly ILogger<RedisConsumerGroupBootstrapper> _logger;
    private readonly IConnectionMultiplexer _connectionMultiplexer;
    private readonly RedisConsumerOptions _options;

    public RedisConsumerGroupBootstrapper(
        ILogger<RedisConsumerGroupBootstrapper> logger,
        IConnectionMultiplexer connectionMultiplexer,
        IOptions<RedisConsumerOptions> options)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _connectionMultiplexer = connectionMultiplexer ?? throw new ArgumentNullException(nameof(connectionMultiplexer));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));

        if (_options.ConsumerGroupBootstrapInitialDelayMilliseconds <= 0)
            throw new ArgumentException("ConsumerGroupBootstrapInitialDelayMilliseconds must be greater than zero.", nameof(options));

        if (_options.ConsumerGroupBootstrapMaxDelayMilliseconds <= 0)
            throw new ArgumentException("ConsumerGroupBootstrapMaxDelayMilliseconds must be greater than zero.", nameof(options));

        if (_options.ConsumerGroupBootstrapInitialDelayMilliseconds > _options.ConsumerGroupBootstrapMaxDelayMilliseconds)
            throw new ArgumentException("ConsumerGroupBootstrapInitialDelayMilliseconds cannot be greater than ConsumerGroupBootstrapMaxDelayMilliseconds.", nameof(options));

        if (_options.ConsumerGroupBootstrapBackoffFactor < 1.0)
            throw new ArgumentException("ConsumerGroupBootstrapBackoffFactor must be greater than or equal to one.", nameof(options));

        if (_options.ConsumerGroupBootstrapMaxRetryAttempts < 0)
            throw new ArgumentException("ConsumerGroupBootstrapMaxRetryAttempts must be greater than or equal to zero.", nameof(options));
    }

    public Task EnsureConsumerGroupAsync(CancellationToken cancellationToken)
    {
        var database = _connectionMultiplexer.GetDatabase();

        return ExponentialBackoffRetry.ExecuteAsync(
            operation: token => EnsureConsumerGroupCoreAsync(database, token),
            isTransient: IsTransientBootstrapException,
            initialDelayMilliseconds: _options.ConsumerGroupBootstrapInitialDelayMilliseconds,
            maxDelayMilliseconds: _options.ConsumerGroupBootstrapMaxDelayMilliseconds,
            backoffFactor: _options.ConsumerGroupBootstrapBackoffFactor,
            maxRetryAttempts: _options.ConsumerGroupBootstrapMaxRetryAttempts,
            onRetry: OnRetryAsync,
            cancellationToken: cancellationToken);
    }

    private async Task EnsureConsumerGroupCoreAsync(IDatabase database, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            await database
                .StreamCreateConsumerGroupAsync(
                    _options.StreamName,
                    _options.GroupName,
                    "$",
                    createStream: true)
                .ConfigureAwait(false);

            _logger.LogInformation(
                "Created Redis consumer group {Group} for stream {Stream}.",
                _options.GroupName,
                _options.StreamName);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP", StringComparison.OrdinalIgnoreCase))
        {
            _logger.LogInformation(
                "Redis consumer group {Group} already exists for stream {Stream}.",
                _options.GroupName,
                _options.StreamName);
        }
    }

    private Task OnRetryAsync(int attemptNumber, TimeSpan delay, Exception exception, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        _logger.LogWarning(
            exception,
            "Retrying Redis consumer-group bootstrap (attempt: {Attempt}, delayMs: {DelayMs}, exceptionType: {ExceptionType}).",
            attemptNumber,
            delay.TotalMilliseconds,
            exception.GetType().Name);

        return Task.CompletedTask;
    }

    private static bool IsTransientBootstrapException(Exception exception)
        => exception is RedisConnectionException
            or RedisTimeoutException
            or TimeoutException;
}
