using EventWorker;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using StackExchange.Redis;

namespace EventPlatform.UnitTests.Workers;

public class RedisConsumerGroupBootstrapperTests
{
    [Fact]
    public async Task EnsureConsumerGroupAsync_CreatesConsumerGroup_OnFirstAttempt()
    {
        var options = CreateOptions();
        var database = new Mock<IDatabase>();
        database
            .Setup(d => d.StreamCreateConsumerGroupAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                "$",
                true,
                CommandFlags.None))
            .ReturnsAsync(true);

        var multiplexer = CreateMultiplexer(database.Object);
        var sut = new RedisConsumerGroupBootstrapper(
            NullLogger<RedisConsumerGroupBootstrapper>.Instance,
            multiplexer.Object,
            options);

        await sut.EnsureConsumerGroupAsync(CancellationToken.None);

        database.Verify(
            d => d.StreamCreateConsumerGroupAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                "$",
                true,
                CommandFlags.None),
            Times.Once);
    }

    [Fact]
    public async Task EnsureConsumerGroupAsync_TreatsBusyGroupAsSuccess()
    {
        var options = CreateOptions();
        var database = new Mock<IDatabase>();
        database
            .Setup(d => d.StreamCreateConsumerGroupAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                "$",
                true,
                CommandFlags.None))
            .ThrowsAsync(new RedisServerException("BUSYGROUP Consumer Group name already exists"));

        var multiplexer = CreateMultiplexer(database.Object);
        var sut = new RedisConsumerGroupBootstrapper(
            NullLogger<RedisConsumerGroupBootstrapper>.Instance,
            multiplexer.Object,
            options);

        await sut.EnsureConsumerGroupAsync(CancellationToken.None);

        database.Verify(
            d => d.StreamCreateConsumerGroupAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                "$",
                true,
                CommandFlags.None),
            Times.Once);
    }

    [Fact]
    public async Task EnsureConsumerGroupAsync_RetriesOnTransientRedisFailures_ThenSucceeds()
    {
        var options = CreateOptions(maxRetryAttempts: 5);
        var database = new Mock<IDatabase>();
        database
            .SetupSequence(d => d.StreamCreateConsumerGroupAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                "$",
                true,
                CommandFlags.None))
            .ThrowsAsync(new RedisConnectionException(ConnectionFailureType.UnableToConnect, "redis unavailable"))
            .ThrowsAsync(new RedisTimeoutException("redis timeout", CommandStatus.Unknown))
            .ReturnsAsync(true);

        var multiplexer = CreateMultiplexer(database.Object);
        var sut = new RedisConsumerGroupBootstrapper(
            NullLogger<RedisConsumerGroupBootstrapper>.Instance,
            multiplexer.Object,
            options);

        await sut.EnsureConsumerGroupAsync(CancellationToken.None);

        database.Verify(
            d => d.StreamCreateConsumerGroupAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                "$",
                true,
                CommandFlags.None),
            Times.Exactly(3));
    }

    [Fact]
    public async Task EnsureConsumerGroupAsync_FailsFastOnNonTransientErrors()
    {
        var options = CreateOptions();
        var database = new Mock<IDatabase>();
        database
            .Setup(d => d.StreamCreateConsumerGroupAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                "$",
                true,
                CommandFlags.None))
            .ThrowsAsync(new RedisServerException("ERR invalid argument"));

        var multiplexer = CreateMultiplexer(database.Object);
        var sut = new RedisConsumerGroupBootstrapper(
            NullLogger<RedisConsumerGroupBootstrapper>.Instance,
            multiplexer.Object,
            options);

        await Assert.ThrowsAsync<RedisServerException>(() => sut.EnsureConsumerGroupAsync(CancellationToken.None));

        database.Verify(
            d => d.StreamCreateConsumerGroupAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                "$",
                true,
                CommandFlags.None),
            Times.Once);
    }

    [Fact]
    public async Task EnsureConsumerGroupAsync_StopsRetrying_WhenCancelled()
    {
        var options = CreateOptions(maxRetryAttempts: 0, initialDelayMs: 1_000, maxDelayMs: 1_000);
        var database = new Mock<IDatabase>();
        using var cancellation = new CancellationTokenSource();

        var callCount = 0;
        database
            .Setup(d => d.StreamCreateConsumerGroupAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                "$",
                true,
                CommandFlags.None))
            .Callback(() =>
            {
                callCount++;
                cancellation.Cancel();
            })
            .ThrowsAsync(new RedisConnectionException(ConnectionFailureType.UnableToConnect, "redis unavailable"));

        var multiplexer = CreateMultiplexer(database.Object);
        var sut = new RedisConsumerGroupBootstrapper(
            NullLogger<RedisConsumerGroupBootstrapper>.Instance,
            multiplexer.Object,
            options);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => sut.EnsureConsumerGroupAsync(cancellation.Token));
        Assert.Equal(1, callCount);
    }

    private static OptionsWrapper<RedisConsumerOptions> CreateOptions(
        int maxRetryAttempts = 3,
        int initialDelayMs = 1,
        int maxDelayMs = 5)
        => new(new RedisConsumerOptions
        {
            StreamName = "events:ingress",
            GroupName = "event-worker",
            ConsumerName = "consumer-1",
            ConsumerGroupBootstrapInitialDelayMilliseconds = initialDelayMs,
            ConsumerGroupBootstrapMaxDelayMilliseconds = maxDelayMs,
            ConsumerGroupBootstrapBackoffFactor = 2.0,
            ConsumerGroupBootstrapMaxRetryAttempts = maxRetryAttempts
        });

    private static Mock<IConnectionMultiplexer> CreateMultiplexer(IDatabase database)
    {
        var multiplexer = new Mock<IConnectionMultiplexer>();
        multiplexer
            .Setup(m => m.GetDatabase(It.IsAny<int>(), It.IsAny<object?>()))
            .Returns(database);

        return multiplexer;
    }
}
