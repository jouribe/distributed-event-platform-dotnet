using EventWorker;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using StackExchange.Redis;

namespace EventPlatform.UnitTests.Workers;

public class WorkerBootstrapTests
{
    [Fact]
    public async Task ExecuteAsync_EnsuresConsumerGroupBootstrap_OnStartup()
    {
        var options = Options.Create(new RedisConsumerOptions
        {
            StreamName = "events:ingress",
            GroupName = "event-worker",
            ConsumerName = "consumer-1"
        });

        var bootstrapper = new Mock<IRedisConsumerGroupBootstrapper>();
        bootstrapper
            .Setup(b => b.EnsureConsumerGroupAsync(It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var database = new Mock<IDatabase>();
        var multiplexer = new Mock<IConnectionMultiplexer>();
        multiplexer
            .Setup(m => m.GetDatabase(It.IsAny<int>(), It.IsAny<object?>()))
            .Returns(database.Object);

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(2));

        database
            .Setup(d => d.StreamReadGroupAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                options.Value.ConsumerName,
                "0",
                options.Value.EffectiveReadBatchSize,
                false,
                null,
                CommandFlags.None))
            .ReturnsAsync(Array.Empty<StreamEntry>());

        database
            .Setup(d => d.StreamAutoClaimAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                options.Value.ConsumerName,
                options.Value.ClaimMinIdleTimeMilliseconds,
                It.IsAny<RedisValue>(),
                options.Value.ClaimBatchSize))
            .ReturnsAsync(default(StreamAutoClaimResult));

        database
            .Setup(d => d.StreamReadGroupAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                options.Value.ConsumerName,
                ">",
                options.Value.EffectiveReadBatchSize,
                false,
                null,
                CommandFlags.None))
            .Callback(() => cancellation.Cancel())
            .ReturnsAsync(Array.Empty<StreamEntry>());

        var worker = new TestableWorker(
            NullLogger<EventWorker.Worker>.Instance,
            multiplexer.Object,
            bootstrapper.Object,
            options);

        await worker.RunAsync(cancellation.Token);

        bootstrapper.Verify(
            b => b.EnsureConsumerGroupAsync(cancellation.Token),
            Times.Once);
    }

    [Fact]
    public async Task ExecuteAsync_RetriesBootstrapExceptions_DuringStartupRecovery()
    {
        var options = Options.Create(new RedisConsumerOptions
        {
            StreamName = "events:ingress",
            GroupName = "event-worker",
            ConsumerName = "consumer-1",
            ErrorDelayMilliseconds = 1
        });

        var bootstrapper = new Mock<IRedisConsumerGroupBootstrapper>();
        bootstrapper
            .SetupSequence(b => b.EnsureConsumerGroupAsync(It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("bootstrap failed"))
            .Returns(Task.CompletedTask);

        var database = new Mock<IDatabase>();
        var multiplexer = new Mock<IConnectionMultiplexer>();
        multiplexer
            .Setup(m => m.GetDatabase(It.IsAny<int>(), It.IsAny<object?>()))
            .Returns(database.Object);

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(2));

        database
            .Setup(d => d.StreamReadGroupAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                options.Value.ConsumerName,
                "0",
                options.Value.EffectiveReadBatchSize,
                false,
                null,
                CommandFlags.None))
            .ReturnsAsync(Array.Empty<StreamEntry>());

        database
            .Setup(d => d.StreamAutoClaimAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                options.Value.ConsumerName,
                options.Value.ClaimMinIdleTimeMilliseconds,
                It.IsAny<RedisValue>(),
                options.Value.ClaimBatchSize))
            .ReturnsAsync(default(StreamAutoClaimResult));

        database
            .Setup(d => d.StreamReadGroupAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                options.Value.ConsumerName,
                ">",
                options.Value.EffectiveReadBatchSize,
                false,
                null,
                CommandFlags.None))
            .Callback(() => cancellation.Cancel())
            .ReturnsAsync(Array.Empty<StreamEntry>());

        var worker = new TestableWorker(
            NullLogger<EventWorker.Worker>.Instance,
            multiplexer.Object,
            bootstrapper.Object,
            options);

        await worker.RunAsync(cancellation.Token);

        bootstrapper.Verify(
            b => b.EnsureConsumerGroupAsync(cancellation.Token),
            Times.Exactly(2));
    }

    private sealed class TestableWorker : EventWorker.Worker
    {
        public TestableWorker(
            Microsoft.Extensions.Logging.ILogger<EventWorker.Worker> logger,
            IConnectionMultiplexer connectionMultiplexer,
            IRedisConsumerGroupBootstrapper bootstrapper,
            IOptions<RedisConsumerOptions> options)
            : base(logger, connectionMultiplexer, bootstrapper, options)
        {
        }

        public Task RunAsync(CancellationToken cancellationToken)
            => ExecuteAsync(cancellationToken);
    }
}
