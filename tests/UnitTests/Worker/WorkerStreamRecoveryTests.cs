using EventWorker;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using StackExchange.Redis;

namespace EventPlatform.UnitTests.Workers;

public class WorkerStreamRecoveryTests
{
    [Fact]
    public async Task ExecuteAsync_UsesZeroIdToDrainPendingMessages_OnStartup()
    {
        var options = CreateOptions(new RedisConsumerOptions
        {
            StreamName = "events:ingress",
            GroupName = "event-worker",
            ConsumerName = "consumer-1",
            ReadBatchSize = 5,
            EmptyReadDelay = 1,
            ErrorDelayMilliseconds = 1,
            DrainOnStartupMaxBatches = 2,
            DrainOnStartupMaxMessages = 10,
            ReclaimIntervalMilliseconds = 60_000
        });

        var bootstrapper = CreateBootstrapper();
        var database = new Mock<IDatabase>();
        using var cancellation = new CancellationTokenSource();
        cancellation.CancelAfter(TimeSpan.FromSeconds(2));

        database
            .Setup(d => d.StreamReadGroupAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                options.Value.ConsumerName,
                "0",
                options.Value.ReadBatchSize,
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
                options.Value.ReadBatchSize,
                false,
                null,
                CommandFlags.None))
            .Callback(() => cancellation.Cancel())
            .ReturnsAsync(Array.Empty<StreamEntry>());

        var worker = new TestableWorker(
            NullLogger<EventWorker.Worker>.Instance,
            CreateMultiplexer(database.Object).Object,
            bootstrapper.Object,
            options);

        await worker.RunAsync(cancellation.Token);

        database.Verify(d => d.StreamReadGroupAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                options.Value.ConsumerName,
                "0",
                options.Value.ReadBatchSize,
                false,
            null,
                CommandFlags.None),
            Times.Once);
    }

    [Fact]
    public async Task ExecuteAsync_UsesAutoClaim_ForReclaimPhase()
    {
        var options = CreateOptions(new RedisConsumerOptions
        {
            StreamName = "events:ingress",
            GroupName = "event-worker",
            ConsumerName = "consumer-1",
            ReadBatchSize = 5,
            EmptyReadDelay = 1,
            ErrorDelayMilliseconds = 1,
            DrainOnStartupMaxBatches = 1,
            DrainOnStartupMaxMessages = 10,
            ClaimBatchSize = 5,
            ClaimMinIdleTimeMilliseconds = 1000,
            ReclaimIntervalMilliseconds = 60_000
        });

        var bootstrapper = CreateBootstrapper();
        var database = new Mock<IDatabase>();
        using var cancellation = new CancellationTokenSource();
        cancellation.CancelAfter(TimeSpan.FromSeconds(2));

        database
            .Setup(d => d.StreamReadGroupAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                options.Value.ConsumerName,
                "0",
                options.Value.ReadBatchSize,
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
            .Callback(() => cancellation.Cancel())
            .ThrowsAsync(new OperationCanceledException(cancellation.Token));

        var worker = new TestableWorker(
            NullLogger<EventWorker.Worker>.Instance,
            CreateMultiplexer(database.Object).Object,
            bootstrapper.Object,
            options);

        await worker.RunAsync(cancellation.Token);

        database.Verify(d => d.StreamAutoClaimAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                options.Value.ConsumerName,
                options.Value.ClaimMinIdleTimeMilliseconds,
                It.IsAny<RedisValue>(),
            options.Value.ClaimBatchSize),
            Times.Once);
    }

    [Fact]
    public async Task ExecuteAsync_DoesNotLoopForever_WhenDrainReturnsEmpty()
    {
        var options = CreateOptions(new RedisConsumerOptions
        {
            StreamName = "events:ingress",
            GroupName = "event-worker",
            ConsumerName = "consumer-1",
            ReadBatchSize = 5,
            EmptyReadDelay = 1,
            ErrorDelayMilliseconds = 1,
            DrainOnStartupMaxBatches = 100,
            DrainOnStartupMaxMessages = 1000,
            ReclaimIntervalMilliseconds = 60_000
        });

        var bootstrapper = CreateBootstrapper();
        var database = new Mock<IDatabase>();
        using var cancellation = new CancellationTokenSource();
        cancellation.CancelAfter(TimeSpan.FromSeconds(2));

        database
            .Setup(d => d.StreamReadGroupAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                options.Value.ConsumerName,
                "0",
                options.Value.ReadBatchSize,
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
                options.Value.ReadBatchSize,
                false,
                null,
                CommandFlags.None))
            .Callback(() => cancellation.Cancel())
            .ReturnsAsync(Array.Empty<StreamEntry>());

        var worker = new TestableWorker(
            NullLogger<EventWorker.Worker>.Instance,
            CreateMultiplexer(database.Object).Object,
            bootstrapper.Object,
            options);

        await worker.RunAsync(cancellation.Token);

        database.Verify(d => d.StreamReadGroupAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                options.Value.ConsumerName,
                "0",
                options.Value.ReadBatchSize,
                false,
            null,
                CommandFlags.None),
            Times.Once);
    }

    [Fact]
    public async Task ExecuteAsync_DoesNotAck_WhenHandlerFails()
    {
        var options = CreateOptions(new RedisConsumerOptions
        {
            StreamName = "events:ingress",
            GroupName = "event-worker",
            ConsumerName = "consumer-1",
            ReadBatchSize = 1,
            EmptyReadDelay = 1,
            ErrorDelayMilliseconds = 1,
            DrainOnStartupMaxBatches = 0,
            DrainOnStartupMaxMessages = 0,
            ReclaimIntervalMilliseconds = 60_000
        });

        var bootstrapper = CreateBootstrapper();
        var database = new Mock<IDatabase>();
        using var cancellation = new CancellationTokenSource();
        cancellation.CancelAfter(TimeSpan.FromSeconds(2));

        database
            .Setup(d => d.StreamReadGroupAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                options.Value.ConsumerName,
                ">",
                options.Value.ReadBatchSize,
                false,
                null,
                CommandFlags.None))
            .Callback(() => cancellation.Cancel())
            .ReturnsAsync(new[] { default(StreamEntry) });

        var worker = new FailingTestWorker(
            NullLogger<EventWorker.Worker>.Instance,
            CreateMultiplexer(database.Object).Object,
            bootstrapper.Object,
            options);

        await worker.RunAsync(cancellation.Token);

        database.Verify(d => d.StreamAcknowledgeAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                It.IsAny<RedisValue>(),
                CommandFlags.None),
            Times.Never);
    }

    [Fact]
    public async Task ExecuteAsync_FallsBackToPendingAndClaim_WhenAutoClaimIsUnsupported()
    {
        var options = CreateOptions(new RedisConsumerOptions
        {
            StreamName = "events:ingress",
            GroupName = "event-worker",
            ConsumerName = "consumer-1",
            ReadBatchSize = 1,
            EmptyReadDelay = 1,
            ErrorDelayMilliseconds = 1,
            DrainOnStartupMaxBatches = 1,
            DrainOnStartupMaxMessages = 10,
            ClaimBatchSize = 1,
            ClaimMinIdleTimeMilliseconds = 1000,
            ReclaimIntervalMilliseconds = 60_000
        });

        var bootstrapper = CreateBootstrapper();
        var database = new Mock<IDatabase>();
        using var cancellation = new CancellationTokenSource();
        cancellation.CancelAfter(TimeSpan.FromSeconds(2));

        database
            .Setup(d => d.StreamReadGroupAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                options.Value.ConsumerName,
                "0",
                options.Value.ReadBatchSize,
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
            .ThrowsAsync(new RedisServerException("ERR unknown command 'XAUTOCLAIM'"));

        database
            .Setup(d => d.StreamPendingMessagesAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                options.Value.ClaimBatchSize,
                default,
                null,
                null,
                options.Value.ClaimMinIdleTimeMilliseconds,
                CommandFlags.None))
            .ReturnsAsync(new StreamPendingMessageInfo[1]);

        database
            .Setup(d => d.StreamClaimAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                options.Value.ConsumerName,
                options.Value.ClaimMinIdleTimeMilliseconds,
                It.IsAny<RedisValue[]>(),
                CommandFlags.None))
            .ReturnsAsync(Array.Empty<StreamEntry>());

        database
            .Setup(d => d.StreamReadGroupAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                options.Value.ConsumerName,
                ">",
                options.Value.ReadBatchSize,
                false,
                null,
                CommandFlags.None))
            .Callback(() => cancellation.Cancel())
            .ReturnsAsync(Array.Empty<StreamEntry>());

        var worker = new TestableWorker(
            NullLogger<EventWorker.Worker>.Instance,
            CreateMultiplexer(database.Object).Object,
            bootstrapper.Object,
            options);

        await worker.RunAsync(cancellation.Token);

        database.Verify(d => d.StreamPendingMessagesAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                options.Value.ClaimBatchSize,
                default,
                null,
                null,
                options.Value.ClaimMinIdleTimeMilliseconds,
                CommandFlags.None),
            Times.Once);

        database.Verify(d => d.StreamClaimAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                options.Value.ConsumerName,
                options.Value.ClaimMinIdleTimeMilliseconds,
                It.IsAny<RedisValue[]>(),
                CommandFlags.None),
            Times.Once);
    }

    private static OptionsWrapper<RedisConsumerOptions> CreateOptions(RedisConsumerOptions options)
        => new(options);

    private static Mock<IRedisConsumerGroupBootstrapper> CreateBootstrapper()
    {
        var bootstrapper = new Mock<IRedisConsumerGroupBootstrapper>();
        bootstrapper
            .Setup(b => b.EnsureConsumerGroupAsync(It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        return bootstrapper;
    }

    private static Mock<IConnectionMultiplexer> CreateMultiplexer(IDatabase database)
    {
        var multiplexer = new Mock<IConnectionMultiplexer>();
        multiplexer
            .Setup(m => m.GetDatabase(It.IsAny<int>(), It.IsAny<object?>()))
            .Returns(database);

        return multiplexer;
    }

    private class TestableWorker : EventWorker.Worker
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

    private sealed class FailingTestWorker : TestableWorker
    {
        public FailingTestWorker(
            Microsoft.Extensions.Logging.ILogger<EventWorker.Worker> logger,
            IConnectionMultiplexer connectionMultiplexer,
            IRedisConsumerGroupBootstrapper bootstrapper,
            IOptions<RedisConsumerOptions> options)
            : base(logger, connectionMultiplexer, bootstrapper, options)
        {
        }

        protected override Task<bool> TryHandleEntryAsync(StreamEntry entry, string phase, CancellationToken stoppingToken)
            => Task.FromResult(false);
    }
}
