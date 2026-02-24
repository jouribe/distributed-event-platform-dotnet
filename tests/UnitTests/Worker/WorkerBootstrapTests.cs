using EventWorker;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using StackExchange.Redis;

namespace EventPlatform.UnitTests.Workers;

public class WorkerBootstrapTests
{
    [Fact]
    public async Task ExecuteAsync_CreatesConsumerGroup_OnStartup()
    {
        var options = Options.Create(new RedisConsumerOptions
        {
            StreamName = "events:ingress",
            GroupName = "event-worker",
            ConsumerName = "consumer-1"
        });

        var database = new Mock<IDatabase>();
        database
            .Setup(d => d.StreamCreateConsumerGroupAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                "$",
                true,
                CommandFlags.None))
            .ReturnsAsync(true);

        var multiplexer = new Mock<IConnectionMultiplexer>();
        multiplexer
            .Setup(m => m.GetDatabase(It.IsAny<int>(), It.IsAny<object?>()))
            .Returns(database.Object);

        var worker = new TestableWorker(NullLogger<EventWorker.Worker>.Instance, multiplexer.Object, options);

        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await worker.RunAsync(cancellation.Token);

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
    public async Task ExecuteAsync_DoesNotThrow_WhenConsumerGroupAlreadyExists()
    {
        var options = Options.Create(new RedisConsumerOptions
        {
            StreamName = "events:ingress",
            GroupName = "event-worker",
            ConsumerName = "consumer-1"
        });

        var database = new Mock<IDatabase>();
        database
            .Setup(d => d.StreamCreateConsumerGroupAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                "$",
                true,
                CommandFlags.None))
            .ThrowsAsync(new RedisServerException("BUSYGROUP Consumer Group name already exists"));

        var multiplexer = new Mock<IConnectionMultiplexer>();
        multiplexer
            .Setup(m => m.GetDatabase(It.IsAny<int>(), It.IsAny<object?>()))
            .Returns(database.Object);

        var worker = new TestableWorker(NullLogger<EventWorker.Worker>.Instance, multiplexer.Object, options);

        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await worker.RunAsync(cancellation.Token);
    }

    private sealed class TestableWorker : EventWorker.Worker
    {
        public TestableWorker(
            Microsoft.Extensions.Logging.ILogger<EventWorker.Worker> logger,
            IConnectionMultiplexer connectionMultiplexer,
            IOptions<RedisConsumerOptions> options)
            : base(logger, connectionMultiplexer, options)
        {
        }

        public Task RunAsync(CancellationToken cancellationToken)
            => ExecuteAsync(cancellationToken);
    }
}
