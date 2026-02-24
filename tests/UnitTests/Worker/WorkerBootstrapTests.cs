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

        var worker = new TestableWorker(
            NullLogger<EventWorker.Worker>.Instance,
            multiplexer.Object,
            bootstrapper.Object,
            options);

        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await worker.RunAsync(cancellation.Token);

        bootstrapper.Verify(
            b => b.EnsureConsumerGroupAsync(cancellation.Token),
            Times.Once);
    }

    [Fact]
    public async Task ExecuteAsync_PropagatesBootstrapExceptions()
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
            .ThrowsAsync(new InvalidOperationException("bootstrap failed"));

        var database = new Mock<IDatabase>();
        var multiplexer = new Mock<IConnectionMultiplexer>();
        multiplexer
            .Setup(m => m.GetDatabase(It.IsAny<int>(), It.IsAny<object?>()))
            .Returns(database.Object);

        var worker = new TestableWorker(
            NullLogger<EventWorker.Worker>.Instance,
            multiplexer.Object,
            bootstrapper.Object,
            options);

        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAsync<InvalidOperationException>(() => worker.RunAsync(cancellation.Token));
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
