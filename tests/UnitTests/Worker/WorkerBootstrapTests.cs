using EventWorker;
using EventPlatform.Infrastructure.Persistence.Repositories;
using Microsoft.Extensions.DependencyInjection;
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
            CreateScopeFactory(),
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
            CreateScopeFactory(),
            options);

        await worker.RunAsync(cancellation.Token);

        bootstrapper.Verify(
            b => b.EnsureConsumerGroupAsync(cancellation.Token),
            Times.Exactly(2));
    }

    private static IServiceScopeFactory CreateScopeFactory(
        Mock<IEventRepository>? eventRepositoryMock = null,
        Mock<IWorkerEventHandler>? eventHandlerMock = null)
    {
        if (eventRepositoryMock is null)
        {
            eventRepositoryMock = new Mock<IEventRepository>();
            eventRepositoryMock
                .Setup(r => r.UpdateStatusAsync(It.IsAny<Guid>(), It.IsAny<EventPlatform.Domain.Events.EventStatus>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            eventRepositoryMock
                .Setup(r => r.IncrementAttemptsAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
        }

        if (eventHandlerMock is null)
        {
            eventHandlerMock = new Mock<IWorkerEventHandler>();
            eventHandlerMock
                .Setup(h => h.HandleAsync(It.IsAny<Guid>(), It.IsAny<StreamEntry>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
        }

        var services = new ServiceCollection();
        services.AddScoped(_ => eventRepositoryMock.Object);
        services.AddScoped(_ => eventHandlerMock.Object);

        var provider = services.BuildServiceProvider();
        return provider.GetRequiredService<IServiceScopeFactory>();
    }

    private sealed class TestableWorker : EventWorker.Worker
    {
        public TestableWorker(
            Microsoft.Extensions.Logging.ILogger<EventWorker.Worker> logger,
            IConnectionMultiplexer connectionMultiplexer,
            IRedisConsumerGroupBootstrapper bootstrapper,
            IServiceScopeFactory scopeFactory,
            IOptions<RedisConsumerOptions> options)
            : base(logger, connectionMultiplexer, bootstrapper, scopeFactory, options)
        {
        }

        public Task RunAsync(CancellationToken cancellationToken)
            => ExecuteAsync(cancellationToken);
    }
}
