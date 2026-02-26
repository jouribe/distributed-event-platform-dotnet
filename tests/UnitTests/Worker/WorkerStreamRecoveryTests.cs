using EventWorker;
using EventPlatform.Domain.Events;
using EventPlatform.Infrastructure.Persistence.Repositories;
using Microsoft.Extensions.DependencyInjection;
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
            CreateScopeFactory(),
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
            CreateScopeFactory(),
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
            CreateScopeFactory(),
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
    public async Task ExecuteAsync_PersistsProcessingAndSucceeded_ThenAcksMessage()
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
        var eventRepository = new Mock<IEventRepository>();
        var eventHandler = new Mock<IWorkerEventHandler>();
        using var cancellation = new CancellationTokenSource();
        cancellation.CancelAfter(TimeSpan.FromSeconds(2));

        eventRepository
            .Setup(r => r.UpdateStatusAsync(It.IsAny<Guid>(), It.IsAny<EventStatus>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        eventRepository
            .Setup(r => r.IncrementAttemptsAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        eventHandler
            .Setup(h => h.HandleAsync(It.IsAny<Guid>(), It.IsAny<StreamEntry>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var eventId = Guid.NewGuid();

        database
            .SetupSequence(d => d.StreamReadGroupAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                options.Value.ConsumerName,
                ">",
                options.Value.ReadBatchSize,
                false,
                null,
                CommandFlags.None))
            .ReturnsAsync(new[]
            {
                new StreamEntry("1700000000000-0", new[]
                {
                    new NameValueEntry("event_id", eventId.ToString())
                })
            })
            .Returns(() =>
            {
                cancellation.Cancel();
                return Task.FromResult(Array.Empty<StreamEntry>());
            });

        var worker = new TestableWorker(
            NullLogger<EventWorker.Worker>.Instance,
            CreateMultiplexer(database.Object).Object,
            bootstrapper.Object,
            CreateScopeFactory(eventRepository, eventHandler),
            options);

        await worker.RunAsync(cancellation.Token);

        eventRepository.Verify(r => r.UpdateStatusAsync(
                eventId,
                EventStatus.PROCESSING,
                It.IsAny<CancellationToken>()),
            Times.Once);

        eventRepository.Verify(r => r.UpdateStatusAsync(
                eventId,
                EventStatus.SUCCEEDED,
                It.IsAny<CancellationToken>()),
            Times.Once);

        database.Verify(d => d.StreamAcknowledgeAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                It.IsAny<RedisValue>(),
                CommandFlags.None),
            Times.Once);
    }

    [Fact]
    public async Task ExecuteAsync_Acks_WhenHandlerFailsAndFailedStatusIsPersisted()
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
        var eventRepository = new Mock<IEventRepository>();
        var eventHandler = new Mock<IWorkerEventHandler>();
        using var cancellation = new CancellationTokenSource();
        cancellation.CancelAfter(TimeSpan.FromSeconds(2));

        eventRepository
            .Setup(r => r.UpdateStatusAsync(It.IsAny<Guid>(), It.IsAny<EventStatus>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        eventRepository
            .Setup(r => r.IncrementAttemptsAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        eventHandler
            .Setup(h => h.HandleAsync(It.IsAny<Guid>(), It.IsAny<StreamEntry>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("handler failed"));

        database
            .SetupSequence(d => d.StreamReadGroupAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                options.Value.ConsumerName,
                ">",
                options.Value.ReadBatchSize,
                false,
                null,
                CommandFlags.None))
            .ReturnsAsync(new[]
            {
                new StreamEntry("1700000000000-0", new[]
                {
                    new NameValueEntry("event_id", Guid.NewGuid().ToString())
                })
            })
            .Returns(() =>
            {
                cancellation.Cancel();
                return Task.FromResult(Array.Empty<StreamEntry>());
            });

        var worker = new TestableWorker(
            NullLogger<EventWorker.Worker>.Instance,
            CreateMultiplexer(database.Object).Object,
            bootstrapper.Object,
            CreateScopeFactory(eventRepository, eventHandler),
            options);

        await worker.RunAsync(cancellation.Token);

        database.Verify(d => d.StreamAcknowledgeAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                It.IsAny<RedisValue>(),
                CommandFlags.None),
            Times.Once);

        eventRepository.Verify(r => r.UpdateStatusAsync(
                It.IsAny<Guid>(),
                EventStatus.PROCESSING,
                It.IsAny<CancellationToken>()),
            Times.Once);

        eventRepository.Verify(r => r.UpdateStatusAsync(
                It.IsAny<Guid>(),
                EventStatus.FAILED_RETRYABLE,
                It.IsAny<CancellationToken>()),
            Times.Once);
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
            CreateScopeFactory(),
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

    private static IServiceScopeFactory CreateScopeFactory(
        Mock<IEventRepository>? eventRepositoryMock = null,
        Mock<IWorkerEventHandler>? eventHandlerMock = null)
    {
        if (eventRepositoryMock is null)
        {
            eventRepositoryMock = new Mock<IEventRepository>();
            eventRepositoryMock
                .Setup(r => r.UpdateStatusAsync(It.IsAny<Guid>(), It.IsAny<EventStatus>(), It.IsAny<CancellationToken>()))
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

    private class TestableWorker : EventWorker.Worker
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
