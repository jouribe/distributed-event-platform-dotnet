using EventWorker;
using EventPlatform.Domain.Events;
using EventPlatform.Infrastructure.Persistence.Repositories;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using StackExchange.Redis;

namespace EventPlatform.UnitTests.Workers;

/// <summary>
/// Tests for exponential backoff and max-attempts enforcement in Worker.
/// </summary>
public class WorkerRetryTests
{
    private static IOptions<RedisConsumerOptions> CreateConsumerOptions() =>
        Options.Create(new RedisConsumerOptions
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

    private static StreamEntry BuildEntry(Guid eventId) =>
        new("1700000000000-0", new[] { new NameValueEntry("event_id", eventId.ToString()) });

    [Fact]
    public async Task Worker_CallsMarkRetryableFailure_WithCorrectBackoff_OnFirstFailure()
    {
        // Arrange — handler throws, IncrementAttempts returns 1 → backoff = min(2^1, 60) = 2s
        var eventId = Guid.NewGuid();
        var (worker, repoMock, database, options, cancellation) = CreateScenario(
            attemptsAfterIncrement: 1,
            maxAttempts: 5,
            handlerThrows: new InvalidOperationException("transient error"));

        SetupSingleMessageAndCancel(database, options, eventId, cancellation);

        // Act
        await worker.RunAsync(cancellation.Token);

        // Assert
        repoMock.Verify(r => r.MarkRetryableFailureAsync(
                eventId,
                It.Is<DateTimeOffset>(d => IsApproximately(d, DateTimeOffset.UtcNow.AddSeconds(2), toleranceSec: 5)),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()),
            Times.Once);

        repoMock.Verify(r => r.MarkTerminalFailureAsync(
                It.IsAny<Guid>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task Worker_CallsMarkRetryableFailure_WithBackoffCappedAt60s_OnHighAttempt()
    {
        // Arrange — attempts=10 → 2^10 = 1024s, but cap is 60s
        var eventId = Guid.NewGuid();
        var (worker, repoMock, database, options, cancellation) = CreateScenario(
            attemptsAfterIncrement: 10,
            maxAttempts: 15,
            handlerThrows: new InvalidOperationException("transient error"));

        SetupSingleMessageAndCancel(database, options, eventId, cancellation);

        // Act
        await worker.RunAsync(cancellation.Token);

        // Assert — nextAttemptAt should be ≈ now + 60s (cap)
        repoMock.Verify(r => r.MarkRetryableFailureAsync(
                eventId,
                It.Is<DateTimeOffset>(d => IsApproximately(d, DateTimeOffset.UtcNow.AddSeconds(60), toleranceSec: 5)),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task Worker_CallsMarkTerminalFailure_WhenMaxAttemptsReached()
    {
        // Arrange — IncrementAttempts returns MaxAttempts (5) → terminal
        var eventId = Guid.NewGuid();
        var (worker, repoMock, database, options, cancellation) = CreateScenario(
            attemptsAfterIncrement: 5,
            maxAttempts: 5,
            handlerThrows: new InvalidOperationException("persistent error"));

        SetupSingleMessageAndCancel(database, options, eventId, cancellation);

        // Act
        await worker.RunAsync(cancellation.Token);

        // Assert
        repoMock.Verify(r => r.MarkTerminalFailureAsync(
                eventId,
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()),
            Times.Once);

        repoMock.Verify(r => r.MarkRetryableFailureAsync(
                It.IsAny<Guid>(), It.IsAny<DateTimeOffset>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task Worker_StillAcksMessage_WhenTerminalFailurePersisted()
    {
        // Even on terminal failure the message must be ACKed so it leaves the PEL
        var eventId = Guid.NewGuid();
        var (worker, _, database, options, cancellation) = CreateScenario(
            attemptsAfterIncrement: 5,
            maxAttempts: 5,
            handlerThrows: new InvalidOperationException("persistent error"));

        SetupSingleMessageAndCancel(database, options, eventId, cancellation);

        await worker.RunAsync(cancellation.Token);

        database.Verify(d => d.StreamAcknowledgeAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                It.IsAny<RedisValue>(),
                CommandFlags.None),
            Times.Once);
    }

    [Fact]
    public async Task Worker_DoesNotCallMarkRetryable_OnSuccess()
    {
        var eventId = Guid.NewGuid();
        var (worker, repoMock, database, options, cancellation) = CreateScenario(
            attemptsAfterIncrement: 1,
            maxAttempts: 5,
            handlerThrows: null);

        SetupSingleMessageAndCancel(database, options, eventId, cancellation);

        await worker.RunAsync(cancellation.Token);

        repoMock.Verify(r => r.MarkRetryableFailureAsync(
                It.IsAny<Guid>(), It.IsAny<DateTimeOffset>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
            Times.Never);

        repoMock.Verify(r => r.MarkTerminalFailureAsync(
                It.IsAny<Guid>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static (TestableWorker worker, Mock<IEventRepository> repo, Mock<IDatabase> db, IOptions<RedisConsumerOptions> opts, CancellationTokenSource cts)
        CreateScenario(int attemptsAfterIncrement, int maxAttempts, Exception? handlerThrows)
    {
        var consumerOptions = CreateConsumerOptions();
        var retryOptions = Options.Create(new RetryOptions { MaxAttempts = maxAttempts, MaxBackoffSeconds = 60 });

        var repoMock = new Mock<IEventRepository>();
        repoMock.Setup(r => r.TryTransitionStatusAsync(It.IsAny<Guid>(), EventStatus.QUEUED, EventStatus.PROCESSING, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        repoMock.Setup(r => r.IncrementAttemptsAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(attemptsAfterIncrement);
        repoMock.Setup(r => r.MarkRetryableFailureAsync(It.IsAny<Guid>(), It.IsAny<DateTimeOffset>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
        repoMock.Setup(r => r.MarkTerminalFailureAsync(It.IsAny<Guid>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

        var handlerMock = new Mock<IWorkerEventHandler>();
        if (handlerThrows is not null)
            handlerMock.Setup(h => h.HandleAsync(It.IsAny<Guid>(), It.IsAny<StreamEntry>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                       .ThrowsAsync(handlerThrows);
        else
            handlerMock.Setup(h => h.HandleAsync(It.IsAny<Guid>(), It.IsAny<StreamEntry>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                       .Returns(Task.CompletedTask);

        var services = new ServiceCollection();
        services.AddScoped(_ => repoMock.Object);
        services.AddScoped(_ => handlerMock.Object);
        var scopeFactory = services.BuildServiceProvider().GetRequiredService<IServiceScopeFactory>();

        var database = new Mock<IDatabase>();
        var multiplexer = new Mock<IConnectionMultiplexer>();
        multiplexer.Setup(m => m.GetDatabase(It.IsAny<int>(), It.IsAny<object?>())).Returns(database.Object);

        var cts = new CancellationTokenSource();

        var worker = new TestableWorker(
            NullLogger<EventWorker.Worker>.Instance,
            multiplexer.Object,
            new NoopBootstrapper(),
            scopeFactory,
            consumerOptions,
            retryOptions);

        return (worker, repoMock, database, consumerOptions, cts);
    }

    private static void SetupSingleMessageAndCancel(
        Mock<IDatabase> database,
        IOptions<RedisConsumerOptions> options,
        Guid eventId,
        CancellationTokenSource cts)
    {
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
            .ReturnsAsync(new[] { BuildEntry(eventId) })
            .Returns(() =>
            {
                cts.Cancel();
                return Task.FromResult(Array.Empty<StreamEntry>());
            });

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
            .Setup(d => d.StreamAcknowledgeAsync(
                options.Value.StreamName,
                options.Value.GroupName,
                It.IsAny<RedisValue>(),
                CommandFlags.None))
            .ReturnsAsync(1L);
    }

    private static bool IsApproximately(DateTimeOffset actual, DateTimeOffset expected, double toleranceSec) =>
        Math.Abs((actual - expected).TotalSeconds) <= toleranceSec;

    private sealed class NoopBootstrapper : IRedisConsumerGroupBootstrapper
    {
        public Task EnsureConsumerGroupAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class TestableWorker : EventWorker.Worker
    {
        public TestableWorker(
            Microsoft.Extensions.Logging.ILogger<EventWorker.Worker> logger,
            IConnectionMultiplexer connectionMultiplexer,
            IRedisConsumerGroupBootstrapper bootstrapper,
            IServiceScopeFactory scopeFactory,
            IOptions<RedisConsumerOptions> options,
            IOptions<RetryOptions> retryOptions)
            : base(logger, connectionMultiplexer, bootstrapper, scopeFactory, options, retryOptions)
        {
        }

        public Task RunAsync(CancellationToken cancellationToken)
            => ExecuteAsync(cancellationToken);
    }
}


