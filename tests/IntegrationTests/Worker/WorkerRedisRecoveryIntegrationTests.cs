using EventWorker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using Testcontainers.Redis;

namespace EventPlatform.IntegrationTests.Worker;

public sealed class WorkerRedisRecoveryIntegrationTests : IAsyncLifetime
{
    private readonly RedisContainer _redisContainer = new RedisBuilder("redis:7-alpine").Build();
    private IConnectionMultiplexer _multiplexer = default!;

    [Fact]
    public async Task Worker_DrainsPendingMessagesForSameConsumer_OnStartup()
    {
        var streamName = $"events:ingress:{Guid.NewGuid():N}";
        var groupName = $"event-worker:{Guid.NewGuid():N}";
        const string consumerName = "consumer-a";

        var database = _multiplexer.GetDatabase();

        await database.StreamCreateConsumerGroupAsync(streamName, groupName, "$", createStream: true);

        var messageId = await database.StreamAddAsync(streamName, "event_type", "user.created");

        var delivered = await database.StreamReadGroupAsync(streamName, groupName, consumerName, ">", count: 1, noAck: false);
        Assert.Single(delivered);
        Assert.Equal(messageId, delivered[0].Id);

        var pendingBefore = await database.StreamPendingAsync(streamName, groupName);
        Assert.Equal(1, pendingBefore.PendingMessageCount);

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var worker = new TestableWorker(
            NullLogger<EventWorker.Worker>.Instance,
            _multiplexer,
            new NoopBootstrapper(),
            Options.Create(new RedisConsumerOptions
            {
                ConnectionString = _redisContainer.GetConnectionString(),
                StreamName = streamName,
                GroupName = groupName,
                ConsumerName = consumerName,
                ReadBatchSize = 10,
                DrainOnStartupMaxBatches = 10,
                DrainOnStartupMaxMessages = 100,
                ClaimBatchSize = 10,
                ClaimMinIdleTimeMilliseconds = 50,
                ReclaimIntervalMilliseconds = 30_000,
                EmptyReadDelay = 10,
                ErrorDelayMilliseconds = 10
            }),
            CreateScopeFactory(),
            onHandled: null);

        var workerTask = worker.RunAsync(cancellation.Token);

        await WaitUntilAsync(async () =>
        {
            var pending = await database.StreamPendingAsync(streamName, groupName);
            return pending.PendingMessageCount == 0;
        }, TimeSpan.FromSeconds(3));

        cancellation.Cancel();
        await workerTask;

        var pendingAfter = await database.StreamPendingAsync(streamName, groupName);
        Assert.Equal(0, pendingAfter.PendingMessageCount);
    }

    [Fact]
    public async Task Worker_ReclaimsOrphanedMessagesFromOtherConsumer_OnStartup()
    {
        var streamName = $"events:ingress:{Guid.NewGuid():N}";
        var groupName = $"event-worker:{Guid.NewGuid():N}";
        const string crashedConsumer = "consumer-crashed";
        const string activeConsumer = "consumer-active";

        var database = _multiplexer.GetDatabase();

        await database.StreamCreateConsumerGroupAsync(streamName, groupName, "$", createStream: true);

        var messageId = await database.StreamAddAsync(streamName, "event_type", "user.created");

        var delivered = await database.StreamReadGroupAsync(streamName, groupName, crashedConsumer, ">", count: 1, noAck: false);
        Assert.Single(delivered);
        Assert.Equal(messageId, delivered[0].Id);

        await Task.Delay(TimeSpan.FromMilliseconds(150));

        var pendingBefore = await database.StreamPendingAsync(streamName, groupName);
        Assert.Equal(1, pendingBefore.PendingMessageCount);

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var worker = new TestableWorker(
            NullLogger<EventWorker.Worker>.Instance,
            _multiplexer,
            new NoopBootstrapper(),
            Options.Create(new RedisConsumerOptions
            {
                ConnectionString = _redisContainer.GetConnectionString(),
                StreamName = streamName,
                GroupName = groupName,
                ConsumerName = activeConsumer,
                ReadBatchSize = 10,
                DrainOnStartupMaxBatches = 2,
                DrainOnStartupMaxMessages = 10,
                ClaimBatchSize = 10,
                ClaimMinIdleTimeMilliseconds = 50,
                ReclaimIntervalMilliseconds = 30_000,
                EmptyReadDelay = 10,
                ErrorDelayMilliseconds = 10
            }),
            CreateScopeFactory(),
            onHandled: null);

        var workerTask = worker.RunAsync(cancellation.Token);

        await WaitUntilAsync(async () =>
        {
            var pending = await database.StreamPendingAsync(streamName, groupName);
            return pending.PendingMessageCount == 0;
        }, TimeSpan.FromSeconds(3));

        cancellation.Cancel();
        await workerTask;

        var pendingAfter = await database.StreamPendingAsync(streamName, groupName);
        Assert.Equal(0, pendingAfter.PendingMessageCount);
    }

    public async Task InitializeAsync()
    {
        await _redisContainer.StartAsync();
        _multiplexer = await ConnectionMultiplexer.ConnectAsync(_redisContainer.GetConnectionString());
    }

    public async Task DisposeAsync()
    {
        if (_multiplexer is not null)
        {
            await _multiplexer.CloseAsync();
            await _multiplexer.DisposeAsync();
        }

        await _redisContainer.DisposeAsync();
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition, TimeSpan timeout)
    {
        var start = DateTimeOffset.UtcNow;

        while (DateTimeOffset.UtcNow - start < timeout)
        {
            if (await condition())
            {
                return;
            }

            await Task.Delay(50);
        }

        throw new TimeoutException($"Condition was not met within {timeout.TotalSeconds:F1}s.");
    }

    private sealed class NoopBootstrapper : IRedisConsumerGroupBootstrapper
    {
        public Task EnsureConsumerGroupAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class TestableWorker : EventWorker.Worker
    {
        private readonly Action<StreamEntry>? _onHandled;

        private static IServiceScopeFactory CreateDefaultScopeFactory()
            => new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>();

        public TestableWorker(
            Microsoft.Extensions.Logging.ILogger<EventWorker.Worker> logger,
            IConnectionMultiplexer connectionMultiplexer,
            IRedisConsumerGroupBootstrapper bootstrapper,
            IOptions<RedisConsumerOptions> options,
            IServiceScopeFactory? scopeFactory,
            Action<StreamEntry>? onHandled)
            : base(logger, connectionMultiplexer, bootstrapper, scopeFactory ?? CreateDefaultScopeFactory(), options)
        {
            _onHandled = onHandled;
        }

        public Task RunAsync(CancellationToken cancellationToken)
            => ExecuteAsync(cancellationToken);

        protected override Task<bool> TryHandleEntryAsync(StreamEntry entry, string phase, CancellationToken stoppingToken)
        {
            _onHandled?.Invoke(entry);
            return Task.FromResult(true);
        }
    }

    private static IServiceScopeFactory CreateScopeFactory()
        => new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>();
}
