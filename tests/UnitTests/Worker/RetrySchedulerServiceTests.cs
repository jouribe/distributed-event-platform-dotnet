using EventWorker;
using EventPlatform.Application.Abstractions;
using EventPlatform.Domain.Events;
using EventPlatform.Infrastructure.Persistence.Repositories;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using System.Text.Json;

namespace EventPlatform.UnitTests.Workers;

/// <summary>
/// Tests for RetrySchedulerService: batch processing, per-event re-enqueue, and error resilience.
/// </summary>
public class RetrySchedulerServiceTests
{
    [Fact]
    public async Task Scheduler_DoesNotPublish_WhenNoRetryableEventsFound()
    {
        var (service, repoMock, publisherMock, cts) = CreateScenario();

        SetupGetRetryable(repoMock, cts, firstPage: EmptyPage());

        await service.RunAsync(cts.Token);

        publisherMock.Verify(p => p.PublishAsync(
                It.IsAny<EventEnvelope>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task Scheduler_UpdatesStatusAndPublishes_ForEachEligibleEvent()
    {
        var (service, repoMock, publisherMock, cts) = CreateScenario();

        var envelope1 = CreateEnvelope();
        var envelope2 = CreateEnvelope();

        // Cycle 1: two eligible events; cycle 2: empty + cancel.
        repoMock.SetupSequence(r => r.GetRetryableEventsAsync(
                It.IsAny<DateTimeOffset>(), It.IsAny<int>(), It.IsAny<int>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(PageOf(envelope1, envelope2))
            .Returns(() =>
            {
                cts.Cancel();
                return Task.FromResult(EmptyPage());
            });

        await service.RunAsync(cts.Token);

        repoMock.Verify(r => r.UpdateStatusAsync(
                It.IsAny<Guid>(), EventStatus.QUEUED, It.IsAny<CancellationToken>()),
            Times.Exactly(2));

        publisherMock.Verify(p => p.PublishAsync(
                It.IsAny<EventEnvelope>(), It.IsAny<CancellationToken>()),
            Times.Exactly(2));
    }

    [Fact]
    public async Task Scheduler_ContinuesPolling_WhenPublisherThrows()
    {
        var (service, repoMock, publisherMock, cts) = CreateScenario();

        var envelope = CreateEnvelope();

        // Cycle 1: one event (publish will throw); cycle 2: empty + cancel.
        repoMock.SetupSequence(r => r.GetRetryableEventsAsync(
                It.IsAny<DateTimeOffset>(), It.IsAny<int>(), It.IsAny<int>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(PageOf(envelope))
            .Returns(() =>
            {
                cts.Cancel();
                return Task.FromResult(EmptyPage());
            });

        publisherMock
            .Setup(p => p.PublishAsync(It.IsAny<EventEnvelope>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("simulated publish failure"));

        // Should complete without re-throwing the publish exception.
        await service.RunAsync(cts.Token);

        // UpdateStatus was called before the publish attempt.
        repoMock.Verify(r => r.UpdateStatusAsync(
                envelope.Id, EventStatus.QUEUED, It.IsAny<CancellationToken>()),
            Times.Once);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static (
        TestableRetrySchedulerService service,
        Mock<IEventRepository> repo,
        Mock<IEventPublisher> publisher,
        CancellationTokenSource cts)
        CreateScenario()
    {
        var cts = new CancellationTokenSource();

        var repoMock = new Mock<IEventRepository>();
        repoMock.Setup(r => r.UpdateStatusAsync(
                It.IsAny<Guid>(), It.IsAny<EventStatus>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var publisherMock = new Mock<IEventPublisher>();
        publisherMock
            .Setup(p => p.PublishAsync(It.IsAny<EventEnvelope>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var services = new ServiceCollection();
        services.AddScoped(_ => repoMock.Object);
        var scopeFactory = services.BuildServiceProvider().GetRequiredService<IServiceScopeFactory>();

        var service = new TestableRetrySchedulerService(
            NullLogger<RetrySchedulerService>.Instance,
            scopeFactory,
            publisherMock.Object,
            Options.Create(new RetryOptions { MaxAttempts = 5, MaxBackoffSeconds = 60, PollingIntervalSeconds = 0 }),
            Options.Create(new RedisConsumerOptions { StreamName = "events:ingress" }));

        return (service, repoMock, publisherMock, cts);
    }

    /// <summary>Configures a single-call setup that returns <paramref name="firstPage"/> then cancels.</summary>
    private static void SetupGetRetryable(
        Mock<IEventRepository> repoMock,
        CancellationTokenSource cts,
        RetryableEventsPage firstPage)
    {
        repoMock
            .Setup(r => r.GetRetryableEventsAsync(
                It.IsAny<DateTimeOffset>(), It.IsAny<int>(), It.IsAny<int>(),
                It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                cts.Cancel();
                return Task.FromResult(firstPage);
            });
    }

    private static EventEnvelope CreateEnvelope() =>
        EventEnvelope.CreateNew(
            Guid.NewGuid(),
            "test.event",
            DateTimeOffset.UtcNow,
            "unit-test",
            "tenant-1",
            Guid.NewGuid().ToString(),
            Guid.NewGuid(),
            JsonDocument.Parse("{}"));

    private static RetryableEventsPage EmptyPage() =>
        new(Array.Empty<EventEnvelope>(), hasMore: false, skip: 0, pageSize: 100);

    private static RetryableEventsPage PageOf(params EventEnvelope[] envelopes) =>
        new(envelopes, hasMore: false, skip: 0, pageSize: 100);

    private sealed class TestableRetrySchedulerService : RetrySchedulerService
    {
        public TestableRetrySchedulerService(
            Microsoft.Extensions.Logging.ILogger<RetrySchedulerService> logger,
            IServiceScopeFactory scopeFactory,
            IEventPublisher eventPublisher,
            IOptions<RetryOptions> retryOptions,
            IOptions<RedisConsumerOptions> consumerOptions)
            : base(logger, scopeFactory, eventPublisher, retryOptions, consumerOptions)
        {
        }

        public Task RunAsync(CancellationToken cancellationToken)
            => ExecuteAsync(cancellationToken);
    }
}
