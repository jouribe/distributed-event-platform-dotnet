using System.Text.Json;
using EventPlatform.Application.Abstractions;
using EventPlatform.Domain.Events;
using EventPlatform.Infrastructure.Messaging;
using EventPlatform.Infrastructure.Persistence.Repositories;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;

namespace EventPlatform.UnitTests.Infrastructure.Messaging;

public class OutboxPublisherServiceTests
{
    [Fact]
    public async Task ExecuteAsync_PublishesAndMarksPublished_WhenOutboxHasPendingRows()
    {
        var published = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var payload = JsonDocument.Parse("{\"event_id\":\"11111111-1111-1111-1111-111111111111\",\"event_type\":\"user.created\",\"tenant_id\":\"t\"}");
        var outboxEvent = OutboxEvent.CreateNew(
            Guid.NewGuid(),
            Guid.NewGuid(),
            "events:ingress",
            payload);

        var outboxRepository = new Mock<IOutboxRepository>(MockBehavior.Loose);
        outboxRepository
            .SetupSequence(x => x.GetUnpublishedAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync([outboxEvent])
            .ReturnsAsync(Array.Empty<OutboxEvent>());
        outboxRepository
            .Setup(x => x.MarkPublishedAndQueueEventAsync(outboxEvent.Id, outboxEvent.EventId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .Callback(() => published.TrySetResult(true));
        outboxRepository
            .Setup(x => x.CountPendingAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(0);
        outboxRepository
            .Setup(x => x.DeletePublishedAsync(It.IsAny<DateTimeOffset>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(0);

        var publisher = new Mock<IEventPublisher>(MockBehavior.Loose);
        publisher
            .Setup(x => x.PublishToStreamAsync(outboxEvent.StreamName, outboxEvent.Payload, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var service = new OutboxPublisherService(
            outboxRepository.Object,
            publisher.Object,
            new StaticOptionsMonitor<OutboxPublisherOptions>(new OutboxPublisherOptions
            {
                PollIntervalMilliseconds = 10,
                MaxBatchSize = 10
            }),
            NullLogger<OutboxPublisherService>.Instance);

        await service.StartAsync(CancellationToken.None);

        var completed = await Task.WhenAny(published.Task, Task.Delay(TimeSpan.FromSeconds(2)));
        await service.StopAsync(CancellationToken.None);

        Assert.Same(published.Task, completed);
        publisher.Verify(x => x.PublishToStreamAsync(outboxEvent.StreamName, outboxEvent.Payload, It.IsAny<CancellationToken>()), Times.AtLeastOnce);
        outboxRepository.Verify(x => x.MarkPublishedAndQueueEventAsync(outboxEvent.Id, outboxEvent.EventId, It.IsAny<CancellationToken>()), Times.AtLeastOnce);
    }

    [Fact]
    public async Task ExecuteAsync_RecordsAttempt_WhenPublishFails()
    {
        var recorded = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var payload = JsonDocument.Parse("{\"event_id\":\"11111111-1111-1111-1111-111111111111\",\"event_type\":\"user.created\",\"tenant_id\":\"t\"}");
        var outboxEvent = OutboxEvent.CreateNew(
            Guid.NewGuid(),
            Guid.NewGuid(),
            "events:ingress",
            payload);

        var outboxRepository = new Mock<IOutboxRepository>(MockBehavior.Loose);
        outboxRepository
            .SetupSequence(x => x.GetUnpublishedAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync([outboxEvent])
            .ReturnsAsync(Array.Empty<OutboxEvent>());
        outboxRepository
            .Setup(x => x.RecordPublishAttemptAsync(outboxEvent.Id, It.Is<string>(s => s.Contains("Publish error:")), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask)
            .Callback(() => recorded.TrySetResult(true));
        outboxRepository
            .Setup(x => x.CountPendingAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(1);
        outboxRepository
            .Setup(x => x.DeletePublishedAsync(It.IsAny<DateTimeOffset>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(0);

        var publisher = new Mock<IEventPublisher>(MockBehavior.Loose);
        publisher
            .Setup(x => x.PublishToStreamAsync(outboxEvent.StreamName, outboxEvent.Payload, It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("redis unavailable"));

        var service = new OutboxPublisherService(
            outboxRepository.Object,
            publisher.Object,
            new StaticOptionsMonitor<OutboxPublisherOptions>(new OutboxPublisherOptions
            {
                PollIntervalMilliseconds = 10,
                MaxBatchSize = 10
            }),
            NullLogger<OutboxPublisherService>.Instance);

        await service.StartAsync(CancellationToken.None);

        var completed = await Task.WhenAny(recorded.Task, Task.Delay(TimeSpan.FromSeconds(2)));
        await service.StopAsync(CancellationToken.None);

        Assert.Same(recorded.Task, completed);
        publisher.Verify(x => x.PublishToStreamAsync(outboxEvent.StreamName, outboxEvent.Payload, It.IsAny<CancellationToken>()), Times.AtLeastOnce);
        outboxRepository.Verify(x => x.RecordPublishAttemptAsync(outboxEvent.Id, It.Is<string>(s => s.Contains("Publish error:")), It.IsAny<CancellationToken>()), Times.AtLeastOnce);
    }

    private sealed class StaticOptionsMonitor<T> : IOptionsMonitor<T>
    {
        public StaticOptionsMonitor(T value) => CurrentValue = value;

        public T CurrentValue { get; }
        public T Get(string? name) => CurrentValue;
        public IDisposable? OnChange(Action<T, string?> listener) => null;
    }
}



