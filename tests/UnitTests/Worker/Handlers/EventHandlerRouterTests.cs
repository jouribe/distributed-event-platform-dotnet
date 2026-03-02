using EventWorker;
using EventWorker.Handlers;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using StackExchange.Redis;

namespace EventPlatform.UnitTests.Worker.Handlers;

public sealed class EventHandlerRouterTests
{
    // -------------------------------------------------------------------------
    // Constructor guard-clauses
    // -------------------------------------------------------------------------

    [Fact]
    public void Constructor_ThrowsArgumentNullException_WhenLoggerIsNull()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new EventHandlerRouter(
                null!,
                new Mock<IWorkerEventHandler>().Object,
                new Dictionary<string, IWorkerEventHandler>()));
    }

    [Fact]
    public void Constructor_ThrowsArgumentNullException_WhenFallbackIsNull()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new EventHandlerRouter(
                NullLogger<EventHandlerRouter>.Instance,
                null!,
                new Dictionary<string, IWorkerEventHandler>()));
    }

    [Fact]
    public void Constructor_ThrowsArgumentNullException_WhenHandlersDictionaryIsNull()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new EventHandlerRouter(
                NullLogger<EventHandlerRouter>.Instance,
                new Mock<IWorkerEventHandler>().Object,
                null!));
    }

    // -------------------------------------------------------------------------
    // Routing behaviour
    // -------------------------------------------------------------------------

    [Fact]
    public async Task HandleAsync_RoutesToRegisteredHandler_WhenEventTypeMatches()
    {
        var specificHandler = new Mock<IWorkerEventHandler>();
        specificHandler
            .Setup(h => h.HandleAsync(It.IsAny<Guid>(), It.IsAny<StreamEntry>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var fallback = new Mock<IWorkerEventHandler>();
        var router   = BuildRouter(fallback.Object, ("user.created", specificHandler.Object));

        var eventId = Guid.NewGuid();
        var entry   = BuildEntry("user.created", eventId);

        await router.HandleAsync(eventId, entry, "read-new", CancellationToken.None);

        specificHandler.Verify(
            h => h.HandleAsync(eventId, entry, "read-new", CancellationToken.None),
            Times.Once);

        fallback.Verify(
            h => h.HandleAsync(It.IsAny<Guid>(), It.IsAny<StreamEntry>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task HandleAsync_RoutingIsCaseInsensitive()
    {
        var specificHandler = new Mock<IWorkerEventHandler>();
        specificHandler
            .Setup(h => h.HandleAsync(It.IsAny<Guid>(), It.IsAny<StreamEntry>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var fallback = new Mock<IWorkerEventHandler>();
        var router   = BuildRouter(fallback.Object, ("user.created", specificHandler.Object));

        var entry = BuildEntry("USER.CREATED", Guid.NewGuid()); // uppercase

        await router.HandleAsync(Guid.NewGuid(), entry, "read-new", CancellationToken.None);

        specificHandler.Verify(
            h => h.HandleAsync(It.IsAny<Guid>(), It.IsAny<StreamEntry>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task HandleAsync_FallsBackToNoop_WhenEventTypeNotRegistered()
    {
        var specificHandler = new Mock<IWorkerEventHandler>();
        var fallback        = new Mock<IWorkerEventHandler>();
        fallback
            .Setup(h => h.HandleAsync(It.IsAny<Guid>(), It.IsAny<StreamEntry>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var router = BuildRouter(fallback.Object, ("user.created", specificHandler.Object));

        var entry = BuildEntry("order.created", Guid.NewGuid()); // unregistered type

        await router.HandleAsync(Guid.NewGuid(), entry, "read-new", CancellationToken.None);

        fallback.Verify(
            h => h.HandleAsync(It.IsAny<Guid>(), It.IsAny<StreamEntry>(), "read-new", CancellationToken.None),
            Times.Once);

        specificHandler.Verify(
            h => h.HandleAsync(It.IsAny<Guid>(), It.IsAny<StreamEntry>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task HandleAsync_FallsBackToNoop_WhenEventTypeFieldAbsent()
    {
        var specificHandler = new Mock<IWorkerEventHandler>();
        var fallback        = new Mock<IWorkerEventHandler>();
        fallback
            .Setup(h => h.HandleAsync(It.IsAny<Guid>(), It.IsAny<StreamEntry>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var router = BuildRouter(fallback.Object, ("user.created", specificHandler.Object));

        // StreamEntry with no event_type field at all
        var entry = new StreamEntry("1-0", [new NameValueEntry("event_id", Guid.NewGuid().ToString())]);

        await router.HandleAsync(Guid.NewGuid(), entry, "drain-pending", CancellationToken.None);

        fallback.Verify(
            h => h.HandleAsync(It.IsAny<Guid>(), It.IsAny<StreamEntry>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task HandleAsync_PropagatesException_WhenRegisteredHandlerThrows()
    {
        var specificHandler = new Mock<IWorkerEventHandler>();
        specificHandler
            .Setup(h => h.HandleAsync(It.IsAny<Guid>(), It.IsAny<StreamEntry>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("handler error"));

        var router = BuildRouter(new Mock<IWorkerEventHandler>().Object, ("user.created", specificHandler.Object));
        var entry  = BuildEntry("user.created", Guid.NewGuid());

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => router.HandleAsync(Guid.NewGuid(), entry, "read-new", CancellationToken.None));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static EventHandlerRouter BuildRouter(
        IWorkerEventHandler fallback,
        params (string EventType, IWorkerEventHandler Handler)[] registrations)
    {
        var dict = new Dictionary<string, IWorkerEventHandler>(StringComparer.OrdinalIgnoreCase);
        foreach (var (eventType, handler) in registrations)
            dict[eventType] = handler;

        return new EventHandlerRouter(NullLogger<EventHandlerRouter>.Instance, fallback, dict);
    }

    private static StreamEntry BuildEntry(string eventType, Guid eventId) =>
        new("1-0",
        [
            new NameValueEntry("event_type", eventType),
            new NameValueEntry("event_id",   eventId.ToString()),
            new NameValueEntry("message",    $"{{\"event_id\":\"{eventId}\",\"event_type\":\"{eventType}\",\"payload\":{{}}}}"),
        ]);
}
