using System.Text.Json;
using EventPlatform.Infrastructure.Persistence.DataAccess;
using EventWorker.Handlers;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using StackExchange.Redis;

namespace EventPlatform.UnitTests.Worker.Handlers;

public sealed class UserCreatedEventHandlerTests
{
    // -------------------------------------------------------------------------
    // Constructor guard-clauses
    // -------------------------------------------------------------------------

    [Fact]
    public void Constructor_ThrowsArgumentNullException_WhenConnectionFactoryIsNull()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new UserCreatedEventHandler(null!, NullLogger<UserCreatedEventHandler>.Instance));
    }

    [Fact]
    public void Constructor_ThrowsArgumentNullException_WhenLoggerIsNull()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new UserCreatedEventHandler(new Mock<IDbConnectionFactory>().Object, null!));
    }

    // -------------------------------------------------------------------------
    // Payload extraction / validation
    // -------------------------------------------------------------------------

    [Fact]
    public async Task HandleAsync_ThrowsInvalidOperationException_WhenMessageFieldMissing()
    {
        var handler = BuildHandler();
        var entry   = new StreamEntry("1-0", [new NameValueEntry("event_type", "user.created")]);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => handler.HandleAsync(Guid.NewGuid(), entry, "read-new", CancellationToken.None));
    }

    [Theory]
    [InlineData("user_id")]
    [InlineData("email")]
    [InlineData("name")]
    public async Task HandleAsync_ThrowsInvalidOperationException_WhenRequiredPayloadFieldMissing(string missingField)
    {
        var handler = BuildHandler();
        var entry   = BuildEntry(userId: "usr-001", email: "a@b.com", name: "Alice", omitField: missingField);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => handler.HandleAsync(Guid.NewGuid(), entry, "read-new", CancellationToken.None));
    }

    [Fact]
    public async Task HandleAsync_ThrowsOperationCanceledException_WhenTokenAlreadyCancelled()
    {
        var handler = BuildHandler();
        var entry   = BuildEntry("usr-001", "a@b.com", "Alice");
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => handler.HandleAsync(Guid.NewGuid(), entry, "read-new", cts.Token));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /// <summary>
    /// Builds a handler whose connection factory returns a mock that never executes SQL.
    /// Useful for testing validation paths that throw before reaching the DB.
    /// </summary>
    private static UserCreatedEventHandler BuildHandler()
        => new(new Mock<IDbConnectionFactory>().Object, NullLogger<UserCreatedEventHandler>.Instance);

    private static StreamEntry BuildEntry(
        string userId,
        string email,
        string name,
        string tenantId    = "tenant-1",
        Guid?  eventId     = null,
        string? omitField  = null)
    {
        var id = eventId ?? Guid.NewGuid();

        var payloadDict = new Dictionary<string, object?>
        {
            ["user_id"] = userId,
            ["email"]   = email,
            ["name"]    = name,
        };

        if (omitField is not null)
            payloadDict.Remove(omitField);

        var message = JsonSerializer.Serialize(new
        {
            event_id   = id,
            event_type = "user.created",
            payload    = payloadDict,
        });

        return new StreamEntry("1-0",
        [
            new NameValueEntry("event_id",   id.ToString()),
            new NameValueEntry("event_type", "user.created"),
            new NameValueEntry("tenant_id",  tenantId),
            new NameValueEntry("message",    message),
        ]);
    }
}
