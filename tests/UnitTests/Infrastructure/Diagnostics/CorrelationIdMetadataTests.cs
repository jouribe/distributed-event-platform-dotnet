using System.Text.Json;
using EventPlatform.Infrastructure.Diagnostics;
using StackExchange.Redis;

namespace EventPlatform.UnitTests.Infrastructure.Diagnostics;

public sealed class CorrelationIdMetadataTests
{
    [Fact]
    public void TryReadFromOutboxPayload_ReturnsCorrelationId_WhenPayloadContainsValidGuid()
    {
        var correlationId = Guid.NewGuid();
        using var payload = JsonDocument.Parse($$"""
        {
            "correlation_id": "{{correlationId}}"
        }
        """);

        var result = CorrelationIdMetadata.TryReadFromOutboxPayload(payload);

        Assert.Equal(correlationId, result);
    }

    [Fact]
    public void TryReadFromStreamEntry_ReturnsTopLevelCorrelationId_WhenFieldExists()
    {
        var correlationId = Guid.NewGuid();
        var entry = new StreamEntry(
            "1700000000000-0",
            [new NameValueEntry("correlation_id", correlationId.ToString())]);

        var result = CorrelationIdMetadata.TryReadFromStreamEntry(entry);

        Assert.Equal(correlationId, result);
    }

    [Fact]
    public void TryReadFromStreamEntry_FallsBackToMessageBody_WhenTopLevelFieldIsMissing()
    {
        var correlationId = Guid.NewGuid();
        var entry = new StreamEntry(
            "1700000000000-0",
            [new NameValueEntry("message", $$"""
            {
                "correlation_id": "{{correlationId}}"
            }
            """)]);

        var result = CorrelationIdMetadata.TryReadFromStreamEntry(entry);

        Assert.Equal(correlationId, result);
    }

    [Fact]
    public void TryReadFromStreamEntry_ReturnsNull_WhenCorrelationIdIsMissing()
    {
        var entry = new StreamEntry(
            "1700000000000-0",
            [new NameValueEntry("message", "{}")]);

        var result = CorrelationIdMetadata.TryReadFromStreamEntry(entry);

        Assert.Null(result);
    }

    [Fact]
    public void TryReadFromStreamEntry_ReturnsNull_WhenMessageJsonRootIsNotAnObject()
    {
        var entry = new StreamEntry(
            "1700000000000-0",
            [new NameValueEntry("message", "\"not-an-object\"")]);

        var result = CorrelationIdMetadata.TryReadFromStreamEntry(entry);

        Assert.Null(result);
    }
}
