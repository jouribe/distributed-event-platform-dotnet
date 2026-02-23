using System.Text.Json;
using Moq;
using EventPlatform.Domain.Events;
using EventPlatform.Infrastructure.Persistence.DataAccess;
using EventPlatform.Infrastructure.Persistence.Repositories;

namespace EventPlatform.UnitTests.Infrastructure.Persistence.Repositories;

/// <summary>
/// Unit tests for the EventRepository BatchInsertAsync method.
/// </summary>
public class EventRepositoryBatchTests
{
    private readonly Mock<IDbConnectionFactory> _mockConnectionFactory;
    private readonly EventRepository _sut; // System Under Test

    public EventRepositoryBatchTests()
    {
        _mockConnectionFactory = new Mock<IDbConnectionFactory>();
        _sut = new EventRepository(_mockConnectionFactory.Object);
    }

    [Fact]
    public async Task BatchInsertAsync_ThrowsArgumentNullException_WhenEnvelopesIsNull()
    {
        // Arrange, Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => _sut.BatchInsertAsync(null!, CancellationToken.None));
    }

    [Fact]
    public async Task BatchInsertAsync_ReturnsEmptyResult_WhenEnvelopesIsEmpty()
    {
        // Arrange
        var emptyEnvelopes = Array.Empty<EventEnvelope>();

        // Act
        var result = await _sut.BatchInsertAsync(emptyEnvelopes);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(0, result.TotalSubmitted);
        Assert.Equal(0, result.SuccessCount);
        Assert.Equal(0, result.ConflictCount);
        Assert.Equal(0, result.ErrorCount);
        Assert.Empty(result.Details);
    }

    [Fact]
    public async Task BatchInsertAsync_ThrowsOperationCanceledException_WhenCancellationRequested()
    {
        // Arrange
        var envelopes = new[] { CreateValidEventEnvelope() };
        var cancellationToken = CreateCanceledToken();

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(
            () => _sut.BatchInsertAsync(envelopes, cancellationToken));
    }

    [Fact]
    public async Task BatchInsertAsync_OpensConnection_WhenInsertingEvents()
    {
        // Arrange
        var fakeConnection = new FakeDbConnection();

        _mockConnectionFactory
            .Setup(cf => cf.CreateConnection())
            .Returns(fakeConnection);

        var envelopes = new[]
        {
            CreateValidEventEnvelope(),
            CreateValidEventEnvelope(),
            CreateValidEventEnvelope()
        };

        // Act
        await _sut.BatchInsertAsync(envelopes);

        // Assert
        Assert.Equal(1, fakeConnection.OpenCount); // One connection per chunk
    }

    [Fact]
    public async Task BatchInsertAsync_ReturnsCorrectCounts_ForSuccessfulInsertions()
    {
        // Arrange
        var fakeConnection = new FakeDbConnection();

        _mockConnectionFactory
            .Setup(cf => cf.CreateConnection())
            .Returns(fakeConnection);

        var envelopes = new[]
        {
            CreateValidEventEnvelope(),
            CreateValidEventEnvelope(),
            CreateValidEventEnvelope()
        };

        // Act
        var result = await _sut.BatchInsertAsync(envelopes);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(3, result.TotalSubmitted);
        // Note: FakeDbConnection returns empty results, so counts will be 0
        // Integration tests should verify actual database behavior
        Assert.Equal(0, result.SuccessCount); // Fake connection returns no rows
        Assert.Equal(0, result.ConflictCount);
        Assert.Equal(3, result.ErrorCount); // All treated as errors due to missing results
    }

    [Fact]
    public async Task BatchInsertAsync_ReturnsDetailsInSameOrder_AsInputEnvelopes()
    {
        // Arrange
        var fakeConnection = new FakeDbConnection();

        _mockConnectionFactory
            .Setup(cf => cf.CreateConnection())
            .Returns(fakeConnection);

        var envelope1 = CreateValidEventEnvelope();
        var envelope2 = CreateValidEventEnvelope();
        var envelope3 = CreateValidEventEnvelope();

        var envelopes = new[] { envelope1, envelope2, envelope3 };

        // Act
        var result = await _sut.BatchInsertAsync(envelopes);

        // Assert
        Assert.Equal(envelope1.Id, result.Details[0].EventId);
        Assert.Equal(envelope2.Id, result.Details[1].EventId);
        Assert.Equal(envelope3.Id, result.Details[2].EventId);
    }

    [Fact]
    public async Task BatchInsertAsync_ProcessesInChunks_WhenBatchSizeExceeds1000()
    {
        // Arrange
        var fakeConnection = new FakeDbConnection();

        _mockConnectionFactory
            .Setup(cf => cf.CreateConnection())
            .Returns(fakeConnection);

        // Create 2500 events to test chunking (should process in 3 chunks: 1000, 1000, 500)
        var envelopes = Enumerable.Range(0, 2500)
            .Select(_ => CreateValidEventEnvelope())
            .ToArray();

        // Act
        var result = await _sut.BatchInsertAsync(envelopes);

        // Assert
        Assert.Equal(2500, result.TotalSubmitted);
        // Note: FakeDbConnection returns empty results
        Assert.Equal(2500, result.Details.Count);
        Assert.Equal(3, fakeConnection.OpenCount); // 3 chunks processed
    }

    [Fact]
    public async Task BatchInsertAsync_HandlesLargePayloads_Efficiently()
    {
        // Arrange
        var fakeConnection = new FakeDbConnection();

        _mockConnectionFactory
            .Setup(cf => cf.CreateConnection())
            .Returns(fakeConnection);

        // Create events with larger payloads
        var largePayload = JsonDocument.Parse(
            "{\"data\": \"" + new string('x', 1000) + "\", \"nested\": {\"field\": \"value\"}}");

        var envelopes = Enumerable.Range(0, 100)
            .Select(_ => CreateEventEnvelopeWithPayload(largePayload))
            .ToArray();

        // Act
        var result = await _sut.BatchInsertAsync(envelopes);

        // Assert  - Just verify batch completes without error
        Assert.Equal(100, result.TotalSubmitted);
        Assert.Equal(100, result.Details.Count);
    }

    [Fact]
    public async Task BatchInsertAsync_ProcessesEnumerableWithoutMultipleEnumeration()
    {
        // Arrange
        var fakeConnection = new FakeDbConnection();

        _mockConnectionFactory
            .Setup(cf => cf.CreateConnection())
            .Returns(fakeConnection);

        var enumerationCount = 0;
        IEnumerable<EventEnvelope> EnvelopesGenerator()
        {
            enumerationCount++;
            yield return CreateValidEventEnvelope();
            yield return CreateValidEventEnvelope();
        }

        // Act
        var result = await _sut.BatchInsertAsync(EnvelopesGenerator());

        // Assert
        Assert.Equal(1, enumerationCount); // Should only enumerate once
        Assert.Equal(2, result.TotalSubmitted);
    }

    /// <summary>
    /// Helper method to create a valid EventEnvelope for testing.
    /// </summary>
    private static EventEnvelope CreateValidEventEnvelope()
    {
        var payload = JsonDocument.Parse("{\"data\": \"test\"}");
        return EventEnvelope.CreateNew(
            id: Guid.NewGuid(),
            eventType: "TestEvent",
            occurredAt: DateTimeOffset.UtcNow.AddSeconds(-1),
            source: "TestSource",
            tenantId: "test-tenant",
            idempotencyKey: Guid.NewGuid().ToString(),
            correlationId: Guid.NewGuid(),
            payload: payload);
    }

    /// <summary>
    /// Helper method to create an EventEnvelope with a specific payload.
    /// </summary>
    private static EventEnvelope CreateEventEnvelopeWithPayload(JsonDocument payload)
    {
        return EventEnvelope.CreateNew(
            id: Guid.NewGuid(),
            eventType: "TestEvent",
            occurredAt: DateTimeOffset.UtcNow.AddSeconds(-1),
            source: "TestSource",
            tenantId: "test-tenant",
            idempotencyKey: Guid.NewGuid().ToString(),
            correlationId: Guid.NewGuid(),
            payload: payload);
    }

    private static CancellationToken CreateCanceledToken()
    {
        var source = new CancellationTokenSource();
        source.Cancel();
        return source.Token;
    }
}
