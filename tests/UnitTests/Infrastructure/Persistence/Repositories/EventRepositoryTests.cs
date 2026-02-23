using System.Text.Json;
using Xunit;
using Moq;
using System.Data;
using EventPlatform.Domain.Events;
using EventPlatform.Infrastructure.Persistence.DataAccess;
using EventPlatform.Infrastructure.Persistence.Repositories;

namespace EventPlatform.UnitTests.Infrastructure.Persistence.Repositories;

/// <summary>
/// Unit tests for the EventRepository class.
/// </summary>
public class EventRepositoryTests
{
    private readonly Mock<IDbConnectionFactory> _mockConnectionFactory;
    private readonly EventRepository _sut; // System Under Test

    public EventRepositoryTests()
    {
        _mockConnectionFactory = new Mock<IDbConnectionFactory>();
        _sut = new EventRepository(_mockConnectionFactory.Object);
    }

    [Fact]
    public void Constructor_ThrowsArgumentNullException_WhenConnectionFactoryIsNull()
    {
        // Arrange, Act & Assert
        Assert.Throws<ArgumentNullException>(
            () => new EventRepository(null!));
    }

    [Fact]
    public async Task InsertAsync_ThrowsArgumentNullException_WhenEnvelopeIsNull()
    {
        // Arrange, Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => _sut.InsertAsync(null!, CancellationToken.None));
    }

    [Fact]
    public async Task InsertAsync_OpensConnection_WhenInsertingEvent()
    {
        // Arrange
        var mockConnection = new Mock<IDbConnection>();
        mockConnection.Setup(c => c.Open())
            .Verifiable();

        _mockConnectionFactory
            .Setup(cf => cf.CreateConnection())
            .Returns(mockConnection.Object);

        var envelope = CreateValidEventEnvelope();

        // Act
        await _sut.InsertAsync(envelope);

        // Assert
        mockConnection.Verify(c => c.Open(), Times.Once);
        mockConnection.Verify(c => c.Dispose(), Times.Once);
    }

    [Fact]
    public async Task UpdateStatusAsync_OpensConnection_WhenUpdatingStatus()
    {
        // Arrange
        var mockConnection = new Mock<IDbConnection>();
        mockConnection.Setup(c => c.Open())
            .Verifiable();

        _mockConnectionFactory
            .Setup(cf => cf.CreateConnection())
            .Returns(mockConnection.Object);

        var eventId = Guid.NewGuid();

        // Act
        await _sut.UpdateStatusAsync(eventId, EventStatus.QUEUED);

        // Assert
        mockConnection.Verify(c => c.Open(), Times.Once);
        mockConnection.Verify(c => c.Dispose(), Times.Once);
    }

    [Fact]
    public async Task IncrementAttemptsAsync_OpensConnection_WhenIncrementingAttempts()
    {
        // Arrange
        var mockConnection = new Mock<IDbConnection>();
        mockConnection.Setup(c => c.Open())
            .Verifiable();

        _mockConnectionFactory
            .Setup(cf => cf.CreateConnection())
            .Returns(mockConnection.Object);

        var eventId = Guid.NewGuid();

        // Act
        await _sut.IncrementAttemptsAsync(eventId);

        // Assert
        mockConnection.Verify(c => c.Open(), Times.Once);
        mockConnection.Verify(c => c.Dispose(), Times.Once);
    }

    [Fact]
    public async Task GetByIdAsync_OpensConnection_WhenRetrievingEvent()
    {
        // Arrange
        var mockConnection = new Mock<IDbConnection>();
        mockConnection.Setup(c => c.Open())
            .Verifiable();

        _mockConnectionFactory
            .Setup(cf => cf.CreateConnection())
            .Returns(mockConnection.Object);

        var eventId = Guid.NewGuid();

        // Act
        var result = await _sut.GetByIdAsync(eventId);

        // Assert
        mockConnection.Verify(c => c.Open(), Times.Once);
        mockConnection.Verify(c => c.Dispose(), Times.Once);
    }

    [Fact]
    public async Task GetRetryableEventsAsync_OpensConnection_WhenRetrievingRetryableEvents()
    {
        // Arrange
        var mockConnection = new Mock<IDbConnection>();
        mockConnection.Setup(c => c.Open())
            .Verifiable();

        _mockConnectionFactory
            .Setup(cf => cf.CreateConnection())
            .Returns(mockConnection.Object);

        var now = DateTimeOffset.UtcNow;

        // Act
        var result = await _sut.GetRetryableEventsAsync(now);

        // Assert
        mockConnection.Verify(c => c.Open(), Times.Once);
        mockConnection.Verify(c => c.Dispose(), Times.Once);
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
}
