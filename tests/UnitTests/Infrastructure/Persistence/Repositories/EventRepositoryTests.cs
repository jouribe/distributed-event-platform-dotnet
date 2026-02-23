using System.Text.Json;
using Moq;
using System.Data;
using System.Data.Common;
using System.Collections;
using System.Diagnostics.CodeAnalysis;
using EventPlatform.Domain.Events;
using EventPlatform.Infrastructure.Persistence.DataAccess;
using EventPlatform.Infrastructure.Persistence.Exceptions;
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
    public async Task InsertAsync_ThrowsOperationCanceledException_WhenCancellationRequested()
    {
        // Arrange
        var cancellationToken = CreateCanceledToken();

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(
            () => _sut.InsertAsync(CreateValidEventEnvelope(), cancellationToken));
    }

    [Fact]
    public async Task InsertAsync_OpensConnection_WhenInsertingEvent()
    {
        // Arrange
        var fakeConnection = new FakeDbConnection();

        _mockConnectionFactory
            .Setup(cf => cf.CreateConnection())
            .Returns(fakeConnection);

        var envelope = CreateValidEventEnvelope();

        // Act
        await _sut.InsertAsync(envelope);

        // Assert
        Assert.Equal(1, fakeConnection.OpenCount);
    }

    [Fact]
    public async Task InsertAsync_ThrowsConflictException_WhenUniqueViolationOccurs()
    {
        // Arrange
        var fakeConnection = CreateConnectionThatThrows(
            CreateSqlStateException("23505", "events_tenant_id_idempotency_key_key"));

        _mockConnectionFactory
            .Setup(cf => cf.CreateConnection())
            .Returns(fakeConnection);

        // Act & Assert
        var exception = await Assert.ThrowsAsync<EventRepositoryConflictException>(
            () => _sut.InsertAsync(CreateValidEventEnvelope()));

        Assert.Equal("events_tenant_id_idempotency_key_key", exception.ConstraintName);
    }

    [Fact]
    public async Task InsertAsync_ThrowsTransientException_WhenDatabaseUnavailable()
    {
        // Arrange
        var fakeConnection = CreateConnectionThatThrows(CreateSqlStateException("57P03"));

        _mockConnectionFactory
            .Setup(cf => cf.CreateConnection())
            .Returns(fakeConnection);

        // Act & Assert
        await Assert.ThrowsAsync<EventRepositoryTransientException>(
            () => _sut.InsertAsync(CreateValidEventEnvelope()));
    }

    [Fact]
    public async Task UpdateStatusAsync_OpensConnection_WhenUpdatingStatus()
    {
        // Arrange
        var fakeConnection = new FakeDbConnection();

        _mockConnectionFactory
            .Setup(cf => cf.CreateConnection())
            .Returns(fakeConnection);

        var eventId = Guid.NewGuid();

        // Act
        await _sut.UpdateStatusAsync(eventId, EventStatus.QUEUED);

        // Assert
        Assert.Equal(1, fakeConnection.OpenCount);
    }

    [Fact]
    public async Task UpdateStatusAsync_ThrowsOperationCanceledException_WhenCancellationRequested()
    {
        // Arrange
        var cancellationToken = CreateCanceledToken();

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(
            () => _sut.UpdateStatusAsync(Guid.NewGuid(), EventStatus.QUEUED, cancellationToken));
    }

    [Fact]
    public async Task UpdateStatusAsync_ThrowsTransientException_WhenTimeoutOccurs()
    {
        // Arrange
        var fakeConnection = CreateConnectionThatThrows(new TimeoutException("timeout"));

        _mockConnectionFactory
            .Setup(cf => cf.CreateConnection())
            .Returns(fakeConnection);

        // Act & Assert
        await Assert.ThrowsAsync<EventRepositoryTransientException>(
            () => _sut.UpdateStatusAsync(Guid.NewGuid(), EventStatus.QUEUED));
    }

    [Fact]
    public async Task IncrementAttemptsAsync_OpensConnection_WhenIncrementingAttempts()
    {
        // Arrange
        var fakeConnection = new FakeDbConnection();

        _mockConnectionFactory
            .Setup(cf => cf.CreateConnection())
            .Returns(fakeConnection);

        var eventId = Guid.NewGuid();

        // Act
        await _sut.IncrementAttemptsAsync(eventId);

        // Assert
        Assert.Equal(1, fakeConnection.OpenCount);
    }

    [Fact]
    public async Task IncrementAttemptsAsync_ThrowsOperationCanceledException_WhenCancellationRequested()
    {
        // Arrange
        var cancellationToken = CreateCanceledToken();

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(
            () => _sut.IncrementAttemptsAsync(Guid.NewGuid(), cancellationToken));
    }

    [Fact]
    public async Task GetByIdAsync_OpensConnection_WhenRetrievingEvent()
    {
        // Arrange
        var fakeConnection = new FakeDbConnection();

        _mockConnectionFactory
            .Setup(cf => cf.CreateConnection())
            .Returns(fakeConnection);

        var eventId = Guid.NewGuid();

        // Act
        var result = await _sut.GetByIdAsync(eventId);

        // Assert
        Assert.Equal(1, fakeConnection.OpenCount);
    }

    [Fact]
    public async Task GetByIdAsync_ThrowsOperationCanceledException_WhenCancellationRequested()
    {
        // Arrange
        var cancellationToken = CreateCanceledToken();

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(
            () => _sut.GetByIdAsync(Guid.NewGuid(), cancellationToken));
    }

    [Fact]
    public async Task GetRetryableEventsAsync_OpensConnection_WhenRetrievingRetryableEvents()
    {
        // Arrange
        var fakeConnection = new FakeDbConnection();

        _mockConnectionFactory
            .Setup(cf => cf.CreateConnection())
            .Returns(fakeConnection);

        var now = DateTimeOffset.UtcNow;

        // Act
        var result = await _sut.GetRetryableEventsAsync(now, pageSize: 1000, skip: 0);

        // Assert
        Assert.Equal(1, fakeConnection.OpenCount);
        Assert.NotNull(result);
        Assert.Equal(0, result.Count);
        Assert.False(result.HasMore);
        Assert.Equal(1000, result.PageSize);
        Assert.Equal(0, result.Skip);
    }

    [Fact]
    public async Task GetRetryableEventsAsync_ThrowsOperationCanceledException_WhenCancellationRequested()
    {
        // Arrange
        var cancellationToken = CreateCanceledToken();

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(
            () => _sut.GetRetryableEventsAsync(DateTimeOffset.UtcNow, cancellationToken: cancellationToken));
    }

    [Fact]
    public async Task GetRetryableEventsAsync_ThrowsArgumentOutOfRangeException_WhenPageSizeIsInvalid()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => _sut.GetRetryableEventsAsync(DateTimeOffset.UtcNow, pageSize: 0, skip: 0));
    }

    [Fact]
    public async Task GetRetryableEventsAsync_ThrowsArgumentOutOfRangeException_WhenSkipIsNegative()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => _sut.GetRetryableEventsAsync(DateTimeOffset.UtcNow, pageSize: 10, skip: -1));
    }

    [Fact]
    public async Task GetCountAsync_OpensConnection_WhenCountingByStatus()
    {
        // Arrange
        var fakeConnection = new FakeDbConnection();

        _mockConnectionFactory
            .Setup(cf => cf.CreateConnection())
            .Returns(fakeConnection);

        // Act
        var result = await _sut.GetCountAsync(EventStatus.SUCCEEDED);

        // Assert
        Assert.Equal(1, fakeConnection.OpenCount);
        Assert.Equal(0L, result);
    }

    [Fact]
    public async Task GetCountAsync_ThrowsOperationCanceledException_WhenCancellationRequested()
    {
        // Arrange
        var cancellationToken = CreateCanceledToken();

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(
            () => _sut.GetCountAsync(EventStatus.SUCCEEDED, cancellationToken: cancellationToken));
    }

    [Fact]
    public async Task GetCountAsync_ThrowsArgumentException_WhenTenantIdIsBlank()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(
            () => _sut.GetCountAsync(EventStatus.SUCCEEDED, tenantId: "  "));
    }

    [Fact]
    public async Task GetByCorrelationIdAsync_OpensConnection_WhenRetrievingEvents()
    {
        // Arrange
        var fakeConnection = new FakeDbConnection();

        _mockConnectionFactory
            .Setup(cf => cf.CreateConnection())
            .Returns(fakeConnection);

        // Act
        var result = await _sut.GetByCorrelationIdAsync(Guid.NewGuid());

        // Assert
        Assert.Equal(1, fakeConnection.OpenCount);
        Assert.NotNull(result);
        Assert.Empty(result);
    }

    [Fact]
    public async Task GetByCorrelationIdAsync_ThrowsOperationCanceledException_WhenCancellationRequested()
    {
        // Arrange
        var cancellationToken = CreateCanceledToken();

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(
            () => _sut.GetByCorrelationIdAsync(Guid.NewGuid(), cancellationToken));
    }

    [Fact]
    public async Task GetByCorrelationIdAsync_ThrowsArgumentException_WhenCorrelationIdIsEmpty()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(
            () => _sut.GetByCorrelationIdAsync(Guid.Empty));
    }

    [Fact]
    public async Task GetByTenantIdAsync_OpensConnection_WhenRetrievingTenantEvents()
    {
        // Arrange
        var fakeConnection = new FakeDbConnection();

        _mockConnectionFactory
            .Setup(cf => cf.CreateConnection())
            .Returns(fakeConnection);

        // Act
        var result = await _sut.GetByTenantIdAsync("test-tenant", pageSize: 25, skip: 0);

        // Assert
        Assert.Equal(1, fakeConnection.OpenCount);
        Assert.NotNull(result);
        Assert.Empty(result);
    }

    [Fact]
    public async Task GetByTenantIdAsync_ThrowsOperationCanceledException_WhenCancellationRequested()
    {
        // Arrange
        var cancellationToken = CreateCanceledToken();

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(
            () => _sut.GetByTenantIdAsync("test-tenant", cancellationToken: cancellationToken));
    }

    [Fact]
    public async Task GetByTenantIdAsync_ThrowsArgumentException_WhenTenantIdIsBlank()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(
            () => _sut.GetByTenantIdAsync(" "));
    }

    [Fact]
    public async Task GetByTenantIdAsync_ThrowsArgumentOutOfRangeException_WhenPageSizeIsInvalid()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => _sut.GetByTenantIdAsync("test-tenant", pageSize: 0, skip: 0));
    }

    [Fact]
    public async Task GetByTenantIdAsync_ThrowsArgumentOutOfRangeException_WhenSkipIsNegative()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => _sut.GetByTenantIdAsync("test-tenant", pageSize: 10, skip: -1));
    }

    [Fact]
    public async Task GetOldestRetryableAsync_OpensConnection_WhenRetrievingOldestRetryableEvent()
    {
        // Arrange
        var fakeConnection = new FakeDbConnection();

        _mockConnectionFactory
            .Setup(cf => cf.CreateConnection())
            .Returns(fakeConnection);

        // Act
        var result = await _sut.GetOldestRetryableAsync(DateTimeOffset.UtcNow);

        // Assert
        Assert.Equal(1, fakeConnection.OpenCount);
        Assert.Null(result);
    }

    [Fact]
    public async Task GetOldestRetryableAsync_ThrowsOperationCanceledException_WhenCancellationRequested()
    {
        // Arrange
        var cancellationToken = CreateCanceledToken();

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(
            () => _sut.GetOldestRetryableAsync(DateTimeOffset.UtcNow, cancellationToken));
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

    private static CancellationToken CreateCanceledToken()
    {
        var source = new CancellationTokenSource();
        source.Cancel();
        return source.Token;
    }

    private static FakeDbConnection CreateConnectionThatThrows(Exception exception)
    {
        return new FakeDbConnection { ExceptionToThrow = exception };
    }

    private static Exception CreateSqlStateException(string sqlState, string? constraintName = null)
    {
        var exception = new Exception("Database error");
        exception.Data["SqlState"] = sqlState;

        if (!string.IsNullOrWhiteSpace(constraintName))
            exception.Data["ConstraintName"] = constraintName;

        return exception;
    }
}

internal sealed class FakeDbConnection : DbConnection
{
    private ConnectionState _state = ConnectionState.Closed;

    public int OpenCount { get; private set; }

    public Exception? ExceptionToThrow { get; set; }

    [AllowNull]
    public override string ConnectionString { get; set; } = string.Empty;

    public override string Database => "Fake";

    public override string DataSource => "Fake";

    public override string ServerVersion => "1.0";

    public override ConnectionState State => _state;

    public override void Open()
    {
        _state = ConnectionState.Open;
        OpenCount += 1;
    }

    public override void Close() => _state = ConnectionState.Closed;

    public override void ChangeDatabase(string databaseName) { }

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => null!;

    protected override DbCommand CreateDbCommand() => new FakeDbCommand(this, ExceptionToThrow);
}

internal sealed class FakeDbCommand : DbCommand
{
    private readonly DbConnection _connection;
    private readonly DbParameterCollection _parameters = new FakeDbParameterCollection();
    private readonly Exception? _exceptionToThrow;

    public FakeDbCommand(DbConnection connection, Exception? exceptionToThrow)
    {
        _connection = connection;
        _exceptionToThrow = exceptionToThrow;
    }

    [AllowNull]
    public override string CommandText { get; set; } = string.Empty;

    public override int CommandTimeout { get; set; }

    public override CommandType CommandType { get; set; }

    public override UpdateRowSource UpdatedRowSource { get; set; }

    [AllowNull]
    protected override DbConnection DbConnection
    {
        get => _connection;
        set { }
    }

    protected override DbParameterCollection DbParameterCollection => _parameters;

    protected override DbTransaction? DbTransaction { get; set; }

    public override bool DesignTimeVisible { get; set; }

    public override void Cancel() { }

    public override int ExecuteNonQuery()
    {
        ThrowIfConfigured();
        return 1;
    }

    public override object? ExecuteScalar()
    {
        ThrowIfConfigured();
        return null;
    }

    public override void Prepare() { }

    protected override DbParameter CreateDbParameter() => new FakeDbParameter();

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        ThrowIfConfigured();
        return new DataTable().CreateDataReader();
    }

    public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        ThrowIfConfigured();
        return Task.FromResult(1);
    }

    public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
    {
        ThrowIfConfigured();
        return Task.FromResult<object?>(null);
    }

    protected override Task<DbDataReader> ExecuteDbDataReaderAsync(
        CommandBehavior behavior,
        CancellationToken cancellationToken)
    {
        ThrowIfConfigured();
        return Task.FromResult<DbDataReader>(new DataTable().CreateDataReader());
    }

    private void ThrowIfConfigured()
    {
        if (_exceptionToThrow != null)
            throw _exceptionToThrow;
    }
}

internal sealed class FakeDbParameter : DbParameter
{
    public override DbType DbType { get; set; }

    public override ParameterDirection Direction { get; set; }

    public override bool IsNullable { get; set; }

    [AllowNull]
    public override string ParameterName { get; set; } = string.Empty;

    [AllowNull]
    public override string SourceColumn { get; set; } = string.Empty;

    public override object? Value { get; set; }

    public override bool SourceColumnNullMapping { get; set; }

    public override int Size { get; set; }

    public override void ResetDbType() { }
}

internal sealed class FakeDbParameterCollection : DbParameterCollection
{
    private readonly List<DbParameter> _parameters = new();

    public override int Count => _parameters.Count;

    public override object SyncRoot => ((ICollection)_parameters).SyncRoot;

    public override int Add(object value)
    {
        _parameters.Add((DbParameter)value);
        return _parameters.Count - 1;
    }

    public override void AddRange(Array values)
    {
        foreach (var value in values)
            Add(value!);
    }

    public override void Clear() => _parameters.Clear();

    public override bool Contains(object value) => _parameters.Contains((DbParameter)value);

    public override bool Contains(string value) => _parameters.Any(p => p.ParameterName == value);

    public override void CopyTo(Array array, int index) => _parameters.ToArray().CopyTo(array, index);

    public override IEnumerator GetEnumerator() => _parameters.GetEnumerator();

    public override int IndexOf(object value) => _parameters.IndexOf((DbParameter)value);

    public override int IndexOf(string parameterName) => _parameters.FindIndex(p => p.ParameterName == parameterName);

    public override void Insert(int index, object value) => _parameters.Insert(index, (DbParameter)value);

    public override void Remove(object value) => _parameters.Remove((DbParameter)value);

    public override void RemoveAt(int index) => _parameters.RemoveAt(index);

    public override void RemoveAt(string parameterName)
    {
        var index = IndexOf(parameterName);
        if (index >= 0)
            RemoveAt(index);
    }

    protected override DbParameter GetParameter(int index) => _parameters[index];

    protected override DbParameter GetParameter(string parameterName)
        => _parameters.First(p => p.ParameterName == parameterName);

    protected override void SetParameter(int index, DbParameter value) => _parameters[index] = value;

    protected override void SetParameter(string parameterName, DbParameter value)
    {
        var index = IndexOf(parameterName);
        if (index >= 0)
            _parameters[index] = value;
        else
            _parameters.Add(value);
    }
}
