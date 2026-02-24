using System.Text.Json;
using Dapper;
using EventPlatform.Domain.Events;
using EventPlatform.Infrastructure.Persistence.DataAccess;
using EventPlatform.Infrastructure.Persistence.Exceptions;
using EventPlatform.Infrastructure.Persistence.Internal;

namespace EventPlatform.Infrastructure.Persistence.Repositories;

/// <summary>
/// Implementation of <see cref="IOutboxRepository"/> using Dapper for PostgreSQL.
/// </summary>
public sealed class OutboxRepository : IOutboxRepository
{
    private readonly IDbConnectionFactory _connectionFactory;

    /// <summary>
    /// Initializes a new instance of the <see cref="OutboxRepository"/> class.
    /// </summary>
    /// <param name="connectionFactory">The database connection factory.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="connectionFactory"/> is null.</exception>
    public OutboxRepository(IDbConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
    }

    /// <summary>
    /// Inserts a new outbox event into the database.
    /// </summary>
    public async Task InsertAsync(OutboxEvent outboxEvent, CancellationToken cancellationToken = default)
    {
        if (outboxEvent == null)
            throw new ArgumentNullException(nameof(outboxEvent));

        cancellationToken.ThrowIfCancellationRequested();

        using var connection = _connectionFactory.CreateConnection();
        connection.Open();

        var parameters = new
        {
            outboxEvent.Id,
            outboxEvent.EventId,
            outboxEvent.StreamName,
            Payload = outboxEvent.Payload.RootElement.ToString(),
            outboxEvent.CreatedAt,
            outboxEvent.PublishedAt,
            outboxEvent.PublishAttempts,
            outboxEvent.LastError
        };

        var command = new CommandDefinition(
            OutboxQueries.InsertOutboxEvent,
            parameters,
            commandTimeout: 30,
            cancellationToken: cancellationToken);

        try
        {
            await connection.ExecuteAsync(command);
        }
        catch (Exception ex)
        {
            if (TryMapException(ex, out var mapped))
                throw mapped;

            throw;
        }
    }

    /// <summary>
    /// Retrieves unpublished outbox events with pagination.
    /// </summary>
    public async Task<IReadOnlyList<OutboxEvent>> GetUnpublishedAsync(
        int limit = 100,
        CancellationToken cancellationToken = default)
    {
        if (limit <= 0)
            throw new ArgumentOutOfRangeException(nameof(limit), "Limit must be greater than zero");

        cancellationToken.ThrowIfCancellationRequested();

        using var connection = _connectionFactory.CreateConnection();
        connection.Open();

        var parameters = new { Limit = limit };

        var command = new CommandDefinition(
            OutboxQueries.GetUnpublished,
            parameters,
            commandTimeout: 30,
            cancellationToken: cancellationToken);

        try
        {
            var results = await connection.QueryAsync<OutboxEventDto>(command);
            return results
                .Select(dto => dto.ToOutboxEvent())
                .ToList()
                .AsReadOnly();
        }
        catch (Exception ex)
        {
            if (TryMapException(ex, out var mapped))
                throw mapped;

            throw;
        }
    }

    /// <summary>
    /// Marks an outbox event as published.
    /// </summary>
    public async Task MarkPublishedAsync(Guid outboxId, CancellationToken cancellationToken = default)
    {
        if (outboxId == Guid.Empty)
            throw new ArgumentException("OutboxId cannot be empty", nameof(outboxId));

        cancellationToken.ThrowIfCancellationRequested();

        using var connection = _connectionFactory.CreateConnection();
        connection.Open();

        var parameters = new
        {
            Id = outboxId,
            PublishedAt = DateTimeOffset.UtcNow
        };

        var command = new CommandDefinition(
            OutboxQueries.MarkPublished,
            parameters,
            commandTimeout: 30,
            cancellationToken: cancellationToken);

        try
        {
            await connection.ExecuteAsync(command);
        }
        catch (Exception ex)
        {
            if (TryMapException(ex, out var mapped))
                throw mapped;

            throw;
        }
    }

    /// <summary>
    /// Records a failed publish attempt.
    /// </summary>
    public async Task RecordPublishAttemptAsync(
        Guid outboxId,
        string? error = null,
        CancellationToken cancellationToken = default)
    {
        if (outboxId == Guid.Empty)
            throw new ArgumentException("OutboxId cannot be empty", nameof(outboxId));

        cancellationToken.ThrowIfCancellationRequested();

        using var connection = _connectionFactory.CreateConnection();
        connection.Open();

        var parameters = new
        {
            Id = outboxId,
            Error = error ?? string.Empty
        };

        var command = new CommandDefinition(
            OutboxQueries.RecordPublishAttempt,
            parameters,
            commandTimeout: 30,
            cancellationToken: cancellationToken);

        try
        {
            await connection.ExecuteAsync(command);
        }
        catch (Exception ex)
        {
            if (TryMapException(ex, out var mapped))
                throw mapped;

            throw;
        }
    }

    /// <summary>
    /// Deletes published outbox events older than the specified time.
    /// </summary>
    public async Task<long> DeletePublishedAsync(
        DateTimeOffset olderThan,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        using var connection = _connectionFactory.CreateConnection();
        connection.Open();

        var parameters = new { OlderThan = olderThan };

        var command = new CommandDefinition(
            OutboxQueries.DeletePublished,
            parameters,
            commandTimeout: 30,
            cancellationToken: cancellationToken);

        try
        {
            return await connection.ExecuteAsync(command);
        }
        catch (Exception ex)
        {
            if (TryMapException(ex, out var mapped))
                throw mapped;

            throw;
        }
    }

    private static bool TryMapException(Exception exception, out Exception mapped)
    {
        return TryMapSqlState(exception, out mapped) || TryMapNetwork(exception, out mapped);
    }

    private static bool TryMapSqlState(Exception exception, out Exception mapped)
    {
        if (TryGetSqlState(exception, out var sqlState, out _))
        {
            if (sqlState == "57P03")
            {
                mapped = new EventRepositoryTransientException(
                    "Database is temporarily unavailable.",
                    exception);
                return true;
            }
        }

        mapped = null!;
        return false;
    }

    private static bool TryMapNetwork(Exception exception, out Exception mapped)
    {
        if (IsTimeout(exception))
        {
            mapped = new EventRepositoryTransientException(
                "Database operation timed out.",
                exception);
            return true;
        }

        mapped = null!;
        return false;
    }

    private static bool TryGetSqlState(Exception exception, out string? sqlState, out string? constraintName)
    {
        if (exception.Data is not null)
        {
            sqlState = exception.Data["SqlState"] as string;
            constraintName = exception.Data["ConstraintName"] as string;
            return !string.IsNullOrWhiteSpace(sqlState);
        }

        sqlState = null;
        constraintName = null;
        return false;
    }

    private static bool IsTimeout(Exception? exception) => exception is TimeoutException;

    /// <summary>
    /// Data transfer object for mapping database rows to OutboxEvent entities.
    /// </summary>
    private sealed class OutboxEventDto
    {
        public Guid Id { get; set; }
        public Guid EventId { get; set; }
        public string StreamName { get; set; } = null!;
        public string Payload { get; set; } = null!;
        public DateTimeOffset CreatedAt { get; set; }
        public DateTimeOffset? PublishedAt { get; set; }
        public int PublishAttempts { get; set; }
        public string? LastError { get; set; }

        /// <summary>
        /// Converts this DTO to an OutboxEvent domain entity.
        /// </summary>
        public OutboxEvent ToOutboxEvent()
        {
            var payloadDocument = JsonDocument.Parse(Payload);
            return new OutboxEvent(
                id: Id,
                eventId: EventId,
                streamName: StreamName,
                payload: payloadDocument,
                createdAt: CreatedAt,
                publishedAt: PublishedAt,
                publishAttempts: PublishAttempts,
                lastError: LastError);
        }
    }
}
