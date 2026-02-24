using System.Text.Json;
using Dapper;
using EventPlatform.Domain.Events;
using EventPlatform.Infrastructure.Persistence.DataAccess;
using EventPlatform.Infrastructure.Persistence.Exceptions;
using EventPlatform.Infrastructure.Persistence.Internal;
using Npgsql;

namespace EventPlatform.Infrastructure.Persistence.Repositories;

/// <summary>
/// Implementation of <see cref="IEventRepository"/> using Dapper for PostgreSQL.
/// </summary>
public sealed class EventRepository : IEventRepository
{
    private readonly IDbConnectionFactory _connectionFactory;

    /// <summary>
    /// Initializes a new instance of the <see cref="EventRepository"/> class.
    /// </summary>
    /// <param name="connectionFactory">The database connection factory.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="connectionFactory"/> is null.</exception>
    public EventRepository(IDbConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
    }

    /// <summary>
    /// Inserts a new event envelope into the database.
    /// </summary>
    /// <param name="envelope">The event envelope to insert.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="envelope"/> is null.</exception>
    public async Task InsertAsync(EventEnvelope envelope, CancellationToken cancellationToken = default)
    {
        if (envelope == null)
            throw new ArgumentNullException(nameof(envelope));

        cancellationToken.ThrowIfCancellationRequested();

        using var connection = _connectionFactory.CreateConnection();
        connection.Open();

        var parameters = new
        {
            envelope.Id,
            envelope.TenantId,
            envelope.EventType,
            envelope.OccurredAt,
            envelope.ReceivedAt,
            Payload = envelope.Payload.RootElement.ToString(), // Serialize JsonDocument to string
            envelope.IdempotencyKey,
            envelope.CorrelationId,
            Status = envelope.Status.ToString(),
            envelope.Attempts,
            envelope.NextAttemptAt,
            envelope.LastError
        };

        var command = new CommandDefinition(
            EventQueries.InsertEvent,
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
    /// Inserts a new event envelope and its outbox record in a single atomic transaction.
    /// </summary>
    public async Task InsertWithOutboxAsync(EventEnvelope envelope, OutboxEvent outboxEvent, CancellationToken cancellationToken = default)
    {
        if (envelope == null)
            throw new ArgumentNullException(nameof(envelope));

        if (outboxEvent == null)
            throw new ArgumentNullException(nameof(outboxEvent));

        cancellationToken.ThrowIfCancellationRequested();

        using var connection = _connectionFactory.CreateConnection();
        connection.Open();

        using var transaction = connection.BeginTransaction();

        try
        {
            // Insert event
            var eventParameters = new
            {
                envelope.Id,
                envelope.TenantId,
                envelope.EventType,
                envelope.OccurredAt,
                envelope.ReceivedAt,
                Payload = envelope.Payload.RootElement.ToString(),
                envelope.IdempotencyKey,
                envelope.CorrelationId,
                Status = envelope.Status.ToString(),
                envelope.Attempts,
                envelope.NextAttemptAt,
                envelope.LastError
            };

            var eventCommand = new CommandDefinition(
                EventQueries.InsertEvent,
                eventParameters,
                transaction: transaction,
                commandTimeout: 30,
                cancellationToken: cancellationToken);

            await connection.ExecuteAsync(eventCommand);

            // Insert outbox event
            var outboxParameters = new
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

            var outboxCommand = new CommandDefinition(
                OutboxQueries.InsertOutboxEvent,
                outboxParameters,
                transaction: transaction,
                commandTimeout: 30,
                cancellationToken: cancellationToken);

            await connection.ExecuteAsync(outboxCommand);

            transaction.Commit();
        }
        catch (Exception ex)
        {
            transaction.Rollback();

            if (TryMapException(ex, out var mapped))
                throw mapped;

            throw;
        }
    }

    /// <summary>
    /// <param name="envelopes">The collection of event envelopes to insert.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A batch insert result containing success/conflict/error counts and per-event details.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="envelopes"/> is null.</exception>
    public async Task<BatchInsertResult> BatchInsertAsync(IEnumerable<EventEnvelope> envelopes, CancellationToken cancellationToken = default)
    {
        if (envelopes == null)
            throw new ArgumentNullException(nameof(envelopes));

        cancellationToken.ThrowIfCancellationRequested();

        // Materialize to array to enable batching and preserve order
        var envelopeArray = envelopes.ToArray();

        if (envelopeArray.Length == 0)
        {
            return new BatchInsertResult(
                TotalSubmitted: 0,
                SuccessCount: 0,
                ConflictCount: 0,
                ErrorCount: 0,
                Details: Array.Empty<EventInsertDetail>());
        }

        const int batchSize = 1000;
        var allDetails = new List<EventInsertDetail>(envelopeArray.Length);

        // Process in chunks to respect PostgreSQL parameter limits (~65,000 / 12 fields ≈ 5,400)
        for (int i = 0; i < envelopeArray.Length; i += batchSize)
        {
            var chunk = envelopeArray.Skip(i).Take(batchSize).ToArray();
            var chunkDetails = await BatchInsertChunkAsync(chunk, cancellationToken);
            allDetails.AddRange(chunkDetails);
        }

        var successCount = allDetails.Count(d => d.Outcome == EventInsertOutcome.Success);
        var conflictCount = allDetails.Count(d => d.Outcome == EventInsertOutcome.Conflict);
        var errorCount = allDetails.Count(d => d.Outcome == EventInsertOutcome.Error);

        return new BatchInsertResult(
            TotalSubmitted: envelopeArray.Length,
            SuccessCount: successCount,
            ConflictCount: conflictCount,
            ErrorCount: errorCount,
            Details: allDetails);
    }

    /// <summary>
    /// Inserts a single chunk of events using PostgreSQL UNNEST for efficient batch operations.
    /// </summary>
    private async Task<IReadOnlyList<EventInsertDetail>> BatchInsertChunkAsync(
        EventEnvelope[] chunk,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        using var connection = _connectionFactory.CreateConnection();
        connection.Open();

        // Build arrays for UNNEST
        var ids = new Guid[chunk.Length];
        var tenantIds = new string[chunk.Length];
        var eventTypes = new string[chunk.Length];
        var occurredAts = new DateTime[chunk.Length];
        var receivedAts = new DateTime[chunk.Length];
        var payloads = new string[chunk.Length];
        var idempotencyKeys = new string[chunk.Length];
        var correlationIds = new Guid[chunk.Length];
        var statuses = new string[chunk.Length];
        var attemptsArray = new int[chunk.Length];
        var nextAttemptAts = new DateTime[chunk.Length]; // Use DateTime.MinValue as sentinel for null
        var lastErrors = new string[chunk.Length]; // Use empty string as sentinel for null

        for (int i = 0; i < chunk.Length; i++)
        {
            var envelope = chunk[i];
            ids[i] = envelope.Id;
            tenantIds[i] = envelope.TenantId;
            eventTypes[i] = envelope.EventType;
            occurredAts[i] = envelope.OccurredAt.UtcDateTime;
            receivedAts[i] = envelope.ReceivedAt.UtcDateTime;
            payloads[i] = envelope.Payload.RootElement.ToString();
            idempotencyKeys[i] = envelope.IdempotencyKey ?? string.Empty; // Use empty string as sentinel for null
            correlationIds[i] = envelope.CorrelationId;
            statuses[i] = envelope.Status.ToString();
            attemptsArray[i] = envelope.Attempts;
            nextAttemptAts[i] = envelope.NextAttemptAt?.UtcDateTime ?? DateTime.MinValue; // Sentinel for null
            lastErrors[i] = envelope.LastError ?? string.Empty; // Sentinel for null
        }

        var parameters = new
        {
            Ids = ids,
            TenantIds = tenantIds,
            EventTypes = eventTypes,
            OccurredAts = occurredAts,
            ReceivedAts = receivedAts,
            Payloads = payloads,
            IdempotencyKeys = idempotencyKeys,
            CorrelationIds = correlationIds,
            Statuses = statuses,
            AttemptsArray = attemptsArray,
            NextAttemptAts = nextAttemptAts,
            LastErrors = lastErrors
        };

        var command = new CommandDefinition(
            EventQueries.BatchInsertWithResults,
            parameters,
            commandTimeout: 60, // Longer timeout for batch operations
            cancellationToken: cancellationToken);

        try
        {
            var results = await connection.QueryAsync<BatchInsertResultRow>(command);
            var resultDict = results.ToDictionary(r => r.Id, r => r.Outcome);

            // Map results back to input order
            var details = new List<EventInsertDetail>(chunk.Length);
            foreach (var envelope in chunk)
            {
                if (resultDict.TryGetValue(envelope.Id, out var outcomeStr))
                {
                    var outcome = outcomeStr.Equals("Success", StringComparison.OrdinalIgnoreCase)
                        ? EventInsertOutcome.Success
                        : EventInsertOutcome.Conflict;

                    details.Add(new EventInsertDetail(envelope.Id, outcome));
                }
                else
                {
                    // Should not happen with proper query, but handle defensively
                    details.Add(new EventInsertDetail(envelope.Id, EventInsertOutcome.Error, "Result not returned from database"));
                }
            }

            return details;
        }
        catch (Exception ex)
        {
            if (TryMapException(ex, out var mapped))
                throw mapped;

            throw;
        }
    }

    /// <summary>
    /// Updates the status of an event by its ID.
    /// </summary>
    /// <param name="eventId">The event ID.</param>
    /// <param name="newStatus">The new event status.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task UpdateStatusAsync(Guid eventId, EventStatus newStatus, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        using var connection = _connectionFactory.CreateConnection();
        connection.Open();

        var parameters = new
        {
            Status = newStatus.ToString(),
            EventId = eventId
        };

        var command = new CommandDefinition(
            EventQueries.UpdateStatus,
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
    /// Increments the attempt counter for an event by its ID.
    /// </summary>
    /// <param name="eventId">The event ID.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task IncrementAttemptsAsync(Guid eventId, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        using var connection = _connectionFactory.CreateConnection();
        connection.Open();

        var parameters = new { EventId = eventId };

        var command = new CommandDefinition(
            EventQueries.IncrementAttempts,
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
    /// Retrieves an event envelope by its ID.
    /// </summary>
    /// <param name="eventId">The event ID.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The event envelope, or null if not found.</returns>
    public async Task<EventEnvelope?> GetByIdAsync(Guid eventId, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        using var connection = _connectionFactory.CreateConnection();
        connection.Open();

        var parameters = new { EventId = eventId };

        var command = new CommandDefinition(
            EventQueries.GetById,
            parameters,
            commandTimeout: 30,
            cancellationToken: cancellationToken);

        try
        {
            var result = await connection.QuerySingleOrDefaultAsync<EventEnvelopeDto>(command);

            return result == null ? null : result.ToEventEnvelope();
        }
        catch (Exception ex)
        {
            if (TryMapException(ex, out var mapped))
                throw mapped;

            throw;
        }
    }

    public async Task<EventEnvelope?> GetByTenantAndIdempotencyKeyAsync(
        string tenantId,
        string idempotencyKey,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (string.IsNullOrWhiteSpace(tenantId))
            throw new ArgumentException("TenantId cannot be null, empty, or whitespace.", nameof(tenantId));

        if (string.IsNullOrWhiteSpace(idempotencyKey))
            throw new ArgumentException("IdempotencyKey cannot be null, empty, or whitespace.", nameof(idempotencyKey));

        using var connection = _connectionFactory.CreateConnection();
        connection.Open();

        var parameters = new
        {
            TenantId = tenantId,
            IdempotencyKey = idempotencyKey
        };

        var command = new CommandDefinition(
            EventQueries.GetByTenantAndIdempotencyKey,
            parameters,
            commandTimeout: 30,
            cancellationToken: cancellationToken);

        try
        {
            var result = await connection.QuerySingleOrDefaultAsync<EventEnvelopeDto>(command);
            return result == null ? null : result.ToEventEnvelope();
        }
        catch (Exception ex)
        {
            if (TryMapException(ex, out var mapped))
                throw mapped;

            throw;
        }
    }

    /// <summary>
    /// Retrieves retryable events with pagination metadata.
    /// </summary>
    /// <param name="now">The current time reference.</param>
    /// <param name="pageSize">The maximum number of events to return.</param>
    /// <param name="skip">The number of events to skip.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A paginated retryable event result with metadata.</returns>
    public async Task<RetryableEventsPage> GetRetryableEventsAsync(
        DateTimeOffset now,
        int pageSize = 1000,
        int skip = 0,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (pageSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(pageSize), "Page size must be greater than zero.");

        if (skip < 0)
            throw new ArgumentOutOfRangeException(nameof(skip), "Skip cannot be negative.");

        using var connection = _connectionFactory.CreateConnection();
        connection.Open();

        var parameters = new
        {
            Status = EventStatus.FAILED_RETRYABLE.ToString(),
            Now = now,
            Take = pageSize + 1,
            Skip = skip
        };

        var command = new CommandDefinition(
            EventQueries.GetRetryableEventsPage,
            parameters,
            commandTimeout: 30,
            cancellationToken: cancellationToken);

        try
        {
            var results = await connection.QueryAsync<EventEnvelopeDto>(command);
            var mappedResults = results.Select(dto => dto.ToEventEnvelope()).ToList();
            var hasMore = mappedResults.Count > pageSize;

            if (hasMore)
                mappedResults.RemoveAt(mappedResults.Count - 1);

            return new RetryableEventsPage(mappedResults, hasMore, skip, pageSize);
        }
        catch (Exception ex)
        {
            if (TryMapException(ex, out var mapped))
                throw mapped;

            throw;
        }
    }

    /// <summary>
    /// Counts events by status, optionally filtered by tenant.
    /// </summary>
    /// <param name="status">The event status to count.</param>
    /// <param name="tenantId">Optional tenant filter.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The number of events matching the criteria.</returns>
    public async Task<long> GetCountAsync(
        EventStatus status,
        string? tenantId = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (tenantId is not null && string.IsNullOrWhiteSpace(tenantId))
            throw new ArgumentException("TenantId cannot be blank when provided.", nameof(tenantId));

        using var connection = _connectionFactory.CreateConnection();
        connection.Open();

        var parameters = new
        {
            Status = status.ToString(),
            TenantId = tenantId
        };

        var command = new CommandDefinition(
            EventQueries.GetCountByStatus,
            parameters,
            commandTimeout: 30,
            cancellationToken: cancellationToken);

        try
        {
            return await connection.ExecuteScalarAsync<long>(command);
        }
        catch (Exception ex)
        {
            if (TryMapException(ex, out var mapped))
                throw mapped;

            throw;
        }
    }

    /// <summary>
    /// Retrieves all events with the specified correlation ID.
    /// </summary>
    /// <param name="correlationId">The correlation ID.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The events associated with the correlation ID.</returns>
    public async Task<IReadOnlyList<EventEnvelope>> GetByCorrelationIdAsync(
        Guid correlationId,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (correlationId == Guid.Empty)
            throw new ArgumentException("CorrelationId cannot be empty.", nameof(correlationId));

        using var connection = _connectionFactory.CreateConnection();
        connection.Open();

        var parameters = new { CorrelationId = correlationId };

        var command = new CommandDefinition(
            EventQueries.GetByCorrelationId,
            parameters,
            commandTimeout: 30,
            cancellationToken: cancellationToken);

        try
        {
            var results = await connection.QueryAsync<EventEnvelopeDto>(command);
            return results.Select(dto => dto.ToEventEnvelope()).ToList();
        }
        catch (Exception ex)
        {
            if (TryMapException(ex, out var mapped))
                throw mapped;

            throw;
        }
    }

    /// <summary>
    /// Retrieves events for a tenant with pagination.
    /// </summary>
    /// <param name="tenantId">The tenant ID.</param>
    /// <param name="pageSize">The maximum number of events to return.</param>
    /// <param name="skip">The number of events to skip.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The events for the tenant.</returns>
    public async Task<IReadOnlyList<EventEnvelope>> GetByTenantIdAsync(
        string tenantId,
        int pageSize = 100,
        int skip = 0,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (string.IsNullOrWhiteSpace(tenantId))
            throw new ArgumentException("TenantId is required.", nameof(tenantId));

        if (pageSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(pageSize), "Page size must be greater than zero.");

        if (skip < 0)
            throw new ArgumentOutOfRangeException(nameof(skip), "Skip cannot be negative.");

        using var connection = _connectionFactory.CreateConnection();
        connection.Open();

        var parameters = new
        {
            TenantId = tenantId,
            Take = pageSize,
            Skip = skip
        };

        var command = new CommandDefinition(
            EventQueries.GetByTenantIdPage,
            parameters,
            commandTimeout: 30,
            cancellationToken: cancellationToken);

        try
        {
            var results = await connection.QueryAsync<EventEnvelopeDto>(command);
            return results.Select(dto => dto.ToEventEnvelope()).ToList();
        }
        catch (Exception ex)
        {
            if (TryMapException(ex, out var mapped))
                throw mapped;

            throw;
        }
    }

    /// <summary>
    /// Retrieves the single oldest retryable event eligible for processing.
    /// </summary>
    /// <param name="now">The current time reference.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The oldest retryable event, or null if none exist.</returns>
    public async Task<EventEnvelope?> GetOldestRetryableAsync(
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        using var connection = _connectionFactory.CreateConnection();
        connection.Open();

        var parameters = new
        {
            Status = EventStatus.FAILED_RETRYABLE.ToString(),
            Now = now
        };

        var command = new CommandDefinition(
            EventQueries.GetOldestRetryable,
            parameters,
            commandTimeout: 30,
            cancellationToken: cancellationToken);

        try
        {
            var result = await connection.QuerySingleOrDefaultAsync<EventEnvelopeDto>(command);
            return result == null ? null : result.ToEventEnvelope();
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
        if (TryMapSqlState(exception, out mapped))
            return true;

        if (exception.InnerException != null && TryMapSqlState(exception.InnerException, out mapped))
            return true;

        if (IsTimeout(exception) || IsTimeout(exception.InnerException))
        {
            mapped = new EventRepositoryTransientException("Database operation timed out.", exception);
            return true;
        }

        mapped = null!;
        return false;
    }

    private static bool TryMapSqlState(Exception exception, out Exception mapped)
    {
        if (TryGetSqlState(exception, out var sqlState, out var constraintName))
        {
            if (sqlState == PostgresErrorCodes.UniqueViolation)
            {
                mapped = new EventRepositoryConflictException(
                    "Idempotency constraint violation.",
                    constraintName,
                    exception);
                return true;
            }

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

    private static bool TryGetSqlState(Exception exception, out string? sqlState, out string? constraintName)
    {
        if (exception is PostgresException postgresException)
        {
            sqlState = postgresException.SqlState;
            constraintName = postgresException.ConstraintName;
            return true;
        }

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
    /// Data transfer object for mapping batch insert results.
    /// </summary>
    private sealed class BatchInsertResultRow
    {
        public Guid Id { get; set; }
        public string Outcome { get; set; } = null!;
    }

    /// <summary>
    /// Data transfer object for mapping database rows to EventEnvelope entities.
    /// </summary>
    private sealed class EventEnvelopeDto
    {
        public Guid Id { get; set; }
        public string EventType { get; set; } = null!;
        public DateTimeOffset OccurredAt { get; set; }
        public DateTimeOffset ReceivedAt { get; set; }
        public string Payload { get; set; } = null!;
        public string? IdempotencyKey { get; set; }
        public string TenantId { get; set; } = null!;
        public Guid CorrelationId { get; set; }
        public string Status { get; set; } = null!;
        public int Attempts { get; set; }
        public DateTimeOffset? NextAttemptAt { get; set; }
        public string? LastError { get; set; }

        /// <summary>
        /// Converts this DTO to an EventEnvelope domain entity.
        /// </summary>
        /// <returns>An EventEnvelope instance.</returns>
        public EventEnvelope ToEventEnvelope()
        {
            var payloadDocument = JsonDocument.Parse(Payload);
            var status = Enum.Parse<EventStatus>(Status);

            return EventEnvelope.RehydrateFromPersistence(
                id: Id,
                eventType: EventType,
                occurredAt: OccurredAt,
                receivedAt: ReceivedAt,
                source: "PERSISTED", // Note: 'source' is not persisted; we use a marker value
                tenantId: TenantId,
                idempotencyKey: IdempotencyKey,
                correlationId: CorrelationId,
                payload: payloadDocument,
                status: status,
                attempts: Attempts,
                nextAttemptAt: NextAttemptAt,
                lastError: LastError);
        }
    }
}
