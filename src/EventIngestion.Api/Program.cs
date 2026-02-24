using EventIngestion.Api.Contracts;
using EventIngestion.Api.Ingestion;
using EventPlatform.Application.Abstractions;
using EventPlatform.Domain.Events;
using EventPlatform.Infrastructure;
using EventPlatform.Infrastructure.Messaging;
using EventPlatform.Infrastructure.Persistence.Exceptions;
using EventPlatform.Infrastructure.Persistence.Repositories;
using FluentValidation;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenApi();
builder.Services.AddHttpsRedirection(options =>
{
    options.HttpsPort = builder.Configuration.GetValue<int?>("HttpsRedirection:HttpsPort") ?? 7267;
});
builder.Services.Configure<IngestionOptions>(builder.Configuration.GetSection("Ingestion"));
builder.Services.AddScoped<IValidator<IngestEventCommand>, IngestEventCommandValidator>();

var dbConnectionString =
    Environment.GetEnvironmentVariable("EVENTPLATFORM_DB")
    ?? builder.Configuration.GetConnectionString("EventPlatformDb")
    ?? throw new InvalidOperationException("EVENTPLATFORM_DB or ConnectionStrings:EventPlatformDb must be configured.");

var redisConnectionString =
    Environment.GetEnvironmentVariable("EVENTPLATFORM_REDIS")
    ?? builder.Configuration.GetConnectionString("EventPlatformRedis")
    ?? throw new InvalidOperationException("EVENTPLATFORM_REDIS or ConnectionStrings:EventPlatformRedis must be configured.");

var streamName = builder.Configuration.GetValue<string>("Ingestion:RedisStreamName") ?? "events:ingress";

builder.Services.AddInfrastructurePersistence(dbConnectionString);
builder.Services.AddInfrastructureRedisPublisher(redisConnectionString, streamName);
builder.Services.AddOutboxPublisher(); // Register the outbox publisher service

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.MapPost("/events", async (
    IngestEventRequest request,
    HttpRequest httpRequest,
    IValidator<IngestEventCommand> validator,
    IEventRepository eventRepository,
    ILogger<Program> logger,
    CancellationToken cancellationToken) =>
{
    IngestionMetrics.RequestsTotal.Add(1);

    var idempotencyHeader = httpRequest.Headers["Idempotency-Key"].FirstOrDefault();
    var correlationHeader = httpRequest.Headers["X-Correlation-Id"].FirstOrDefault();

    var effectiveIdempotencyKey = string.IsNullOrWhiteSpace(idempotencyHeader)
        ? request.IdempotencyKey?.Trim()
        : idempotencyHeader.Trim();

    Guid? correlationIdFromHeader = null;
    if (!string.IsNullOrWhiteSpace(correlationHeader)
        && Guid.TryParse(correlationHeader, out var parsedCorrelation))
    {
        correlationIdFromHeader = parsedCorrelation;
    }

    var command = new IngestEventCommand(
        EventId: request.EventId is { } requestEventId && requestEventId != Guid.Empty
            ? requestEventId
            : Guid.NewGuid(),
        EventType: request.EventType?.Trim() ?? string.Empty,
        OccurredAt: request.OccurredAt ?? DateTimeOffset.UtcNow,
        Source: request.Source?.Trim() ?? string.Empty,
        TenantId: request.TenantId?.Trim() ?? string.Empty,
        IdempotencyKey: effectiveIdempotencyKey ?? string.Empty,
        CorrelationId: correlationIdFromHeader
            ?? (request.CorrelationId is { } requestCorrelationId && requestCorrelationId != Guid.Empty
                ? requestCorrelationId
                : Guid.NewGuid()),
        Payload: request.Payload);

    var validationResult = await validator.ValidateAsync(command, cancellationToken);
    if (!validationResult.IsValid)
    {
        return Results.ValidationProblem(validationResult.ToDictionary());
    }

    var envelope = EventEnvelope.CreateNew(
        id: command.EventId,
        eventType: command.EventType,
        occurredAt: command.OccurredAt,
        source: command.Source,
        tenantId: command.TenantId,
        idempotencyKey: command.IdempotencyKey,
        correlationId: command.CorrelationId,
        payload: JsonDocument.Parse(command.Payload.GetRawText()));

    using var scope = logger.BeginScope(new Dictionary<string, object>
    {
        ["correlation_id"] = command.CorrelationId,
        ["event_id"] = command.EventId,
        ["tenant_id"] = command.TenantId,
        ["event_type"] = command.EventType
    });

    try
    {
        // Insert event + outbox in a single atomic transaction
        var outboxEvent = OutboxEvent.CreateNew(
            id: Guid.NewGuid(),
            eventId: command.EventId,
            streamName: streamName,
            payload: envelope.Payload);

        await eventRepository.InsertWithOutboxAsync(envelope, outboxEvent, cancellationToken);

        logger.LogInformation("Event ingested and queued via outbox pattern.");

        return Results.Accepted(value: new IngestEventResponse(
            EventId: envelope.Id,
            Status: EventStatus.QUEUED.ToString(),
            IdempotencyReplayed: false));
    }
    catch (EventRepositoryConflictException)
    {
        // Idempotency key conflict - retrieve the existing event
        var existing = await eventRepository.GetByTenantAndIdempotencyKeyAsync(
            command.TenantId,
            command.IdempotencyKey,
            cancellationToken);

        if (existing is null)
        {
            logger.LogWarning("Idempotency conflict detected but existing event could not be resolved.");
            return Results.Conflict();
        }

        IngestionMetrics.IdempotentReplayCount.Add(1);

        if (existing.Status == EventStatus.RECEIVED)
        {
            // Event exists but hasn't been published to the outbox yet
            // Create an outbox entry to ensure it gets published
            var outboxEvent = OutboxEvent.CreateNew(
                id: Guid.NewGuid(),
                eventId: existing.Id,
                streamName: streamName,
                payload: existing.Payload);

            try
            {
                await eventRepository.InsertWithOutboxAsync(existing, outboxEvent, cancellationToken);
            }
            catch (EventRepositoryConflictException)
            {
                // Outbox creation conflict (rare) - just continue, the event will be published eventually
                logger.LogWarning("Outbox entry already exists for idempotent replay of event {EventId}", existing.Id);
            }

            logger.LogInformation("Idempotency replay queued event for publishing.");

            return Results.Ok(new IngestEventResponse(
                EventId: existing.Id,
                Status: EventStatus.QUEUED.ToString(),
                IdempotencyReplayed: true));
        }

        logger.LogInformation("Idempotency replay detected. Returning existing event without republish.");

        return Results.Ok(new IngestEventResponse(
            EventId: existing.Id,
            Status: existing.Status.ToString(),
            IdempotencyReplayed: true));
    }
})
.WithName("IngestEvent");

app.Run();

public partial class Program;
