using System.Text.Json;
using EventIngestion.Api.Ingestion;
using Microsoft.Extensions.Options;

namespace EventPlatform.UnitTests.Api;

public class IngestEventCommandValidatorTests
{
    private static IngestEventCommandValidator CreateValidator(params string[] allowedEventTypes)
    {
        var options = Options.Create(new IngestionOptions
        {
            AllowedEventTypes = allowedEventTypes
        });

        return new IngestEventCommandValidator(options);
    }

    [Fact]
    public async Task ValidateAsync_ReturnsValid_WhenCommandIsWellFormed()
    {
        var validator = CreateValidator("user.created");

        var command = new IngestEventCommand(
            EventId: Guid.NewGuid(),
            EventType: "user.created",
            OccurredAt: DateTimeOffset.UtcNow,
            Source: "tests",
            TenantId: "tenant-a",
            IdempotencyKey: "idem-001",
            CorrelationId: Guid.NewGuid(),
            Payload: JsonDocument.Parse("{\"id\":\"u-1\"}").RootElement);

        var result = await validator.ValidateAsync(command);

        Assert.True(result.IsValid);
    }

    [Fact]
    public async Task ValidateAsync_ReturnsInvalid_WhenEventTypeIsUnknown()
    {
        var validator = CreateValidator("user.created");

        var command = new IngestEventCommand(
            EventId: Guid.NewGuid(),
            EventType: "order.cancelled",
            OccurredAt: DateTimeOffset.UtcNow,
            Source: "tests",
            TenantId: "tenant-a",
            IdempotencyKey: "idem-001",
            CorrelationId: Guid.NewGuid(),
            Payload: JsonDocument.Parse("{\"id\":\"u-1\"}").RootElement);

        var result = await validator.ValidateAsync(command);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.PropertyName == nameof(IngestEventCommand.EventType));
    }

    [Fact]
    public async Task ValidateAsync_ReturnsInvalid_WhenIdempotencyKeyIsMissing()
    {
        var validator = CreateValidator("user.created");

        var command = new IngestEventCommand(
            EventId: Guid.NewGuid(),
            EventType: "user.created",
            OccurredAt: DateTimeOffset.UtcNow,
            Source: "tests",
            TenantId: "tenant-a",
            IdempotencyKey: string.Empty,
            CorrelationId: Guid.NewGuid(),
            Payload: JsonDocument.Parse("{\"id\":\"u-1\"}").RootElement);

        var result = await validator.ValidateAsync(command);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.PropertyName == nameof(IngestEventCommand.IdempotencyKey));
    }
}
