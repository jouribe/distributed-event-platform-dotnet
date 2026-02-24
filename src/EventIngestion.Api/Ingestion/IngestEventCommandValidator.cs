using FluentValidation;
using Microsoft.Extensions.Options;

namespace EventIngestion.Api.Ingestion;

public sealed class IngestEventCommandValidator : AbstractValidator<IngestEventCommand>
{
    public IngestEventCommandValidator(IOptions<IngestionOptions> options)
    {
        var allowedEventTypes = (options.Value.AllowedEventTypes ?? Array.Empty<string>())
            .Where(static value => !string.IsNullOrWhiteSpace(value))
            .ToHashSet(StringComparer.Ordinal);

        RuleFor(x => x.EventType)
            .NotEmpty()
            .Must(value => allowedEventTypes.Contains(value))
            .WithMessage("event_type is not registered.");

        RuleFor(x => x.TenantId)
            .NotEmpty();

        RuleFor(x => x.IdempotencyKey)
            .NotEmpty();

        RuleFor(x => x.Source)
            .NotEmpty();

        RuleFor(x => x.EventId)
            .NotEqual(Guid.Empty);

        RuleFor(x => x.CorrelationId)
            .NotEqual(Guid.Empty);

        RuleFor(x => x.Payload)
            .Must(payload => payload.ValueKind is not System.Text.Json.JsonValueKind.Undefined and not System.Text.Json.JsonValueKind.Null)
            .WithMessage("payload is required.");
    }
}
