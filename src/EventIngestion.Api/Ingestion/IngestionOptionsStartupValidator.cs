using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace EventIngestion.Api.Ingestion;

public sealed class IngestionOptionsStartupValidator : IValidateOptions<IngestionOptions>
{
    private readonly IHostEnvironment _hostEnvironment;

    public IngestionOptionsStartupValidator(IHostEnvironment hostEnvironment)
    {
        _hostEnvironment = hostEnvironment;
    }

    public ValidateOptionsResult Validate(string? name, IngestionOptions options)
    {
        var failures = new List<string>();

        if (string.IsNullOrWhiteSpace(options.RedisStreamName))
        {
            failures.Add("Configuration 'Ingestion:RedisStreamName' is required and cannot be empty.");
        }

        if (_hostEnvironment.IsProduction())
        {
            var allowedEventTypesCount = (options.AllowedEventTypes ?? Array.Empty<string>())
                .Count(static eventType => !string.IsNullOrWhiteSpace(eventType));

            if (allowedEventTypesCount == 0)
            {
                failures.Add("Configuration 'Ingestion:AllowedEventTypes' must contain at least one non-empty value in Production.");
            }
        }

        return failures.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(failures);
    }
}
