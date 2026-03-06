using System.Diagnostics;

namespace EventIngestion.Api.Correlation;

internal sealed class CorrelationIdMiddleware
{
    private readonly RequestDelegate _next;
    private readonly ILogger<CorrelationIdMiddleware> _logger;

    public CorrelationIdMiddleware(RequestDelegate next, ILogger<CorrelationIdMiddleware> logger)
    {
        _next = next ?? throw new ArgumentNullException(nameof(next));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task InvokeAsync(HttpContext httpContext)
    {
        ArgumentNullException.ThrowIfNull(httpContext);

        if (TryResolveFromHeader(httpContext.Request.Headers, out var headerCorrelationId))
        {
            HttpCorrelationContext.Set(httpContext, headerCorrelationId);
        }

        using var scope = BeginRequestScope(httpContext);

        httpContext.Response.OnStarting(() =>
        {
            var correlationId = HttpCorrelationContext.Resolve(httpContext);
            httpContext.Response.Headers[HttpCorrelationContext.HeaderName] = correlationId.ToString();
            Activity.Current?.SetTag("correlation_id", correlationId);
            return Task.CompletedTask;
        });

        await _next(httpContext);
    }

    private IDisposable? BeginRequestScope(HttpContext httpContext)
    {
        if (!HttpCorrelationContext.TryGet(httpContext, out var correlationId))
        {
            return null;
        }

        return _logger.BeginScope(new Dictionary<string, object?>
        {
            ["correlation_id"] = correlationId
        });
    }

    private static bool TryResolveFromHeader(IHeaderDictionary headers, out Guid correlationId)
    {
        correlationId = Guid.Empty;

        var rawValue = headers[HttpCorrelationContext.HeaderName].FirstOrDefault();
        return !string.IsNullOrWhiteSpace(rawValue)
            && Guid.TryParse(rawValue, out correlationId)
            && correlationId != Guid.Empty;
    }
}
