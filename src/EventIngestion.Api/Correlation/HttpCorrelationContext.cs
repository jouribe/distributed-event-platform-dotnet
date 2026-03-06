using System.Diagnostics;

namespace EventIngestion.Api.Correlation;

internal static class HttpCorrelationContext
{
    public const string HeaderName = "X-Correlation-Id";

    private const string ItemKey = "__correlation_id";

    public static Guid Resolve(HttpContext httpContext, Guid? bodyCorrelationId = null)
    {
        ArgumentNullException.ThrowIfNull(httpContext);

        if (TryGet(httpContext, out var correlationId))
        {
            return correlationId;
        }

        correlationId = bodyCorrelationId is { } bodyValue && bodyValue != Guid.Empty
            ? bodyValue
            : Guid.NewGuid();

        Set(httpContext, correlationId);
        return correlationId;
    }

    public static bool TryGet(HttpContext httpContext, out Guid correlationId)
    {
        ArgumentNullException.ThrowIfNull(httpContext);

        if (httpContext.Items.TryGetValue(ItemKey, out var value)
            && value is Guid parsed
            && parsed != Guid.Empty)
        {
            correlationId = parsed;
            return true;
        }

        correlationId = Guid.Empty;
        return false;
    }

    public static void Set(HttpContext httpContext, Guid correlationId)
    {
        ArgumentNullException.ThrowIfNull(httpContext);

        if (correlationId == Guid.Empty)
        {
            throw new ArgumentException("CorrelationId cannot be empty.", nameof(correlationId));
        }

        httpContext.Items[ItemKey] = correlationId;
        Activity.Current?.SetTag("correlation_id", correlationId);
    }
}
