using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace EventWorker.Handlers;

/// <summary>
/// Routes incoming stream entries to the appropriate <see cref="IWorkerEventHandler"/>
/// based on the <c>event_type</c> field. Falls back to a no-op handler for unrecognised types.
/// </summary>
public sealed class EventHandlerRouter : IWorkerEventHandler
{
    private readonly IReadOnlyDictionary<string, IWorkerEventHandler> _handlers;
    private readonly IWorkerEventHandler _fallback;
    private readonly ILogger<EventHandlerRouter> _logger;

    /// <summary>
    /// Initializes a new instance of <see cref="EventHandlerRouter"/>.
    /// </summary>
    /// <param name="logger">Logger.</param>
    /// <param name="fallback">Handler used when no specific handler is registered for an event type.</param>
    /// <param name="handlers">Map of event-type → handler (key comparison is case-insensitive).</param>
    public EventHandlerRouter(
        ILogger<EventHandlerRouter> logger,
        IWorkerEventHandler fallback,
        IReadOnlyDictionary<string, IWorkerEventHandler> handlers)
    {
        _logger   = logger   ?? throw new ArgumentNullException(nameof(logger));
        _fallback = fallback ?? throw new ArgumentNullException(nameof(fallback));
        _handlers = handlers ?? throw new ArgumentNullException(nameof(handlers));
    }

    /// <inheritdoc/>
    public async Task HandleAsync(
        Guid eventId,
        StreamEntry entry,
        string phase,
        CancellationToken cancellationToken)
    {
        var eventType = GetStreamField(entry, "event_type");

        if (eventType is not null && _handlers.TryGetValue(eventType, out var handler))
        {
            _logger.LogDebug(
                "Routing event {EventId} (type: {EventType}) to {Handler} (phase: {Phase})",
                eventId, eventType, handler.GetType().Name, phase);

            await handler.HandleAsync(eventId, entry, phase, cancellationToken);
            return;
        }

        _logger.LogDebug(
            "No handler registered for event type '{EventType}' — using fallback (event: {EventId}, phase: {Phase})",
            eventType, eventId, phase);

        await _fallback.HandleAsync(eventId, entry, phase, cancellationToken);
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private static string? GetStreamField(StreamEntry entry, string fieldName)
    {
        foreach (var field in entry.Values)
        {
            if (!field.Name.IsNullOrEmpty
                && string.Equals(field.Name.ToString(), fieldName, StringComparison.OrdinalIgnoreCase))
            {
                return field.Value.IsNull ? null : field.Value.ToString();
            }
        }

        return null;
    }
}
