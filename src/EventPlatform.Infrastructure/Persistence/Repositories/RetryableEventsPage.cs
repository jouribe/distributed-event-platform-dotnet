using EventPlatform.Domain.Events;

namespace EventPlatform.Infrastructure.Persistence.Repositories;

/// <summary>
/// Represents a paginated result for retryable events.
/// </summary>
public sealed class RetryableEventsPage
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RetryableEventsPage"/> class.
    /// </summary>
    /// <param name="items">The events included in the current page.</param>
    /// <param name="hasMore">Whether there are more events available after this page.</param>
    /// <param name="skip">The number of events skipped.</param>
    /// <param name="pageSize">The requested page size.</param>
    public RetryableEventsPage(IReadOnlyList<EventEnvelope> items, bool hasMore, int skip, int pageSize)
    {
        Items = items ?? throw new ArgumentNullException(nameof(items));
        Count = items.Count;
        HasMore = hasMore;
        Skip = skip;
        PageSize = pageSize;
    }

    /// <summary>
    /// Gets the events included in the current page.
    /// </summary>
    public IReadOnlyList<EventEnvelope> Items { get; }

    /// <summary>
    /// Gets the number of events returned in the current page.
    /// </summary>
    public int Count { get; }

    /// <summary>
    /// Gets a value indicating whether more events are available.
    /// </summary>
    public bool HasMore { get; }

    /// <summary>
    /// Gets the number of events skipped.
    /// </summary>
    public int Skip { get; }

    /// <summary>
    /// Gets the requested page size.
    /// </summary>
    public int PageSize { get; }
}
