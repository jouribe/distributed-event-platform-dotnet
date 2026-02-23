namespace EventPlatform.Infrastructure.Persistence.Repositories;

/// <summary>
/// Result of a batch insert operation containing aggregate counts and per-event details.
/// </summary>
/// <param name="TotalSubmitted">The total number of events submitted for insertion.</param>
/// <param name="SuccessCount">The number of events successfully inserted.</param>
/// <param name="ConflictCount">The number of events that encountered idempotency conflicts.</param>
/// <param name="ErrorCount">The number of events that encountered errors.</param>
/// <param name="Details">Detailed results for each event in the same order as submitted.</param>
public sealed record BatchInsertResult(
    int TotalSubmitted,
    int SuccessCount,
    int ConflictCount,
    int ErrorCount,
    IReadOnlyList<EventInsertDetail> Details);
