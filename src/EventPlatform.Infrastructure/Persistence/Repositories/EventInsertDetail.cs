namespace EventPlatform.Infrastructure.Persistence.Repositories;

/// <summary>
/// Details about the result of inserting a single event in a batch operation.
/// </summary>
/// <param name="EventId">The unique identifier of the event.</param>
/// <param name="Outcome">The outcome of the insert attempt.</param>
/// <param name="ErrorMessage">Optional error message if the outcome is Error.</param>
public sealed record EventInsertDetail(
    Guid EventId,
    EventInsertOutcome Outcome,
    string? ErrorMessage = null);
