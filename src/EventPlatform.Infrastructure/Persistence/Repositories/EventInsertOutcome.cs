namespace EventPlatform.Infrastructure.Persistence.Repositories;

/// <summary>
/// Represents the outcome of attempting to insert a single event during a batch operation.
/// </summary>
public enum EventInsertOutcome
{
    /// <summary>
    /// The event was successfully inserted into the database.
    /// </summary>
    Success,

    /// <summary>
    /// The event was not inserted due to an idempotency conflict (duplicate tenant_id + idempotency_key).
    /// </summary>
    Conflict,

    /// <summary>
    /// The event was not inserted due to an error (validation, constraint violation, etc.).
    /// </summary>
    Error
}
