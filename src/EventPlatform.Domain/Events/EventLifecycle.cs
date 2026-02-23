namespace EventPlatform.Domain.Events;

/// <summary>
/// Defines the lifecycle of events in the system, including valid state transitions. This class provides methods to validate and enforce the rules governing how events can transition between different statuses, ensuring that the event processing pipeline operates correctly and consistently.
/// The lifecycle includes the following states:
/// - RECEIVED: The event has been received but not yet processed.
/// - QUEUED: The event is queued for processing.
/// - PROCESSING: The event is currently being processed.
/// - SUCCEEDED: The event has been processed successfully.
/// - FAILED_RETRYABLE: The event processing failed but can be retried.
/// - FAILED_TERMINAL: The event processing failed and cannot be retried.
/// Any other transitions are considered invalid and should be prevented to maintain the integrity of the event lifecycle. This class can be used
/// to validate state changes before they occur, ensuring that the event lifecycle is maintained correctly throughout the processing pipeline.
/// Example usage:
/// if (EventLifecycle.CanTransition(currentStatus, newStatus))
/// {
///     // Proceed with the state change
/// }
/// else
/// {
///    // Handle invalid transition, e.g., log an error or throw an exception
/// }
/// </summary>
public static class EventLifecycle
{
    /// <summary>
    /// Defines valid state transitions for events. This ensures that events follow a consistent lifecycle and prevents invalid state changes.
    /// For example, an event can only transition from RECEIVED to QUEUED, from QUEUED to PROCESSING, and from PROCESSING to either SUCCEEDED, FAILED_RETRYABLE, or FAILED_TERMINAL. Additionally, a FAILED_RETRYABLE event can transition back to QUEUED for retrying. Any other transitions are considered invalid.
    /// This method can be used to validate state changes before they occur, ensuring that the event lifecycle is maintained correctly throughout the processing pipeline.
    /// Usage example:
    /// if (EventLifecycle.CanTransition(currentStatus, newStatus))
    /// {
    ///     // Proceed with the state change
    /// }
    /// else
    /// {
    ///     // Handle invalid transition, e.g., log an error or throw an exception
    /// }
    /// </summary>
    /// <param name="from">The current state of the event.</param>
    /// <param name="to">The desired state to transition to.</param>
    /// <returns>True if the transition is valid; otherwise, false.</returns>
    public static bool CanTransition(EventStatus from, EventStatus to)
    {
        return (from, to) switch
        {
            (EventStatus.RECEIVED, EventStatus.QUEUED) => true,

            (EventStatus.QUEUED, EventStatus.PROCESSING) => true,

            (EventStatus.PROCESSING, EventStatus.SUCCEEDED) => true,
            (EventStatus.PROCESSING, EventStatus.FAILED_RETRYABLE) => true,
            (EventStatus.PROCESSING, EventStatus.FAILED_TERMINAL) => true,

            (EventStatus.FAILED_RETRYABLE, EventStatus.QUEUED) => true, // retry re-enqueue

            _ => false
        };
    }

    /// <summary>
    /// Ensures that a state transition is valid. If the transition is not valid, an InvalidOperationException is thrown. This method can be used to enforce the event lifecycle rules and prevent invalid state changes in the application.
    /// Usage example:
    /// try
    /// {
    ///     EventLifecycle.EnsureTransition(currentStatus, newStatus);
    ///     // Proceed with the state change
    /// }
    /// catch (InvalidOperationException ex)
    /// {
    ///    // Handle the exception, e.g., log the error or notify the user
    /// }
    /// </summary>
    /// <param name="from">The current state of the event.</param>
    /// <param name="to">The desired state to transition to.</param>
    /// <exception cref="InvalidOperationException">Thrown when the transition is invalid.</exception>
    public static void EnsureTransition(EventStatus from, EventStatus to)
    {
        if (!CanTransition(from, to))
            throw new InvalidOperationException($"Invalid transition: {from} -> {to}");
    }
}
