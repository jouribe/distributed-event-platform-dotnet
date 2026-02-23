namespace EventPlatform.Infrastructure.Persistence.Exceptions;

/// <summary>
/// Base exception for repository persistence failures.
/// </summary>
public abstract class EventRepositoryException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="EventRepositoryException"/> class.
    /// </summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The original exception.</param>
    /// <param name="isTransient">Whether the error is transient.</param>
    protected EventRepositoryException(string message, Exception innerException, bool isTransient)
        : base(message, innerException)
    {
        IsTransient = isTransient;
    }

    /// <summary>
    /// Gets a value indicating whether the error is transient.
    /// </summary>
    public bool IsTransient { get; }
}

/// <summary>
/// Exception raised when an idempotency conflict occurs.
/// </summary>
public sealed class EventRepositoryConflictException : EventRepositoryException
{
    /// <summary>
    /// Initializes a new instance of the <see cref="EventRepositoryConflictException"/> class.
    /// </summary>
    /// <param name="message">The exception message.</param>
    /// <param name="constraintName">The violated constraint name, if available.</param>
    /// <param name="innerException">The original exception.</param>
    public EventRepositoryConflictException(string message, string? constraintName, Exception innerException)
        : base(message, innerException, isTransient: false)
    {
        ConstraintName = constraintName;
    }

    /// <summary>
    /// Gets the violated constraint name when available.
    /// </summary>
    public string? ConstraintName { get; }
}

/// <summary>
/// Exception raised for transient persistence errors.
/// </summary>
public sealed class EventRepositoryTransientException : EventRepositoryException
{
    /// <summary>
    /// Initializes a new instance of the <see cref="EventRepositoryTransientException"/> class.
    /// </summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The original exception.</param>
    public EventRepositoryTransientException(string message, Exception innerException)
        : base(message, innerException, isTransient: true)
    {
    }
}
