namespace EventPlatform.Domain.Events;

public enum EventStatus
{
    RECEIVED = 0,
    QUEUED = 1,
    PROCESSING = 2,
    SUCCEEDED = 3,
    FAILED_RETRYABLE = 4,
    FAILED_TERMINAL = 5
}
