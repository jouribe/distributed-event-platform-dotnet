namespace EventWorker.Resilience;

public static class ExponentialBackoffRetry
{
    public static async Task ExecuteAsync(
        Func<CancellationToken, Task> operation,
        Func<Exception, bool> isTransient,
        int initialDelayMilliseconds,
        int maxDelayMilliseconds,
        double backoffFactor,
        int maxRetryAttempts,
        Func<int, TimeSpan, Exception, CancellationToken, Task>? onRetry,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(isTransient);

        if (initialDelayMilliseconds <= 0)
            throw new ArgumentOutOfRangeException(nameof(initialDelayMilliseconds), "Initial delay must be greater than zero.");

        if (maxDelayMilliseconds <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxDelayMilliseconds), "Max delay must be greater than zero.");

        if (initialDelayMilliseconds > maxDelayMilliseconds)
            throw new ArgumentOutOfRangeException(nameof(initialDelayMilliseconds), "Initial delay cannot be greater than max delay.");

        if (backoffFactor < 1.0)
            throw new ArgumentOutOfRangeException(nameof(backoffFactor), "Backoff factor must be greater than or equal to one.");

        if (maxRetryAttempts < 0)
            throw new ArgumentOutOfRangeException(nameof(maxRetryAttempts), "Max retry attempts must be greater than or equal to zero.");

        var attempt = 0;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            attempt++;

            try
            {
                await operation(cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex) when (isTransient(ex))
            {
                if (maxRetryAttempts > 0 && attempt >= maxRetryAttempts)
                {
                    throw;
                }

                var delay = CalculateDelay(attempt, initialDelayMilliseconds, maxDelayMilliseconds, backoffFactor);

                if (onRetry is not null)
                {
                    await onRetry(attempt, delay, ex, cancellationToken).ConfigureAwait(false);
                }

                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private static TimeSpan CalculateDelay(
        int attempt,
        int initialDelayMilliseconds,
        int maxDelayMilliseconds,
        double backoffFactor)
    {
        var exponent = Math.Max(0, attempt - 1);
        var delayMilliseconds = initialDelayMilliseconds * Math.Pow(backoffFactor, exponent);
        var capped = Math.Min(maxDelayMilliseconds, delayMilliseconds);

        return TimeSpan.FromMilliseconds(capped);
    }
}
