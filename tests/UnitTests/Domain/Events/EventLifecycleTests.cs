using EventPlatform.Domain.Events;

namespace EventPlatform.UnitTests.Domain.Events;

public class EventLifecycleTests
{
    [Theory]
    [InlineData(EventStatus.RECEIVED, EventStatus.QUEUED)]
    [InlineData(EventStatus.QUEUED, EventStatus.PROCESSING)]
    [InlineData(EventStatus.PROCESSING, EventStatus.SUCCEEDED)]
    [InlineData(EventStatus.PROCESSING, EventStatus.FAILED_RETRYABLE)]
    [InlineData(EventStatus.PROCESSING, EventStatus.FAILED_TERMINAL)]
    [InlineData(EventStatus.FAILED_RETRYABLE, EventStatus.QUEUED)]
    public void CanTransition_ReturnsTrue_ForValidTransitions(EventStatus from, EventStatus to)
    {
        var canTransition = EventLifecycle.CanTransition(from, to);

        Assert.True(canTransition);
    }

    [Theory]
    [InlineData(EventStatus.RECEIVED, EventStatus.PROCESSING)]
    [InlineData(EventStatus.RECEIVED, EventStatus.SUCCEEDED)]
    [InlineData(EventStatus.QUEUED, EventStatus.SUCCEEDED)]
    [InlineData(EventStatus.SUCCEEDED, EventStatus.QUEUED)]
    [InlineData(EventStatus.FAILED_TERMINAL, EventStatus.QUEUED)]
    public void EnsureTransition_Throws_ForInvalidTransitions(EventStatus from, EventStatus to)
    {
        Assert.Throws<InvalidOperationException>(() => EventLifecycle.EnsureTransition(from, to));
    }
}
