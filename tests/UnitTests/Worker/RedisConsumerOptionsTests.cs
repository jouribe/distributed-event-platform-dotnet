using EventWorker;

namespace EventPlatform.UnitTests.Workers;

public class RedisConsumerOptionsTests
{
    [Fact]
    public void EffectiveReadBatchSize_UsesReadCount_WhenReadBatchSizeIsUnset()
    {
        var options = new RedisConsumerOptions
        {
            ReadCount = 42
        };

        Assert.Equal(42, options.EffectiveReadBatchSize);
    }

    [Fact]
    public void EffectiveReadBatchSize_UsesReadBatchSize_WhenConfigured()
    {
        var options = new RedisConsumerOptions
        {
            ReadCount = 42,
            ReadBatchSize = 7
        };

        Assert.Equal(7, options.EffectiveReadBatchSize);
    }

    [Fact]
    public void EffectiveEmptyReadDelayMilliseconds_UsesLegacyValue_WhenEmptyReadDelayIsUnset()
    {
        var options = new RedisConsumerOptions
        {
            EmptyReadDelayMilliseconds = 1234
        };

        Assert.Equal(1234, options.EffectiveEmptyReadDelayMilliseconds);
    }

    [Fact]
    public void EffectiveEmptyReadDelayMilliseconds_UsesEmptyReadDelay_WhenConfigured()
    {
        var options = new RedisConsumerOptions
        {
            EmptyReadDelayMilliseconds = 1234,
            EmptyReadDelay = 25
        };

        Assert.Equal(25, options.EffectiveEmptyReadDelayMilliseconds);
    }
}
