using EventIngestion.Api.Ingestion;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Hosting;

namespace EventPlatform.UnitTests.Api;

public class IngestionOptionsStartupValidatorTests
{
    [Fact]
    public void Validate_ReturnsSuccess_WhenConfigurationIsValidInProduction()
    {
        var validator = CreateValidator(Environments.Production);
        var options = new IngestionOptions
        {
            RedisStreamName = "events:ingress",
            AllowedEventTypes = ["user.created"]
        };

        var result = validator.Validate(name: null, options);

        Assert.True(result.Succeeded);
    }

    [Fact]
    public void Validate_ReturnsFailure_WhenRedisStreamNameIsMissing()
    {
        var validator = CreateValidator(Environments.Development);
        var options = new IngestionOptions
        {
            RedisStreamName = "   ",
            AllowedEventTypes = ["user.created"]
        };

        var result = validator.Validate(name: null, options);

        Assert.True(result.Failed);
        Assert.Contains(result.Failures, failure =>
            failure.Contains("Ingestion:RedisStreamName", StringComparison.Ordinal));
    }

    [Fact]
    public void Validate_ReturnsFailure_WhenAllowedEventTypesIsEmptyInProduction()
    {
        var validator = CreateValidator(Environments.Production);
        var options = new IngestionOptions
        {
            RedisStreamName = "events:ingress",
            AllowedEventTypes = Array.Empty<string>()
        };

        var result = validator.Validate(name: null, options);

        Assert.True(result.Failed);
        Assert.Contains(result.Failures, failure =>
            failure.Contains("Ingestion:AllowedEventTypes", StringComparison.Ordinal));
    }

    [Fact]
    public void Validate_ReturnsSuccess_WhenAllowedEventTypesIsEmptyInDevelopment()
    {
        var validator = CreateValidator(Environments.Development);
        var options = new IngestionOptions
        {
            RedisStreamName = "events:ingress",
            AllowedEventTypes = Array.Empty<string>()
        };

        var result = validator.Validate(name: null, options);

        Assert.True(result.Succeeded);
    }

    private static IngestionOptionsStartupValidator CreateValidator(string environmentName)
        => new(new TestHostEnvironment { EnvironmentName = environmentName });

    private sealed class TestHostEnvironment : IHostEnvironment
    {
        public string EnvironmentName { get; set; } = Environments.Production;
        public string ApplicationName { get; set; } = "EventIngestion.Api";
        public string ContentRootPath { get; set; } = AppContext.BaseDirectory;
        public IFileProvider ContentRootFileProvider { get; set; } = new NullFileProvider();
    }
}
