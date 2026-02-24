using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;

namespace EventPlatform.IntegrationTests.Api;

public class StartupConfigurationValidationTests
{
    [Fact]
    public void CreateClient_FailsFast_WhenRedisStreamNameIsMissing()
    {
        using var factory = new StartupValidationWebApplicationFactory(new Dictionary<string, string?>
        {
            ["ConnectionStrings:EventPlatformDb"] = "Host=localhost;Port=5432;Database=event_platform;Username=event_platform;Password=event_platform",
            ["ConnectionStrings:EventPlatformRedis"] = "localhost:6379",
            ["Ingestion:RedisStreamName"] = "   ",
            ["Ingestion:AllowedEventTypes:0"] = "user.created"
        });

        var exception = Assert.ThrowsAny<Exception>(() => factory.CreateClient());

        Assert.Contains("Ingestion:RedisStreamName", exception.ToString(), StringComparison.Ordinal);
    }

    private sealed class StartupValidationWebApplicationFactory : WebApplicationFactory<Program>
    {
        private readonly IReadOnlyDictionary<string, string?> _configurationValues;

        public StartupValidationWebApplicationFactory(IReadOnlyDictionary<string, string?> configurationValues)
        {
            _configurationValues = configurationValues;
        }

        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Production");

            builder.ConfigureAppConfiguration((_, configBuilder) =>
            {
                configBuilder.AddInMemoryCollection(_configurationValues);
            });
        }
    }
}
