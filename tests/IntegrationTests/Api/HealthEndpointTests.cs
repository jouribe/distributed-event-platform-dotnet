using System.Net;
using System.Net.Http.Json;
using EventPlatform.IntegrationTests.Fixtures;
using Microsoft.AspNetCore.Mvc.Testing;

namespace EventPlatform.IntegrationTests.Api;

[Collection(nameof(ApiTestsCollection))]
public sealed class HealthEndpointTests : IClassFixture<CustomWebApplicationFactory>
{
    private readonly CustomWebApplicationFactory _factory;

    public HealthEndpointTests(CustomWebApplicationFactory factory)
    {
        _factory = factory;
    }

    [Fact]
    public async Task GetHealth_ReturnsHealthy_WhenDbAndRedisAreReachable()
    {
        var client = _factory.CreateClient(new WebApplicationFactoryClientOptions
        {
            AllowAutoRedirect = false,
            BaseAddress = new Uri("https://localhost")
        });

        var response = await client.GetAsync("/health");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var payload = await response.Content.ReadFromJsonAsync<HealthResponse>();
        Assert.NotNull(payload);
        Assert.Equal("Healthy", payload.Status);
        Assert.Equal("Healthy", payload.Checks["postgres"]);
        Assert.Equal("Healthy", payload.Checks["redis"]);
    }

    private sealed record HealthResponse(string Status, Dictionary<string, string> Checks);
}
