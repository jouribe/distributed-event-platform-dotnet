using System.Net;
using System.Net.Http.Json;
using EventPlatform.IntegrationTests.Contracts;
using EventPlatform.IntegrationTests.Fixtures;

namespace EventPlatform.IntegrationTests.Api;

public class EventIngestionApiTests : IClassFixture<CustomWebApplicationFactory>
{
    private readonly CustomWebApplicationFactory _factory;

    public EventIngestionApiTests(CustomWebApplicationFactory factory)
    {
        _factory = factory;
    }

    [Fact]
    public async Task PostEvents_Returns202_WhenEventIsNew()
    {
        _factory.ResetState();

        var client = _factory.CreateClient();

        var request = new
        {
            event_type = "user.created",
            source = "integration-tests",
            tenant_id = "tenant-a",
            payload = new { user_id = "u-1" }
        };

        var message = new HttpRequestMessage(HttpMethod.Post, "/events")
        {
            Content = JsonContent.Create(request)
        };
        message.Headers.Add("Idempotency-Key", "idem-new-1");

        var response = await client.SendAsync(message);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
    }

    [Fact]
    public async Task PostEvents_Returns200_WhenRequestIsDuplicate()
    {
        _factory.ResetState();

        var client = _factory.CreateClient();

        var request = new
        {
            event_type = "user.created",
            source = "integration-tests",
            tenant_id = "tenant-a",
            payload = new { user_id = "u-1" }
        };

        var first = new HttpRequestMessage(HttpMethod.Post, "/events")
        {
            Content = JsonContent.Create(request)
        };
        first.Headers.Add("Idempotency-Key", "idem-dup-1");

        var firstResponse = await client.SendAsync(first);
        Assert.Equal(HttpStatusCode.Accepted, firstResponse.StatusCode);

        var second = new HttpRequestMessage(HttpMethod.Post, "/events")
        {
            Content = JsonContent.Create(request)
        };
        second.Headers.Add("Idempotency-Key", "idem-dup-1");

        var secondResponse = await client.SendAsync(second);

        Assert.Equal(HttpStatusCode.OK, secondResponse.StatusCode);

        var body = await secondResponse.Content.ReadFromJsonAsync<IngestResponseModel>();
        Assert.NotNull(body);
        Assert.True(body.IdempotencyReplayed);
        Assert.Equal(1, _factory.Publisher.PublishedCount);
    }
}
