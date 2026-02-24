using System.Net;
using System.Net.Http.Json;
using EventPlatform.IntegrationTests.Contracts;
using EventPlatform.IntegrationTests.Fixtures;
using Microsoft.AspNetCore.Mvc.Testing;

namespace EventPlatform.IntegrationTests.Api;

public class EventIngestionApiTests : IClassFixture<CustomWebApplicationFactory>
{
    private readonly CustomWebApplicationFactory _factory;

    public EventIngestionApiTests(CustomWebApplicationFactory factory)
    {
        _factory = factory;
    }

    [Fact]
    public async Task PostEvents_PersistsRowAndPublishesStreamEntry_WhenEventIsNew()
    {
        await _factory.ResetStateAsync();

        var client = _factory.CreateClient(new WebApplicationFactoryClientOptions
        {
            AllowAutoRedirect = false,
            BaseAddress = new Uri("https://localhost")
        });

        const string tenantId = "tenant-a";
        const string idempotencyKey = "idem-new-1";
        var request = new
        {
            event_type = "user.created",
            source = "integration-tests",
            tenant_id = tenantId,
            payload = new { user_id = "u-1" }
        };

        var message = new HttpRequestMessage(HttpMethod.Post, "/events")
        {
            Content = JsonContent.Create(request)
        };
        message.Headers.Add("Idempotency-Key", idempotencyKey);

        var response = await client.SendAsync(message);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);

        await _factory.WaitForStreamLengthAsync(expectedLength: 1, timeout: TimeSpan.FromSeconds(10));

        var eventRows = await _factory.CountEventsByTenantAndIdempotencyKeyAsync(tenantId, idempotencyKey);
        Assert.Equal(1, eventRows);

        var streamLength = await _factory.GetStreamLengthAsync();
        Assert.Equal(1, streamLength);
    }

    [Fact]
    public async Task PostEvents_DoesNotCreateDuplicateRowOrStreamEntry_WhenRequestIsDuplicate()
    {
        await _factory.ResetStateAsync();

        var client = _factory.CreateClient(new WebApplicationFactoryClientOptions
        {
            AllowAutoRedirect = false,
            BaseAddress = new Uri("https://localhost")
        });

        const string tenantId = "tenant-a";
        const string idempotencyKey = "idem-dup-1";
        var request = new
        {
            event_type = "user.created",
            source = "integration-tests",
            tenant_id = tenantId,
            payload = new { user_id = "u-1" }
        };

        var first = new HttpRequestMessage(HttpMethod.Post, "/events")
        {
            Content = JsonContent.Create(request)
        };
        first.Headers.Add("Idempotency-Key", idempotencyKey);

        var firstResponse = await client.SendAsync(first);
        Assert.Equal(HttpStatusCode.Accepted, firstResponse.StatusCode);

        await _factory.WaitForStreamLengthAsync(expectedLength: 1, timeout: TimeSpan.FromSeconds(10));

        var second = new HttpRequestMessage(HttpMethod.Post, "/events")
        {
            Content = JsonContent.Create(request)
        };
        second.Headers.Add("Idempotency-Key", idempotencyKey);

        var secondResponse = await client.SendAsync(second);

        Assert.Equal(HttpStatusCode.OK, secondResponse.StatusCode);

        var body = await secondResponse.Content.ReadFromJsonAsync<IngestResponseModel>();
        Assert.NotNull(body);
        Assert.True(body.IdempotencyReplayed);

        await _factory.WaitForStreamLengthAsync(expectedLength: 1, timeout: TimeSpan.FromSeconds(5));

        var eventRows = await _factory.CountEventsByTenantAndIdempotencyKeyAsync(tenantId, idempotencyKey);
        Assert.Equal(1, eventRows);

        var streamLength = await _factory.GetStreamLengthAsync();
        Assert.Equal(1, streamLength);
    }

    [Fact]
    public async Task PostEvents_UsesHeaderIdempotencyKey_WhenHeaderAndBodyProvideDifferentValues()
    {
        await _factory.ResetStateAsync();

        var client = _factory.CreateClient(new WebApplicationFactoryClientOptions
        {
            AllowAutoRedirect = false,
            BaseAddress = new Uri("https://localhost")
        });

        const string tenantId = "tenant-a";
        const string headerIdempotencyKey = "idem-header-precedence-1";
        const string bodyIdempotencyKey = "idem-body-precedence-1";
        var request = new
        {
            event_type = "user.created",
            source = "integration-tests",
            tenant_id = tenantId,
            idempotency_key = bodyIdempotencyKey,
            payload = new { user_id = "u-2" }
        };

        var message = new HttpRequestMessage(HttpMethod.Post, "/events")
        {
            Content = JsonContent.Create(request)
        };
        message.Headers.Add("Idempotency-Key", headerIdempotencyKey);

        var response = await client.SendAsync(message);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);

        await _factory.WaitForStreamLengthAsync(expectedLength: 1, timeout: TimeSpan.FromSeconds(10));

        var headerRows = await _factory.CountEventsByTenantAndIdempotencyKeyAsync(tenantId, headerIdempotencyKey);
        var bodyRows = await _factory.CountEventsByTenantAndIdempotencyKeyAsync(tenantId, bodyIdempotencyKey);

        Assert.Equal(1, headerRows);
        Assert.Equal(0, bodyRows);
    }

    [Fact]
    public async Task PostEvents_UsesHeaderCorrelationId_WhenHeaderAndBodyProvideDifferentValues()
    {
        await _factory.ResetStateAsync();

        var client = _factory.CreateClient(new WebApplicationFactoryClientOptions
        {
            AllowAutoRedirect = false,
            BaseAddress = new Uri("https://localhost")
        });

        const string tenantId = "tenant-a";
        const string idempotencyKey = "idem-correlation-precedence-1";
        var bodyCorrelationId = Guid.Parse("11111111-1111-1111-1111-111111111111");
        var headerCorrelationId = Guid.Parse("22222222-2222-2222-2222-222222222222");

        var request = new
        {
            event_type = "user.created",
            source = "integration-tests",
            tenant_id = tenantId,
            correlation_id = bodyCorrelationId,
            payload = new { user_id = "u-3" }
        };

        var message = new HttpRequestMessage(HttpMethod.Post, "/events")
        {
            Content = JsonContent.Create(request)
        };
        message.Headers.Add("Idempotency-Key", idempotencyKey);
        message.Headers.Add("X-Correlation-Id", headerCorrelationId.ToString());

        var response = await client.SendAsync(message);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);

        await _factory.WaitForStreamLengthAsync(expectedLength: 1, timeout: TimeSpan.FromSeconds(10));

        var storedCorrelationId = await _factory.GetCorrelationIdByTenantAndIdempotencyKeyAsync(tenantId, idempotencyKey);
        Assert.Equal(headerCorrelationId, storedCorrelationId);
    }

    [Fact]
    public async Task PostEvents_UsesBodyCorrelationId_WhenCorrelationHeaderIsInvalid()
    {
        await _factory.ResetStateAsync();

        var client = _factory.CreateClient(new WebApplicationFactoryClientOptions
        {
            AllowAutoRedirect = false,
            BaseAddress = new Uri("https://localhost")
        });

        const string tenantId = "tenant-a";
        const string idempotencyKey = "idem-correlation-invalid-header-1";
        var bodyCorrelationId = Guid.Parse("33333333-3333-3333-3333-333333333333");

        var request = new
        {
            event_type = "user.created",
            source = "integration-tests",
            tenant_id = tenantId,
            correlation_id = bodyCorrelationId,
            payload = new { user_id = "u-4" }
        };

        var message = new HttpRequestMessage(HttpMethod.Post, "/events")
        {
            Content = JsonContent.Create(request)
        };
        message.Headers.Add("Idempotency-Key", idempotencyKey);
        message.Headers.Add("X-Correlation-Id", "not-a-guid");

        var response = await client.SendAsync(message);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);

        await _factory.WaitForStreamLengthAsync(expectedLength: 1, timeout: TimeSpan.FromSeconds(10));

        var storedCorrelationId = await _factory.GetCorrelationIdByTenantAndIdempotencyKeyAsync(tenantId, idempotencyKey);
        Assert.Equal(bodyCorrelationId, storedCorrelationId);
    }

    [Fact]
    public async Task PostEvents_GeneratesCorrelationId_WhenCorrelationHeaderIsInvalidAndBodyIsMissing()
    {
        await _factory.ResetStateAsync();

        var client = _factory.CreateClient(new WebApplicationFactoryClientOptions
        {
            AllowAutoRedirect = false,
            BaseAddress = new Uri("https://localhost")
        });

        const string tenantId = "tenant-a";
        const string idempotencyKey = "idem-correlation-invalid-header-2";

        var request = new
        {
            event_type = "user.created",
            source = "integration-tests",
            tenant_id = tenantId,
            payload = new { user_id = "u-5" }
        };

        var message = new HttpRequestMessage(HttpMethod.Post, "/events")
        {
            Content = JsonContent.Create(request)
        };
        message.Headers.Add("Idempotency-Key", idempotencyKey);
        message.Headers.Add("X-Correlation-Id", "still-not-a-guid");

        var response = await client.SendAsync(message);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);

        await _factory.WaitForStreamLengthAsync(expectedLength: 1, timeout: TimeSpan.FromSeconds(10));

        var storedCorrelationId = await _factory.GetCorrelationIdByTenantAndIdempotencyKeyAsync(tenantId, idempotencyKey);
        Assert.NotEqual(Guid.Empty, storedCorrelationId);
    }
}
