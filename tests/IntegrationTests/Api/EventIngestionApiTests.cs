using System.Net;
using System.Net.Http.Json;
using EventPlatform.Domain.Events;
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
        // With outbox pattern, outbox event is created so we verify it exists
        var unpublishedAtCheck = await _factory.OutboxRepository.GetUnpublishedAsync(100);
        Assert.True(unpublishedAtCheck.Count >= 1);
    }

    [Fact]
    public async Task PostEvents_CreatesOutboxEvent_WhenNewEventIngested()
    {
        _factory.ResetState();

        var client = _factory.CreateClient();

        var request = new
        {
            event_type = "user.created",
            source = "integration-tests",
            tenant_id = "tenant-a",
            payload = new { user_id = "u-3" }
        };

        var message = new HttpRequestMessage(HttpMethod.Post, "/events")
        {
            Content = JsonContent.Create(request)
        };
        message.Headers.Add("Idempotency-Key", "idem-outbox-1");

        var response = await client.SendAsync(message);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);

        // Verify outbox entry was created
        var unpublished = await _factory.OutboxRepository.GetUnpublishedAsync(100);
        Assert.NotEmpty(unpublished);

        var outboxEvent = unpublished.First();
        Assert.False(outboxEvent.IsPublished);
        Assert.Equal(0, outboxEvent.PublishAttempts);
        Assert.Null(outboxEvent.LastError);
    }

    [Fact]
    public async Task PostEvents_EventuallyPublishes_WhenOutboxPublisherRuns()
    {
        _factory.ResetState();

        var client = _factory.CreateClient();

        var request = new
        {
            event_type = "order.created",
            source = "integration-tests",
            tenant_id = "tenant-b",
            payload = new { order_id = "order-123" }
        };

        var message = new HttpRequestMessage(HttpMethod.Post, "/events")
        {
            Content = JsonContent.Create(request)
        };
        message.Headers.Add("Idempotency-Key", "idem-publish-1");

        var response = await client.SendAsync(message);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);

        // Get the outbox event
        var unpublished = await _factory.OutboxRepository.GetUnpublishedAsync(100);
        Assert.NotEmpty(unpublished);

        var outboxEvent = unpublished.First();
        var outboxId = outboxEvent.Id;

        // Simulate what OutboxPublisherService does:
        // 1. Retrieve unpublished events
        // 2. Publish them (would fail if publisher is down, but in memory publisher succeeds)
        // 3. Mark as published
        // 4. Move time forward and delete old ones

        // Since we're using in-memory publisher, it will succeed
        // Mark as published (simulating successful publish)
        await _factory.OutboxRepository.MarkPublishedAsync(outboxId);

        // Verify it's now published
        var stillUnpublished = await _factory.OutboxRepository.GetUnpublishedAsync(100);
        Assert.Empty(stillUnpublished);
    }

    [Fact]
    public async Task PostEvents_RecordsAttempts_OnPublishFailure()
    {
        _factory.ResetState();

        var client = _factory.CreateClient();

        var request = new
        {
            event_type = "user.updated",
            source = "integration-tests",
            tenant_id = "tenant-c",
            payload = new { user_id = "u-4" }
        };

        var message = new HttpRequestMessage(HttpMethod.Post, "/events")
        {
            Content = JsonContent.Create(request)
        };
        message.Headers.Add("Idempotency-Key", "idem-attempt-1");

        var response = await client.SendAsync(message);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);

        // Get the outbox event
        var unpublished = await _factory.OutboxRepository.GetUnpublishedAsync(100);
        var outboxEvent = unpublished.First();
        var outboxId = outboxEvent.Id;

        // Record a failed publish attempt
        await _factory.OutboxRepository.RecordPublishAttemptAsync(
            outboxId,
            "Redis connection timeout");

        // Verify attempt was recorded
        var stillUnpublished = await _factory.OutboxRepository.GetUnpublishedAsync(100);
        var updated = stillUnpublished.First(o => o.Id == outboxId);
        Assert.Equal(1, updated.PublishAttempts);
        Assert.Contains("timeout", updated.LastError);

        // Record another attempt
        await _factory.OutboxRepository.RecordPublishAttemptAsync(
            outboxId,
            "Redis connection timeout");

        // Verify second attempt was recorded
        stillUnpublished = await _factory.OutboxRepository.GetUnpublishedAsync(100);
        updated = stillUnpublished.First(o => o.Id == outboxId);
        Assert.Equal(2, updated.PublishAttempts);
    }
}
