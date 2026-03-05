using System.Linq;
using System.Net;
using System.Net.Http.Json;
using EventPlatform.Infrastructure;
using EventPlatform.Infrastructure.Messaging;
using EventPlatform.IntegrationTests.Contracts;
using EventPlatform.IntegrationTests.Fixtures;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace EventPlatform.IntegrationTests.Api;

[Collection(nameof(ApiTestsCollection))]
public sealed class OutboxCrashRecoveryIntegrationTests : IClassFixture<NoOutboxPublisherWebApplicationFactory>
{
    private readonly NoOutboxPublisherWebApplicationFactory _factory;

    public OutboxCrashRecoveryIntegrationTests(NoOutboxPublisherWebApplicationFactory factory)
    {
        _factory = factory;
    }

    [Fact]
    public async Task EventIsPersistedAndEventuallyPublished_WhenPublisherRecoversAfterApiWrite()
    {
        await _factory.ResetStateAsync();

        var client = _factory.CreateClient(new WebApplicationFactoryClientOptions
        {
            AllowAutoRedirect = false,
            BaseAddress = new Uri("https://localhost")
        });

        const string tenantId = "tenant-crash-recovery";
        const string idempotencyKey = "idem-crash-recovery-1";
        var request = new
        {
            event_type = "user.created",
            source = "integration-tests",
            tenant_id = tenantId,
            payload = new { user_id = "u-77" }
        };

        var message = new HttpRequestMessage(HttpMethod.Post, "/events")
        {
            Content = JsonContent.Create(request)
        };
        message.Headers.Add("Idempotency-Key", idempotencyKey);

        var response = await client.SendAsync(message);
        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);

        var body = await response.Content.ReadFromJsonAsync<IngestResponseModel>();
        Assert.NotNull(body);

        await _factory.EnsureStreamLengthStaysAtAsync(0, TimeSpan.FromMilliseconds(500));

        var pendingBeforeRecovery = await _factory.CountPendingOutboxAsync();
        Assert.Equal(1, pendingBeforeRecovery);
        Assert.Equal("RECEIVED", await _factory.GetEventStatusByIdAsync(body.EventId));

        await using var provider = BuildRecoveryServiceProvider(_factory);
        var publisherService = provider
            .GetServices<IHostedService>()
            .OfType<OutboxPublisherService>()
            .Single();

        await publisherService.StartAsync(CancellationToken.None);

        await _factory.WaitForStreamLengthAsync(1, TimeSpan.FromSeconds(5));

        await publisherService.StopAsync(CancellationToken.None);

        Assert.Equal(0, await _factory.CountPendingOutboxAsync());
        Assert.True(await _factory.IsOutboxPublishedForEventAsync(body.EventId));
        Assert.Equal("QUEUED", await _factory.GetEventStatusByIdAsync(body.EventId));
    }

    private static ServiceProvider BuildRecoveryServiceProvider(NoOutboxPublisherWebApplicationFactory factory)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInfrastructurePersistence(factory.PostgresConnectionString);
        services.AddInfrastructureRedisPublisher(factory.RedisConnectionString, factory.IngestionStreamName);
        services.AddOutboxPublisher(options =>
        {
            options.PollIntervalMilliseconds = 20;
            options.MaxBatchSize = 100;
        });

        return services.BuildServiceProvider();
    }
}

public sealed class NoOutboxPublisherWebApplicationFactory : CustomWebApplicationFactory
{
    protected override bool EnableOutboxPublisher => false;
}

