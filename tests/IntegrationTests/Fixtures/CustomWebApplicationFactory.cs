using EventPlatform.Application.Abstractions;
using EventPlatform.IntegrationTests.Fakes;
using EventPlatform.Infrastructure.Messaging;
using EventPlatform.Infrastructure.Persistence.Repositories;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace EventPlatform.IntegrationTests.Fixtures;

public sealed class CustomWebApplicationFactory : WebApplicationFactory<Program>
{
    public InMemoryEventRepository Repository { get; } = new();
    public InMemoryEventPublisher Publisher { get; } = new();
    public InMemoryOutboxRepository OutboxRepository { get; } = new();

    public void ResetState()
    {
        Repository.Clear();
        Publisher.Clear();
        OutboxRepository.Clear();
    }

    protected override void ConfigureWebHost(Microsoft.AspNetCore.Hosting.IWebHostBuilder builder)
    {
        builder.ConfigureServices(services =>
        {
            services.RemoveAll<IEventRepository>();
            services.RemoveAll<IEventPublisher>();
            services.RemoveAll<IOutboxRepository>();
            services.RemoveAll<OutboxPublisherService>();

            services.AddSingleton<IEventRepository>(Repository);
            services.AddSingleton<IEventPublisher>(Publisher);
            services.AddSingleton<IOutboxRepository>(OutboxRepository);

            // Set up the cross-reference so InMemoryEventRepository can insert outbox events
            Repository.OutboxRepository = OutboxRepository;

            // Don't run the OutboxPublisherService in tests - we test it separately
        });
    }
}
