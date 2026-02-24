using EventPlatform.Application.Abstractions;
using EventPlatform.IntegrationTests.Fakes;
using EventPlatform.Infrastructure.Persistence.Repositories;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace EventPlatform.IntegrationTests.Fixtures;

public sealed class CustomWebApplicationFactory : WebApplicationFactory<Program>
{
    public InMemoryEventRepository Repository { get; } = new();
    public InMemoryEventPublisher Publisher { get; } = new();

    public void ResetState()
    {
        Repository.Clear();
        Publisher.Clear();
    }

    protected override void ConfigureWebHost(Microsoft.AspNetCore.Hosting.IWebHostBuilder builder)
    {
        builder.ConfigureServices(services =>
        {
            services.RemoveAll<IEventRepository>();
            services.RemoveAll<IEventPublisher>();

            services.AddSingleton<IEventRepository>(Repository);
            services.AddSingleton<IEventPublisher>(Publisher);
        });
    }
}
