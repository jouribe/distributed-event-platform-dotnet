using EventWorker;
using EventWorker.Handlers;
using EventPlatform.Application.Abstractions;
using EventPlatform.Infrastructure;
using EventPlatform.Infrastructure.Messaging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

var builder = Host.CreateApplicationBuilder(args);

var dbConnectionString =
	Environment.GetEnvironmentVariable("EVENTPLATFORM_DB")
	?? builder.Configuration.GetConnectionString("EventPlatformDb")
	?? throw new InvalidOperationException("EVENTPLATFORM_DB or ConnectionStrings:EventPlatformDb must be configured.");

builder.Services
	.AddOptions<RedisConsumerOptions>()
	.Bind(builder.Configuration.GetSection(RedisConsumerOptions.SectionName));

builder.Services.AddInfrastructurePersistence(dbConnectionString);

builder.Services.AddSingleton<IConnectionMultiplexer>(serviceProvider =>
{
	var options = serviceProvider.GetRequiredService<IOptions<RedisConsumerOptions>>().Value;

	if (string.IsNullOrWhiteSpace(options.ConnectionString))
	{
		throw new InvalidOperationException("RedisConsumer:ConnectionString must be configured.");
	}

	var configurationOptions = ConfigurationOptions.Parse(options.ConnectionString);
	configurationOptions.AbortOnConnectFail = false;
	configurationOptions.ConnectRetry = 5;

	return ConnectionMultiplexer.Connect(configurationOptions);
});

builder.Services.AddSingleton<IRedisConsumerGroupBootstrapper, RedisConsumerGroupBootstrapper>();

builder.Services.AddSingleton<NoopWorkerEventHandler>();
builder.Services.AddSingleton<UserCreatedEventHandler>();
builder.Services.AddSingleton<IWorkerEventHandler>(sp =>
    new EventHandlerRouter(
        sp.GetRequiredService<ILogger<EventHandlerRouter>>(),
        sp.GetRequiredService<NoopWorkerEventHandler>(),
        new Dictionary<string, IWorkerEventHandler>(StringComparer.OrdinalIgnoreCase)
        {
            ["user.created"] = sp.GetRequiredService<UserCreatedEventHandler>()
        }));

builder.Services.AddHostedService<Worker>();

builder.Services
    .AddOptions<RetryOptions>()
    .Bind(builder.Configuration.GetSection(RetryOptions.SectionName));

// IEventPublisher — reuse the IConnectionMultiplexer already registered above.
var retryStreamName = builder.Configuration
    .GetSection(RedisConsumerOptions.SectionName)
    .GetValue<string>("StreamName") ?? "events:ingress";
builder.Services.AddSingleton(new RedisPublisherOptions { StreamName = retryStreamName });
builder.Services.AddSingleton<IEventPublisher, RedisEventPublisher>();

builder.Services.AddHostedService<RetrySchedulerService>();

var host = builder.Build();
host.Run();
