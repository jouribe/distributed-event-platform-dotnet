using EventWorker;
using EventPlatform.Infrastructure;
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
builder.Services.AddSingleton<IWorkerEventHandler, NoopWorkerEventHandler>();
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
