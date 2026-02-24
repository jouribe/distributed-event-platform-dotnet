using EventWorker;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

var builder = Host.CreateApplicationBuilder(args);

builder.Services
	.AddOptions<RedisConsumerOptions>()
	.Bind(builder.Configuration.GetSection(RedisConsumerOptions.SectionName));

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

builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
