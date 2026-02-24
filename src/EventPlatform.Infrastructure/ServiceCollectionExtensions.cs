using Microsoft.Extensions.DependencyInjection;
using EventPlatform.Application.Abstractions;
using EventPlatform.Infrastructure.Messaging;
using EventPlatform.Infrastructure.Persistence.DataAccess;
using EventPlatform.Infrastructure.Persistence.Repositories;
using StackExchange.Redis;

namespace EventPlatform.Infrastructure;

/// <summary>
/// Extension methods for registering infrastructure services in the dependency injection container.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers infrastructure persistence services to the dependency injection container.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The PostgreSQL connection string.</param>
    /// <returns>The updated service collection.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="services"/> or <paramref name="connectionString"/> is null.</exception>
    public static IServiceCollection AddInfrastructurePersistence(
        this IServiceCollection services,
        string connectionString)
    {
        if (services == null)
            throw new ArgumentNullException(nameof(services));

        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentNullException(nameof(connectionString), "Connection string cannot be null or empty");

        // Register the connection factory as a singleton
        services.AddSingleton<IDbConnectionFactory>(
            _ => new DbConnectionFactory(connectionString));

        // Register the event repository as scoped
        services.AddScoped<IEventRepository, EventRepository>();

        return services;
    }

    public static IServiceCollection AddInfrastructureRedisPublisher(
        this IServiceCollection services,
        string redisConnectionString,
        string streamName)
    {
        if (services == null)
            throw new ArgumentNullException(nameof(services));

        if (string.IsNullOrWhiteSpace(redisConnectionString))
            throw new ArgumentNullException(nameof(redisConnectionString), "Redis connection string cannot be null or empty");

        if (string.IsNullOrWhiteSpace(streamName))
            throw new ArgumentNullException(nameof(streamName), "Redis stream name cannot be null or empty");

        services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(redisConnectionString));
        services.AddSingleton(new RedisPublisherOptions { StreamName = streamName });
        services.AddSingleton<IEventPublisher, RedisEventPublisher>();

        return services;
    }
}
