using Microsoft.Extensions.Diagnostics.HealthChecks;
using StackExchange.Redis;

namespace EventIngestion.Api.Health;

internal sealed class RedisReadinessHealthCheck : IHealthCheck
{
    private readonly IConnectionMultiplexer _connectionMultiplexer;

    public RedisReadinessHealthCheck(IConnectionMultiplexer connectionMultiplexer)
    {
        _connectionMultiplexer = connectionMultiplexer ?? throw new ArgumentNullException(nameof(connectionMultiplexer));
    }

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (!_connectionMultiplexer.IsConnected)
            {
                return HealthCheckResult.Unhealthy("Redis is not connected.");
            }

            var database = _connectionMultiplexer.GetDatabase();
            _ = await database.PingAsync();

            return HealthCheckResult.Healthy("Redis is reachable.");
        }
        catch (Exception exception)
        {
            return HealthCheckResult.Unhealthy("Redis readiness check failed.", exception);
        }
    }
}
