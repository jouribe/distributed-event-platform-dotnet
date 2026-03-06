using System.Data.Common;
using EventPlatform.Infrastructure.Persistence.DataAccess;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace EventIngestion.Api.Health;

internal sealed class DatabaseReadinessHealthCheck : IHealthCheck
{
    private readonly IDbConnectionFactory _connectionFactory;

    public DatabaseReadinessHealthCheck(IDbConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
    }

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            using var connection = _connectionFactory.CreateConnection();

            if (connection is DbConnection dbConnection)
            {
                await dbConnection.OpenAsync(cancellationToken);

                await using var command = dbConnection.CreateCommand();
                command.CommandText = "SELECT 1;";
                _ = await command.ExecuteScalarAsync(cancellationToken);
            }
            else
            {
                connection.Open();
                using var command = connection.CreateCommand();
                command.CommandText = "SELECT 1;";
                _ = command.ExecuteScalar();
            }

            return HealthCheckResult.Healthy("PostgreSQL is reachable.");
        }
        catch (Exception exception)
        {
            return HealthCheckResult.Unhealthy("PostgreSQL readiness check failed.", exception);
        }
    }
}
