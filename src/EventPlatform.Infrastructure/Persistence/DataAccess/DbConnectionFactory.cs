using System.Data;
using Npgsql;

namespace EventPlatform.Infrastructure.Persistence.DataAccess;

/// <summary>
/// Implementation of <see cref="IDbConnectionFactory"/> using Npgsql for PostgreSQL connections.
/// </summary>
public sealed class DbConnectionFactory : IDbConnectionFactory
{
    private readonly string _connectionString;

    /// <summary>
    /// Initializes a new instance of the <see cref="DbConnectionFactory"/> class.
    /// </summary>
    /// <param name="connectionString">The PostgreSQL connection string.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="connectionString"/> is null or empty.</exception>
    public DbConnectionFactory(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentNullException(nameof(connectionString), "Connection string cannot be null or empty");

        _connectionString = connectionString;
    }

    /// <summary>
    /// Creates a new PostgreSQL database connection.
    /// </summary>
    /// <returns>A new <see cref="NpgsqlConnection"/> instance.</returns>
    public IDbConnection CreateConnection() => new NpgsqlConnection(_connectionString);
}
