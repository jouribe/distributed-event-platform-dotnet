using System.Text.Json;
using Dapper;
using EventPlatform.Infrastructure.Persistence.DataAccess;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace EventWorker.Handlers;

/// <summary>
/// Handles <c>user.created</c> events by inserting a user record into the users read-model.
/// Idempotent: duplicate events targeting the same <c>external_user_id</c> are silently ignored
/// via <c>ON CONFLICT DO NOTHING</c>.
/// </summary>
public sealed class UserCreatedEventHandler : IWorkerEventHandler
{
    private readonly IDbConnectionFactory _connectionFactory;
    private readonly ILogger<UserCreatedEventHandler> _logger;

    /// <summary>
    /// Initializes a new instance of <see cref="UserCreatedEventHandler"/>.
    /// </summary>
    public UserCreatedEventHandler(
        IDbConnectionFactory connectionFactory,
        ILogger<UserCreatedEventHandler> logger)
    {
        _connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc/>
    public async Task HandleAsync(
        Guid eventId,
        StreamEntry entry,
        string phase,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var (externalUserId, email, name) = ExtractUserData(entry);
        var tenantId = GetStreamField(entry, "tenant_id") ?? string.Empty;

        using var connection = _connectionFactory.CreateConnection();

        var rowsAffected = await connection.ExecuteAsync(
            """
            INSERT INTO event_platform.users (external_user_id, tenant_id, email, name, source_event_id)
            VALUES (@external_user_id, @tenant_id, @email, @name, @source_event_id)
            ON CONFLICT (external_user_id) DO NOTHING;
            """,
            new
            {
                external_user_id = externalUserId,
                tenant_id        = tenantId,
                email,
                name,
                source_event_id  = eventId == Guid.Empty ? (Guid?)null : eventId
            });

        if (rowsAffected == 0)
        {
            _logger.LogInformation(
                "User {ExternalUserId} already exists — idempotent skip (event: {EventId}, phase: {Phase})",
                externalUserId, eventId, phase);
        }
        else
        {
            _logger.LogInformation(
                "Created user {ExternalUserId} (email: {Email}, event: {EventId}, phase: {Phase})",
                externalUserId, email, eventId, phase);
        }
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private static (string UserId, string Email, string Name) ExtractUserData(StreamEntry entry)
    {
        var messageRaw = GetStreamField(entry, "message")
            ?? throw new InvalidOperationException("Stream entry is missing the 'message' field.");

        using var document = JsonDocument.Parse(messageRaw);
        var root = document.RootElement;

        if (!root.TryGetProperty("payload", out var payload))
            throw new InvalidOperationException("Message JSON is missing the 'payload' property.");

        if (!payload.TryGetProperty("user_id", out var userIdProp) || userIdProp.ValueKind != JsonValueKind.String)
            throw new InvalidOperationException("payload is missing required string field 'user_id'.");

        if (!payload.TryGetProperty("email", out var emailProp) || emailProp.ValueKind != JsonValueKind.String)
            throw new InvalidOperationException("payload is missing required string field 'email'.");

        if (!payload.TryGetProperty("name", out var nameProp) || nameProp.ValueKind != JsonValueKind.String)
            throw new InvalidOperationException("payload is missing required string field 'name'.");

        return (
            userIdProp.GetString()!,
            emailProp.GetString()!,
            nameProp.GetString()!
        );
    }

    private static string? GetStreamField(StreamEntry entry, string fieldName)
    {
        foreach (var field in entry.Values)
        {
            if (!field.Name.IsNullOrEmpty
                && string.Equals(field.Name.ToString(), fieldName, StringComparison.OrdinalIgnoreCase))
            {
                return field.Value.IsNull ? null : field.Value.ToString();
            }
        }

        return null;
    }
}
