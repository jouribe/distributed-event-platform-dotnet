using System.Text.Json;
using StackExchange.Redis;

namespace EventPlatform.Infrastructure.Diagnostics;

public static class CorrelationIdMetadata
{
    public static Guid? TryReadFromOutboxPayload(JsonDocument payload)
    {
        ArgumentNullException.ThrowIfNull(payload);

        return TryReadFromJsonElement(payload.RootElement);
    }

    public static Guid? TryReadFromStreamEntry(StreamEntry entry)
    {
        if (TryReadGuidField(entry, "correlation_id", out var correlationId))
        {
            return correlationId;
        }

        if (!TryReadStringField(entry, "message", out var messageRaw)
            || string.IsNullOrWhiteSpace(messageRaw))
        {
            return null;
        }

        try
        {
            using var document = JsonDocument.Parse(messageRaw);
            return TryReadFromJsonElement(document.RootElement);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static Guid? TryReadFromJsonElement(JsonElement root)
    {
        if (root.ValueKind != JsonValueKind.Object)
        {
            return null;
        }

        if (!root.TryGetProperty("correlation_id", out var property))
        {
            return null;
        }

        if (property.ValueKind == JsonValueKind.String
            && Guid.TryParse(property.GetString(), out var parsedString)
            && parsedString != Guid.Empty)
        {
            return parsedString;
        }

        if (property.ValueKind != JsonValueKind.Null
            && Guid.TryParse(property.GetRawText().Trim('"'), out var parsedRaw)
            && parsedRaw != Guid.Empty)
        {
            return parsedRaw;
        }

        return null;
    }

    private static bool TryReadGuidField(StreamEntry entry, string fieldName, out Guid correlationId)
    {
        correlationId = Guid.Empty;
        return TryReadStringField(entry, fieldName, out var rawValue)
            && Guid.TryParse(rawValue, out correlationId)
            && correlationId != Guid.Empty;
    }

    private static bool TryReadStringField(StreamEntry entry, string fieldName, out string value)
    {
        value = string.Empty;

        foreach (var field in entry.Values)
        {
            if (!field.Name.IsNullOrEmpty
                && string.Equals(field.Name.ToString(), fieldName, StringComparison.OrdinalIgnoreCase))
            {
                if (field.Value.IsNull)
                {
                    return false;
                }

                value = field.Value.ToString();
                return true;
            }
        }

        return false;
    }
}
