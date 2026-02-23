using DbUp;
using DbUp.Engine;
using System.Text.RegularExpressions;

var connectionString = Environment.GetEnvironmentVariable("EVENTPLATFORM_DB") ?? throw new InvalidOperationException("EVENTPLATFORM_DB environment variable is not set.");

Console.WriteLine("Running database migration...");
Console.WriteLine($"DB: {Redact(connectionString)}");

var scriptsPath = Path.Combine(AppContext.BaseDirectory);
var migrationScriptRegex = new Regex(@"^\d+_.+\.sql$", RegexOptions.IgnoreCase | RegexOptions.Compiled);

UpgradeEngine upgrader =
    DeployChanges.To
        .PostgresqlDatabase(connectionString)
        .JournalToPostgresqlTable("event_platform", "schema_versions")
        .WithScriptsFromFileSystem(
            scriptsPath,
            f => migrationScriptRegex.IsMatch(Path.GetFileName(f)))
        .LogToConsole()
        .Build();

var result = upgrader.PerformUpgrade();

if (!result.Successful)
{
    Console.Error.WriteLine(result.Error);
    Environment.ExitCode = -1;
    return;
}

Console.WriteLine("✅ Database migrations applied successfully.");

static string Redact(string cs)
{
    // Covers: Password=...; Pwd=...
    var parts = cs.Split(';', StringSplitOptions.RemoveEmptyEntries);
    for (var i = 0; i < parts.Length; i++)
    {
        var kv = parts[i].Split('=', 2);
        if (kv.Length != 2) continue;

        var key = kv[0].Trim();
        if (key.Equals("Password", StringComparison.OrdinalIgnoreCase) ||
            key.Equals("Pwd", StringComparison.OrdinalIgnoreCase))
        {
            parts[i] = $"{key}=***";
        }
    }
    return string.Join(';', parts);
}
