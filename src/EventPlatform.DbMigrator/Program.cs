using DbUp;
using DbUp.Engine;

var connectionString = Environment.GetEnvironmentVariable("EVENTPLATFORM_DB") ?? throw new InvalidOperationException("DB_CONNECTION_STRING environment variable is not set.");

Console.WriteLine("Runing database migration...");
Console.WriteLine($"DB: {Redact(connectionString)}");

var scriptsPath = Path.Combine(AppContext.BaseDirectory);

UpgradeEngine upgrader =
    DeployChanges.To
        .PostgresqlDatabase(connectionString)
        .WithScriptsFromFileSystem(
            scriptsPath,
            f => f.EndsWith(".sql", StringComparison.OrdinalIgnoreCase))
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
