# Stack Research: DLQ + Observability for .NET 10 Event Platform

## Critical Discovery

**Existing `IngestionMetrics.cs` already uses `System.Diagnostics.Metrics` (`Meter` / `Counter<T>`)** — the standard .NET instrumentation API that OpenTelemetry reads natively. Adding `prometheus-net` would create two incompatible parallel metrics APIs. **Use OpenTelemetry Prometheus exporter, not prometheus-net.**

---

## New Packages Required

| Package | Version | Project(s) | Confidence |
|---------|---------|-----------|-----------|
| `OpenTelemetry.Exporter.Prometheus.AspNetCore` | 1.9.0 | EventIngestion.Api | High |
| `OpenTelemetry.Extensions.Hosting` | 1.9.0 | EventIngestion.Api, EventWorker | High |

That's it. Everything else reuses existing infrastructure.

---

## Stack Decisions by Concern

### Prometheus Metrics

**Use:** `OpenTelemetry.Exporter.Prometheus.AspNetCore` 1.9.0
**Why:** The existing `IngestionMetrics.cs` is already instrumented with `System.Diagnostics.Metrics`. OTel reads this natively — zero migration cost. Exposes `/metrics` as Prometheus scrape endpoint via ASP.NET Core middleware.
**Do NOT use:** `prometheus-net` — would require a parallel instrumentation API and conflict with existing Meter usage.

```csharp
// Registration (EventIngestion.Api/Program.cs)
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics
        .AddMeter("EventPlatform.*")
        .AddPrometheusExporter());

app.MapPrometheusScrapingEndpoint(); // /metrics
```

### Health Checks

**Use:** `Microsoft.Extensions.Diagnostics.HealthChecks` (in-framework, no new package)
**Why:** Deep checks need custom Dapper queries (outbox lag count). xabaril `AspNetCore.HealthChecks.*` packages add no value when custom logic is required anyway.

Implement as `IHealthCheck` classes:
- `PostgresHealthCheck` — execute `SELECT 1`
- `RedisHealthCheck` — `PING` via StackExchange.Redis
- `OutboxLagHealthCheck` — `SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL`

### DLQ Persistence

**Use:** Existing `Dapper` + `Npgsql` + `IDbConnectionFactory`
**Pattern:** New `IDlqRepository` interface in `EventPlatform.Application`, `DlqRepository` in `EventPlatform.Infrastructure`. Follows the established repository pattern exactly.

New table: `dlq_events` — columns: `id`, `event_id`, `tenant_id`, `event_type`, `payload`, `failure_reason`, `attempt_count`, `dead_lettered_at`, `replayed_at` (nullable), `correlation_id`.

### DLQ Redis Routing

**Use:** Existing `StackExchange.Redis` `IDatabase.StreamAddAsync` to `events:dlq` stream
**No new packages needed.** The Worker publishes to the DLQ stream after setting `FAILED_TERMINAL`.

### Admin API Auth

**Use:** Custom 30-line `IMiddleware` + `IAdminAuthPolicy` interface (zero third-party packages)
**Why:** No package handles the "replaceable static-key-now / JWT-later" requirement cleanly. A 30-line middleware + interface is simpler, testable, and explicitly designed for the JWT upgrade path.

```csharp
public interface IAdminAuthPolicy
{
    bool IsAuthorized(HttpContext context);
}

public class StaticKeyAuthPolicy(IOptions<AdminAuthOptions> options) : IAdminAuthPolicy
{
    public bool IsAuthorized(HttpContext context) =>
        context.Request.Headers.TryGetValue("X-Admin-Key", out var key) &&
        key == options.Value.ApiKey;
}
```

JWT upgrade: swap `StaticKeyAuthPolicy` registration in DI — zero middleware changes.

**Do NOT use:** `Microsoft.AspNetCore.Authentication.JwtBearer` — deferred to later milestone.

---

## What NOT to Use

| Package | Reason |
|---------|--------|
| `prometheus-net` | Conflicts with existing `System.Diagnostics.Metrics` instrumentation |
| `MassTransit` | Explicitly out of scope — raw Redis Streams only |
| `Entity Framework Core` | Explicitly excluded — Dapper only |
| `AspNetCore.HealthChecks.*` (xabaril) | Custom logic needed anyway; adds dependency for no gain |
| `Microsoft.AspNetCore.Authentication.JwtBearer` | Deferred — modular middleware handles this upgrade path |

---

## Summary

**2 new NuGet packages. Everything else is zero new dependencies.** The existing Dapper/Redis/PostgreSQL/StackExchange.Redis infrastructure handles DLQ persistence and routing. OpenTelemetry (already the right choice given existing instrumentation) handles Prometheus export. Custom middleware handles Admin API auth modularity.
