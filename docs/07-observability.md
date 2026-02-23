# Observability

## Correlation ID

- Accept X-Correlation-Id header.
- Generate if missing.
- Store in DB.
- Include in Redis message.
- Include in logs.

## Logs

Structured JSON logs with:

- timestamp
- level
- service
- correlation_id
- event_id
- tenant_id
- event_type
- attempt

## Metrics

Track at minimum:

- ingestion requests total
- idempotent replay count
- processing success/failure count
- retry scheduled count
- terminal failure count
- processing latency (p50/p95/p99)

## Traces

- Propagate `correlation_id` as trace attribute.
- Create spans for ingest, publish, consume, handle, and persistence.
