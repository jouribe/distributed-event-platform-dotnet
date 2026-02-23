-- Improves count-by-status with optional tenant filter used by operational monitoring queries.
CREATE INDEX IF NOT EXISTS ix_events_status_tenant_id
  ON event_platform.events (status, tenant_id);
