-- Expand-first: add schema_version column with a safe default so all existing rows
-- remain valid without a data migration.  The CHECK constraint mirrors the domain
-- invariant enforced in EventEnvelope (SchemaVersion >= 1).

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'event_platform'
      AND table_name   = 'events'
      AND column_name  = 'schema_version'
  ) THEN
    ALTER TABLE event_platform.events
      ADD COLUMN schema_version SMALLINT NOT NULL DEFAULT 1;
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'chk_events_schema_version_positive'
  ) THEN
    ALTER TABLE event_platform.events
      ADD CONSTRAINT chk_events_schema_version_positive CHECK (schema_version >= 1);
  END IF;
END $$;
