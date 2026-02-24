-- Add source column to events table for better traceability
-- Persists the original source value instead of using a synthetic marker

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'event_platform'
      AND table_name = 'events'
      AND column_name = 'source'
  ) THEN
    ALTER TABLE event_platform.events
      ADD COLUMN source text NOT NULL DEFAULT 'UNKNOWN';
  END IF;
END $$;

-- Add constraint to ensure source is not blank
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'chk_events_source_not_blank'
  ) THEN
    ALTER TABLE event_platform.events
      ADD CONSTRAINT chk_events_source_not_blank CHECK (length(btrim(source)) > 0);
  END IF;
END $$;
