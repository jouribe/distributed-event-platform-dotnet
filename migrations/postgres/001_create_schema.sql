CREATE SCHEMA IF NOT EXISTS event_platform;

-- Needed if you use gen_random_uuid() anywhere
CREATE EXTENSION IF NOT EXISTS pgcrypto;
