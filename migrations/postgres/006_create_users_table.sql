CREATE TABLE event_platform.users (
    id               uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    external_user_id text        NOT NULL,
    tenant_id        text        NOT NULL,
    email            text        NOT NULL,
    name             text        NOT NULL,
    created_at       timestamptz NOT NULL DEFAULT now(),
    source_event_id  uuid        NULL,
    CONSTRAINT uq_users_tenant_external_user_id UNIQUE (tenant_id, external_user_id)
);

CREATE INDEX ix_users_tenant_id ON event_platform.users (tenant_id);
