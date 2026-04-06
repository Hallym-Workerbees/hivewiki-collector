CREATE TABLE IF NOT EXISTS sources (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    target_url TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,

    initial_backfill_done BOOLEAN NOT NULL DEFAULT FALSE,
    backfill_completed_at TIMESTAMPTZ,

    last_polled_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    last_error_at TIMESTAMPTZ,
    last_error_message TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS documents (
    id BIGSERIAL PRIMARY KEY,
    source_id BIGINT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    external_id TEXT NOT NULL,
    canonical_url TEXT NOT NULL,
    title TEXT NOT NULL,
    published_at TIMESTAMPTZ,

    body_text TEXT, -- 처리 완료 전까지만 저장, 성공 시 NULL 처리

    queue_status TEXT NOT NULL DEFAULT 'PENDING', -- PENDING, ENQUEUED, DONE, FAILED, DEAD
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_error_message TEXT,
    last_enqueued_at TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,

    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (source_id, external_id)
);

CREATE INDEX IF NOT EXISTS idx_sources_enabled
ON sources (enabled);

CREATE INDEX IF NOT EXISTS idx_documents_source_external
ON documents (source_id, external_id);

CREATE INDEX IF NOT EXISTS idx_documents_queue_status
ON documents (queue_status);
