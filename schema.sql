CREATE TABLE IF NOT EXISTS sources (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    source_type TEXT NOT NULL,
    parser_type TEXT,
    target_url TEXT NOT NULL,
    category_path TEXT,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    poll_interval_minutes INTEGER NOT NULL DEFAULT 30,
    latest_fetch_limit INTEGER NOT NULL DEFAULT 20,
    rate_limit_seconds INTEGER NOT NULL DEFAULT 2,
    update_policy TEXT NOT NULL DEFAULT 'new_only',
    ocr_enabled BOOLEAN NOT NULL DEFAULT FALSE,

    next_run_at TIMESTAMPTZ,
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
    content_hash TEXT,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_dispatched_at TIMESTAMPTZ,
    process_status TEXT NOT NULL DEFAULT 'PENDING',
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_error_message TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (source_id, external_id)
);

CREATE INDEX IF NOT EXISTS idx_sources_due
ON sources (enabled, next_run_at);

CREATE INDEX IF NOT EXISTS idx_documents_source_external
ON documents (source_id, external_id);

CREATE INDEX IF NOT EXISTS idx_documents_status
ON documents (process_status);
