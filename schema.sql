CREATE TABLE IF NOT EXISTS sources (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    target_url TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    poll_interval_minutes INTEGER NOT NULL DEFAULT 30,
    next_poll_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
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

CREATE TABLE IF NOT EXISTS source_documents (
    id BIGSERIAL PRIMARY KEY,
    source_id BIGINT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    canonical_url TEXT NOT NULL,
    title TEXT NOT NULL,
    published_at TIMESTAMPTZ,
    body_text TEXT,
    fetch_status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    fetch_retry_count INTEGER NOT NULL DEFAULT 0,
    fetch_error_message TEXT,
    wiki_status VARCHAR(20) NOT NULL DEFAULT 'NOT_REQUESTED',
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_id, canonical_url)
);

CREATE TABLE IF NOT EXISTS ingestion_jobs (
    id BIGSERIAL PRIMARY KEY,
    source_document_id BIGINT NOT NULL REFERENCES source_documents(id) ON DELETE CASCADE,
    status VARCHAR(20) NOT NULL DEFAULT 'QUEUED',
    retry_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    queued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_sources_due_poll
ON sources (enabled, next_poll_at);

CREATE INDEX IF NOT EXISTS idx_source_documents_source
ON source_documents (source_id, collected_at DESC);

CREATE INDEX IF NOT EXISTS idx_source_documents_wiki_status
ON source_documents (wiki_status);

CREATE INDEX IF NOT EXISTS idx_ingestion_jobs_status
ON ingestion_jobs (status, queued_at);

CREATE INDEX IF NOT EXISTS idx_ingestion_jobs_source_document
ON ingestion_jobs (source_document_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_ingestion_jobs_source_document
ON ingestion_jobs (source_document_id);
