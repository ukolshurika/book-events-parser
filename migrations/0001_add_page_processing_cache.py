from yoyo import step

__depends__ = {}

steps = [
    step(
        """
        CREATE TABLE page_processing_cache (
            blob_key    TEXT NOT NULL,
            page_number INTEGER NOT NULL,
            book_id     INTEGER NOT NULL,
            page_text   TEXT,
            events      JSONB,
            status      TEXT NOT NULL DEFAULT 'text_ready',
            created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (blob_key, page_number)
        );
        CREATE INDEX idx_page_cache_created_at
            ON page_processing_cache (created_at);
        """,
        "DROP TABLE IF EXISTS page_processing_cache;",
    )
]
