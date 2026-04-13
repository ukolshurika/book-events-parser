from yoyo import step

__depends__ = {"0001_add_page_processing_cache"}

steps = [
    step(
        """
        ALTER TABLE page_processing_cache
            DROP CONSTRAINT page_processing_cache_pkey,
            ADD COLUMN id BIGSERIAL PRIMARY KEY;

        ALTER TABLE page_processing_cache
            ADD CONSTRAINT uq_page_cache_blob_page UNIQUE (blob_key, page_number);

        CREATE INDEX idx_page_cache_book_id  ON page_processing_cache (book_id);
        CREATE INDEX idx_page_cache_status   ON page_processing_cache (status);
        CREATE INDEX idx_page_cache_blob_key ON page_processing_cache (blob_key);
        """,
        """
        DROP INDEX IF EXISTS idx_page_cache_book_id;
        DROP INDEX IF EXISTS idx_page_cache_status;
        DROP INDEX IF EXISTS idx_page_cache_blob_key;

        ALTER TABLE page_processing_cache
            DROP CONSTRAINT uq_page_cache_blob_page,
            DROP COLUMN id;

        ALTER TABLE page_processing_cache
            ADD PRIMARY KEY (blob_key, page_number);
        """,
    )
]
