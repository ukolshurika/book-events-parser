import json
import logging
from datetime import datetime, timezone, timedelta

import asyncpg

from config import get_database_url, get_cache_ttl_days

logger = logging.getLogger(__name__)

_pool: asyncpg.Pool | None = None


async def _init_connection(conn):
    await conn.set_type_codec(
        "jsonb",
        encoder=lambda v: json.dumps(v, ensure_ascii=False),
        decoder=json.loads,
        schema="pg_catalog",
    )


async def init_db():
    global _pool
    _pool = await asyncpg.create_pool(get_database_url(), init=_init_connection)
    logger.info("Database pool initialized")


async def close_db():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("Database pool closed")


async def get_page_cache(blob_key: str, page_number: int) -> asyncpg.Record | None:
    async with _pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM page_processing_cache WHERE blob_key = $1 AND page_number = $2",
            blob_key,
            page_number,
        )


async def save_page_text(blob_key: str, page_number: int, book_id: int, page_text: str):
    async with _pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO page_processing_cache (blob_key, page_number, book_id, page_text, status)
            VALUES ($1, $2, $3, $4, 'text_ready')
            ON CONFLICT (blob_key, page_number) DO NOTHING
            """,
            blob_key,
            page_number,
            book_id,
            page_text,
        )


async def save_page_events(blob_key: str, page_number: int, events: list[dict]):
    async with _pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE page_processing_cache
            SET events = $3::jsonb, status = 'events_ready'
            WHERE blob_key = $1 AND page_number = $2 AND status = 'text_ready'
            """,
            blob_key,
            page_number,
            events,
        )


async def mark_page_sent(blob_key: str, page_number: int):
    async with _pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE page_processing_cache
            SET status = 'sent'
            WHERE blob_key = $1 AND page_number = $2
            """,
            blob_key,
            page_number,
        )


async def cleanup_old_records():
    days = get_cache_ttl_days()
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    async with _pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM page_processing_cache WHERE created_at < $1",
            cutoff,
        )
    logger.info(f"Cleanup: removed records older than {days} days, result: {result}")
