import asyncio
import logging
from functools import partial

from botocore.exceptions import ClientError

from db import get_page_cache, save_page_text, save_page_events, mark_page_sent
from services import (
    download_book_from_s3,
    extract_pages_from_pdf,
    extract_events_from_text,
    send_events_to_endpoint,
)

logger = logging.getLogger(__name__)

BATCH_SIZE = 10


async def parse_batch(batch_start: int, pages: list[tuple[int, str]], blob_key: str, book_id: int, callback_url: str, language: str):
    """
    Worker task that processes a batch of pages with a single YandexGPT call.
    Extracts events using YandexGPT and sends them to the configured endpoint,
    grouped by page_number.

    Args:
        batch_start: The first page number in this batch
        pages: List of (page_number, page_text) tuples
        blob_key: Original S3 key of the book
        book_id: Book identifier
        callback_url: URL to POST events to
        language: Language code for processing
    """
    logger.info(f"ParseBatch: Processing batch starting at page {batch_start} ({len(pages)} pages) for book_id={book_id}")

    try:
        non_empty = [(pn, text) for pn, text in pages if text and len(text.strip()) >= 10]

        if not non_empty:
            logger.info(f"ParseBatch: All pages in batch {batch_start} are empty, skipping")
            return {"batch_start": batch_start, "status": "skipped", "events_count": 0}

        cache = await get_page_cache(blob_key, batch_start)

        if cache and cache["status"] == "sent":
            logger.info(f"ParseBatch: Batch {batch_start} already sent, skipping")
            return {
                "batch_start": batch_start,
                "status": "cached_sent",
                "events_count": len(cache["events"] or []),
            }

        combined_text = "\n\n".join(
            f"--- Страница {pn} ---\n{text}" for pn, text in non_empty
        )

        if not cache:
            await save_page_text(blob_key, batch_start, book_id, combined_text)

        if cache and cache["status"] == "events_ready" and cache["events"] is not None:
            events = cache["events"]
            logger.info(f"ParseBatch: Using cached events for batch {batch_start}")
        else:
            events = await extract_events_from_text(combined_text, language)
            await save_page_events(blob_key, batch_start, events)

        # Group events by page_number and send one callback per page
        events_by_page: dict[int, list] = {}
        for event in events:
            pn = event.get("page_number", batch_start)
            events_by_page.setdefault(pn, []).append(event)

        for pn, page_events in events_by_page.items():
            await send_events_to_endpoint(page_events, book_id, blob_key, pn, callback_url)

        await mark_page_sent(blob_key, batch_start)

        logger.info(f"ParseBatch: Completed batch {batch_start}, found {len(events)} events")

        return {
            "batch_start": batch_start,
            "status": "completed",
            "events_count": len(events),
            "events": events,
        }

    except Exception as e:
        logger.error(f"ParseBatch: Error processing batch {batch_start}: {e}")
        raise


async def parse_page(page_number: int, page_text: str, blob_key: str, book_id: int, callback_url: str, language: str):
    """
    Worker task that processes a single page of the book.
    Extracts events using YandexGPT and sends them to the configured endpoint.

    Args:
        page_number: The page number (1-indexed)
        page_text: The extracted text content of the page
        blob_key: Original S3 key of the book
        book_id: Book identifier
        callback_url: URL to POST events to
        language: Language code for processing
    """
    logger.info(f"ParsePage: Processing page {page_number} for book_id={book_id}, blob_key={blob_key}, language={language}")

    try:
        logger.info(f"ParsePage: Page {page_number} has {len(page_text)} characters")

        cache = await get_page_cache(blob_key, page_number)

        # Already successfully sent — skip entirely
        if cache and cache["status"] == "sent":
            logger.info(f"ParsePage: Page {page_number} already sent, skipping")
            return {
                "page_number": page_number,
                "status": "cached_sent",
                "events_count": len(cache["events"] or []),
            }

        # Skip processing if page is empty or too short
        if not page_text or len(page_text.strip()) < 10:
            logger.info(f"ParsePage: Skipping page {page_number} - insufficient text")
            return {
                "page_number": page_number,
                "status": "skipped",
                "char_count": len(page_text),
                "events_count": 0
            }

        # Persist page text so it survives restarts
        if not cache:
            await save_page_text(blob_key, page_number, book_id, page_text)

        # Use cached events if YandexGPT already ran for this page
        if cache and cache["status"] == "events_ready" and cache["events"] is not None:
            events = cache["events"]
            logger.info(f"ParsePage: Using cached events for page {page_number}")
        else:
            events = await extract_events_from_text(page_text, language)
            await save_page_events(blob_key, page_number, events)

        # Send events to callback endpoint
        if events:
            await send_events_to_endpoint(events, book_id, blob_key, page_number, callback_url)

        await mark_page_sent(blob_key, page_number)

        logger.info(f"ParsePage: Completed processing page {page_number}, found {len(events)} events")

        return {
            "page_number": page_number,
            "status": "completed",
            "char_count": len(page_text),
            "events_count": len(events),
            "events": events
        }

    except Exception as e:
        logger.error(f"ParsePage: Error processing page {page_number}: {e}")
        raise


async def get_book_location_events(blob_key: str, book_id: int, callback_url: str, language: str = "en"):
    """
    Async task that processes book location events.
    Downloads file from S3, divides into pages, and processes them in batches.

    Args:
        blob_key: The S3 object key for the book file
        book_id: Book identifier
        callback_url: URL to POST events to
        language: Language code for processing (default: "en")
    """
    logger.info(f"Starting 'Get Book Location Events' for book_id={book_id}, blob_key={blob_key}, language={language}")

    try:
        # Step 1: Download book from S3
        file_content = download_book_from_s3(blob_key)

        # Step 2: Divide book into pages (with OCR support for image-based PDFs)
        # Run in thread pool to avoid blocking the async event loop during OCR
        loop = asyncio.get_event_loop()
        pages = await loop.run_in_executor(None, partial(extract_pages_from_pdf, file_content, language))

        logger.info(f"Processing {len(pages)} pages for blob_key={blob_key}")

        # Step 3: Process pages in batches of BATCH_SIZE
        results = []
        pages_with_numbers = list(enumerate(pages, start=1))

        for i in range(0, len(pages_with_numbers), BATCH_SIZE):
            batch = pages_with_numbers[i:i + BATCH_SIZE]
            batch_start = batch[0][0]
            try:
                result = await parse_batch(
                    batch_start=batch_start,
                    pages=batch,
                    blob_key=blob_key,
                    book_id=book_id,
                    callback_url=callback_url,
                    language=language,
                )
                results.append(result)
            except Exception as e:
                logger.error(f"Skipping batch starting at page {batch_start} after error: {e}")
                results.append({"batch_start": batch_start, "status": "failed", "error": str(e)})

        logger.info(f"Completed 'Get Book Location Events' for book_id={book_id}, processed {len(results)} batches")

        return results

    except ClientError as e:
        logger.error(f"S3 error for blob_key={blob_key}: {e}")
        raise
    except Exception as e:
        logger.error(f"Error processing book location events: {e}")
        raise
