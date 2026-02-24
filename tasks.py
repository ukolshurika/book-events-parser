import logging

from botocore.exceptions import ClientError

from services import (
    download_book_from_s3,
    extract_pages_from_pdf,
    extract_events_from_text,
    send_events_to_endpoint,
)

logger = logging.getLogger(__name__)


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

        # Skip processing if page is empty or too short
        if not page_text or len(page_text.strip()) < 10:
            logger.info(f"ParsePage: Skipping page {page_number} - insufficient text")
            return {
                "page_number": page_number,
                "status": "skipped",
                "char_count": len(page_text),
                "events_count": 0
            }

        # Extract events from page text using YandexGPT
        events = await extract_events_from_text(page_text, language)

        # Send events to callback endpoint
        if events:
            await send_events_to_endpoint(events, book_id, blob_key, page_number, callback_url)

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
    Downloads file from S3, divides into pages, and processes each page.

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
        pages = extract_pages_from_pdf(file_content, language)

        logger.info(f"Processing {len(pages)} pages for blob_key={blob_key}")

        # Step 3: Process each page with ParsePage worker
        results = []
        for page_number, page_text in enumerate(pages, start=1):
            result = await parse_page(
                page_number=page_number,
                page_text=page_text,
                blob_key=blob_key,
                book_id=book_id,
                callback_url=callback_url,
                language=language
            )
            results.append(result)

        logger.info(f"Completed 'Get Book Location Events' for book_id={book_id}, processed {len(results)} pages")

        return results

    except ClientError as e:
        logger.error(f"S3 error for blob_key={blob_key}: {e}")
        raise
    except Exception as e:
        logger.error(f"Error processing book location events: {e}")
        raise
