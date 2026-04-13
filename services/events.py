import hashlib
import hmac
import json
import logging

import httpx

from config import get_callback_secret

logger = logging.getLogger(__name__)


async def send_events_to_endpoint(events: list[dict], book_id: int, blob_key: str, page_number: int, callback_url: str):
    """
    Sends extracted events to the callback URL.

    Args:
        events: List of events to send
        book_id: Book identifier
        blob_key: Original S3 key of the book
        page_number: Page number where events were found
        callback_url: URL to POST events to
    """
    if not callback_url:
        logger.warning("Callback URL not provided, skipping POST")
        return

    if not events:
        logger.info(f"No events to send for page {page_number}")
        return

    mapped_events = [
        {
            "title": event.get("name", ""),
            "description": event.get("geo") or event.get("name", ""),
            "date": event.get("date"),
        }
        for event in events
    ]

    payload = {
        "book_id": book_id,
        "blob_key": blob_key,
        "page_number": page_number,
        "events": mapped_events,
    }
    logger.info(f"Sending to {callback_url} payload: {payload}")

    body = json.dumps(payload, separators=(",", ":"))
    signature = hmac.new(
        get_callback_secret().encode(),
        body.encode(),
        hashlib.sha256
    ).hexdigest()

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                callback_url,
                content=body,
                headers={
                    "Content-Type": "application/json",
                    "X-Signature": signature,
                }
            )
            logger.info(f"Callback response status: {response.status_code}, body: {response.text[:500]}")
            response.raise_for_status()
            logger.info(f"Successfully sent {len(events)} events to {callback_url} for page {page_number}")

    except httpx.HTTPError as e:
        logger.error(f"HTTP error sending events to {callback_url}: {e}")
        raise
    except Exception as e:
        logger.error(f"Error sending events to {callback_url}: {e}")
        raise
