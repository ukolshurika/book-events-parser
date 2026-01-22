import logging

import httpx

from config import get_events_endpoint

logger = logging.getLogger(__name__)


async def send_events_to_endpoint(events: list[dict], user_id: str, s3_key: str, page_number: int):
    """
    Sends extracted events to the configured POST endpoint.

    Args:
        events: List of events to send
        user_id: User identifier
        s3_key: Original S3 key of the book
        page_number: Page number where events were found
    """
    endpoint = get_events_endpoint()

    if not endpoint:
        logger.warning("Events endpoint not configured, skipping POST")
        return

    if not events:
        logger.info(f"No events to send for page {page_number}")
        return

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                endpoint,
                json=events,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            logger.info(f"Successfully sent {len(events)} events to {endpoint} for page {page_number}")

    except httpx.HTTPError as e:
        logger.error(f"HTTP error sending events to endpoint: {e}")
        raise
    except Exception as e:
        logger.error(f"Error sending events to endpoint: {e}")
        raise
