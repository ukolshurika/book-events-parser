import logging

logger = logging.getLogger(__name__)

FORM_FEED = "\f"


def extract_pages_from_txt(file_content: bytes) -> list[str]:
    """
    Extracts text content from a plain text file.
    Splits by form feed characters (\\f) if present, otherwise
    treats the entire file as a single page.

    Args:
        file_content: The TXT file content as bytes

    Returns:
        A list of strings, each representing a "page" of text
    """
    text = file_content.decode("utf-8", errors="replace")

    if FORM_FEED in text:
        pages = text.split(FORM_FEED)
    else:
        pages = [text]

    pages = [p.strip() for p in pages if p.strip()]

    logger.info(f"Extracted {len(pages)} pages from TXT file")

    return pages
