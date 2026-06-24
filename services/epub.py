import io
import logging
from html.parser import HTMLParser

import ebooklib
from ebooklib import epub as epub_lib

logger = logging.getLogger(__name__)


class _HTMLTextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.text_parts = []

    def handle_data(self, data):
        stripped = data.strip()
        if stripped:
            self.text_parts.append(stripped)


def _extract_text_from_html(html_bytes: bytes) -> str:
    parser = _HTMLTextExtractor()
    parser.feed(html_bytes.decode("utf-8", errors="replace"))
    return " ".join(parser.text_parts)


def extract_pages_from_epub(file_content: bytes) -> list[str]:
    """
    Extracts text content from an EPUB file.
    Each spine item (typically a chapter/section) becomes a "page".
    Navigation documents are excluded.

    Args:
        file_content: The EPUB file content as bytes

    Returns:
        A list of strings, each representing the text of one spine item
    """
    epub_stream = io.BytesIO(file_content)

    try:
        book = epub_lib.read_epub(epub_stream)
    except Exception as e:
        logger.error(f"Failed to read EPUB file: {e}")
        return []

    pages = []
    for item in book.get_items_of_type(ebooklib.ITEM_DOCUMENT):
        nav_classes = (epub_lib.EpubNav, epub_lib.EpubNcx)
        if isinstance(item, nav_classes):
            continue
        text = _extract_text_from_html(item.get_content())
        if text.strip():
            pages.append(text)

    logger.info(f"Extracted {len(pages)} sections from EPUB file")

    return pages
