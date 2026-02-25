import io
import logging

from pypdf import PdfReader
import pytesseract
from pdf2image import convert_from_bytes

logger = logging.getLogger(__name__)

# Mapping from ISO 639-1 language codes to Tesseract language codes
TESSERACT_LANG_MAP = {
    "en": "eng",
    "ru": "rus",
    "de": "deu",
    "fr": "fra",
    "es": "spa",
    "it": "ita",
    "pt": "por",
    "pl": "pol",
    "uk": "ukr",
    "zh": "chi_sim",
    "ja": "jpn",
    "ko": "kor",
    "ar": "ara",
}


def get_tesseract_lang(language: str) -> str:
    """Converts ISO 639-1 or Tesseract language code to Tesseract language code."""
    if language in TESSERACT_LANG_MAP.values():
        return language
    return TESSERACT_LANG_MAP.get(language, "eng")


def extract_pages_from_pdf(file_content: bytes, language: str = "en") -> list[str]:
    """
    Extracts text content from each page of a PDF file.
    If a page contains only images (no extractable text), uses OCR to extract text.

    Args:
        file_content: The PDF file content as bytes
        language: Language code for OCR (default: "en")

    Returns:
        A list of strings, each representing the text content of a page
    """
    pdf_stream = io.BytesIO(file_content)
    reader = PdfReader(pdf_stream)
    tesseract_lang = get_tesseract_lang(language)

    pages = []
    ocr_pages_count = 0

    for page_num, page in enumerate(reader.pages):
        text = page.extract_text() or ""

        # If no text extracted, the page likely contains only images - use OCR
        if not text.strip():
            logger.info(f"Page {page_num + 1} has no text, using OCR with language '{tesseract_lang}'")
            try:
                # Convert only this specific page to image
                images = convert_from_bytes(
                    file_content,
                    first_page=page_num + 1,
                    last_page=page_num + 1
                )
                if images:
                    text = pytesseract.image_to_string(images[0], lang=tesseract_lang)
                    ocr_pages_count += 1
            except Exception as e:
                logger.error(f"OCR failed for page {page_num + 1}: {e}")
                text = ""

        pages.append(text)

    logger.info(f"Extracted {len(pages)} pages from PDF ({ocr_pages_count} pages used OCR)")

    return pages
