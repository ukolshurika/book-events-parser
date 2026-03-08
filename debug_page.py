#!/usr/bin/env python3
"""
Debug script to inspect PDF page extraction and OCR results.

Usage (inside container):
    python debug_page.py <blob_key> <page_number>

Example:
    python debug_page.py tmo877ztc2abzm91uuzi6jtg2nqo 4

The script will:
  - Download the PDF from S3
  - Extract text from the given page via pypdf
  - Convert the page to an image via pdf2image
  - Save the image to /tmp/debug_page_<N>.png
  - Run tesseract on the image and show raw output + confidence stats
"""

import sys
import io
import logging

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)

    blob_key = sys.argv[1]
    page_number = int(sys.argv[2])  # 1-indexed
    language = sys.argv[3] if len(sys.argv) > 3 else "ru"

    from services.s3 import download_book_from_s3
    from services.pdf import get_tesseract_lang
    from pypdf import PdfReader
    from pdf2image import convert_from_bytes
    import pytesseract
    from PIL import Image

    # ── 1. Download ──────────────────────────────────────────────────────────
    logger.info(f"Downloading blob_key={blob_key} ...")
    file_content = download_book_from_s3(blob_key)
    logger.info(f"Downloaded {len(file_content):,} bytes")

    # ── 2. pypdf text extraction ─────────────────────────────────────────────
    reader = PdfReader(io.BytesIO(file_content))
    total_pages = len(reader.pages)
    logger.info(f"PDF has {total_pages} pages")

    if page_number < 1 or page_number > total_pages:
        logger.error(f"Page {page_number} out of range (1..{total_pages})")
        sys.exit(1)

    page = reader.pages[page_number - 1]
    pypdf_text = page.extract_text() or ""
    print("\n" + "=" * 60)
    print(f"  pypdf text for page {page_number} ({len(pypdf_text)} chars):")
    print("=" * 60)
    print(repr(pypdf_text[:500]) if pypdf_text.strip() else "  <EMPTY>")

    # ── 3. Convert page to image ─────────────────────────────────────────────
    logger.info(f"Converting page {page_number} to image ...")
    images = convert_from_bytes(
        file_content,
        first_page=page_number,
        last_page=page_number,
        dpi=300,          # higher DPI = better OCR quality
    )
    if not images:
        logger.error("convert_from_bytes returned no images!")
        sys.exit(1)

    img = images[0]
    out_path = f"/tmp/debug_page_{page_number}.png"
    img.save(out_path)
    logger.info(f"Page image saved to {out_path}  (size: {img.size}, mode: {img.mode})")

    # ── 4. Tesseract OCR ─────────────────────────────────────────────────────
    tesseract_lang = get_tesseract_lang(language)
    logger.info(f"Running tesseract with lang='{tesseract_lang}' ...")

    # Raw text
    ocr_text = pytesseract.image_to_string(img, lang=tesseract_lang)
    print("\n" + "=" * 60)
    print(f"  Tesseract text for page {page_number} ({len(ocr_text)} chars):")
    print("=" * 60)
    print(repr(ocr_text[:500]) if ocr_text.strip() else "  <EMPTY>")

    # Per-word confidence data
    data = pytesseract.image_to_data(img, lang=tesseract_lang, output_type=pytesseract.Output.DICT)
    confs = [int(c) for c in data["conf"] if str(c).lstrip("-").isdigit() and int(c) >= 0]
    words = [w for w, c in zip(data["text"], data["conf"])
             if str(c).lstrip("-").isdigit() and int(c) >= 0 and w.strip()]

    print("\n" + "=" * 60)
    print("  Tesseract word-level stats:")
    print("=" * 60)
    if confs:
        print(f"  Words detected : {len(words)}")
        print(f"  Avg confidence : {sum(confs)/len(confs):.1f}%")
        print(f"  Min confidence : {min(confs)}%")
        print(f"  Max confidence : {max(confs)}%")
        print(f"  First 20 words : {words[:20]}")
    else:
        print("  No words detected by Tesseract at all.")

    # ── 5. Summary ───────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    print(f"  pypdf text    : {'OK (' + str(len(pypdf_text.strip())) + ' chars)' if pypdf_text.strip() else 'EMPTY'}")
    print(f"  Image saved   : {out_path}")
    print(f"  Tesseract OCR : {'OK (' + str(len(ocr_text.strip())) + ' chars)' if ocr_text.strip() else 'EMPTY'}")
    if not ocr_text.strip():
        print()
        print("  Possible reasons OCR is empty:")
        print("  - Image is blank / all white (check the saved PNG)")
        print("  - Wrong language pack (current: " + tesseract_lang + ")")
        print("  - PDF page is vector graphics, not rasterised text")
        print("  - DPI too low for tesseract to detect glyphs")
        print(f"\n  Inspect the image: cp {out_path} /app/  (then open from host)")
    print()


if __name__ == "__main__":
    main()
