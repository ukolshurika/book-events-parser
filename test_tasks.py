import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from botocore.exceptions import ClientError
import io

from tasks import parse_page, get_book_location_events
from services.s3 import get_s3_client, download_book_from_s3
from services.pdf import extract_pages_from_pdf
from services.yandex_gpt import extract_events_from_text
from services.events import send_events_to_endpoint
from config import (
    get_bucket_name,
    get_yandex_api_key,
    get_yandex_folder_id,
)


@pytest.fixture
def db_mock():
    """Mocks all DB functions used by parse_page. Default: no cache (None)."""
    with patch("tasks.get_page_cache", new_callable=AsyncMock) as mock_get, \
         patch("tasks.save_page_text", new_callable=AsyncMock) as mock_save_text, \
         patch("tasks.save_page_events", new_callable=AsyncMock) as mock_save_events, \
         patch("tasks.mark_page_sent", new_callable=AsyncMock) as mock_mark_sent:
        mock_get.return_value = None
        yield {
            "get_page_cache": mock_get,
            "save_page_text": mock_save_text,
            "save_page_events": mock_save_events,
            "mark_page_sent": mock_mark_sent,
        }


class TestGetS3Client:
    def test_returns_boto3_client(self):
        """Test that get_s3_client returns a boto3 S3 client configured for Yandex."""
        with patch("services.s3.boto3.client") as mock_client:
            mock_client.return_value = MagicMock()
            client = get_s3_client()
            mock_client.assert_called_once_with(
                "s3",
                endpoint_url="https://storage.yandexcloud.net",
                region_name="ru-central1",
            )


class TestGetBucketName:
    def test_returns_env_bucket_name(self):
        """Test that get_bucket_name returns value from AWS_BUCKET env var."""
        with patch.dict("os.environ", {"AWS_BUCKET": "my-custom-bucket"}):
            bucket = get_bucket_name()
            assert bucket == "my-custom-bucket"

    def test_returns_default_bucket_name(self):
        """Test that get_bucket_name returns default when env var not set."""
        with patch.dict("os.environ", {}, clear=True):
            bucket = get_bucket_name()
            assert bucket == "history-prism-dev"


class TestDownloadBookFromS3:
    def test_downloads_file_successfully(self):
        """Test successful file download from S3."""
        mock_body = MagicMock()
        mock_body.read.return_value = b"fake pdf content"

        mock_s3_client = MagicMock()
        mock_s3_client.get_object.return_value = {"Body": mock_body}

        with patch("services.s3.get_s3_client", return_value=mock_s3_client):
            with patch("services.s3.get_bucket_name", return_value="test-bucket"):
                content = download_book_from_s3("books/test.pdf")

                assert content == b"fake pdf content"
                mock_s3_client.get_object.assert_called_once_with(
                    Bucket="test-bucket",
                    Key="books/test.pdf"
                )

    def test_raises_client_error_on_s3_failure(self):
        """Test that S3 ClientError is raised on failure."""
        mock_s3_client = MagicMock()
        mock_s3_client.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Not found"}},
            "GetObject"
        )

        with patch("services.s3.get_s3_client", return_value=mock_s3_client):
            with patch("services.s3.get_bucket_name", return_value="test-bucket"):
                with pytest.raises(ClientError):
                    download_book_from_s3("nonexistent/file.pdf")


class TestExtractPagesFromPdf:
    def test_extracts_pages_from_pdf(self):
        """Test that pages are extracted from PDF content."""
        mock_page1 = MagicMock()
        mock_page1.extract_text.return_value = "Page 1 content"

        mock_page2 = MagicMock()
        mock_page2.extract_text.return_value = "Page 2 content"

        mock_page3 = MagicMock()
        mock_page3.extract_text.return_value = "Page 3 content"

        mock_reader = MagicMock()
        mock_reader.pages = [mock_page1, mock_page2, mock_page3]

        with patch("services.pdf.PdfReader", return_value=mock_reader):
            pages = extract_pages_from_pdf(b"fake pdf bytes")

            assert len(pages) == 3
            assert pages[0] == "Page 1 content"
            assert pages[1] == "Page 2 content"
            assert pages[2] == "Page 3 content"

    def test_handles_empty_pages(self):
        """Test that empty pages trigger OCR."""
        mock_page = MagicMock()
        mock_page.extract_text.return_value = None

        mock_reader = MagicMock()
        mock_reader.pages = [mock_page]

        with patch("services.pdf.PdfReader", return_value=mock_reader):
            with patch("services.pdf.convert_from_bytes", return_value=[]) as mock_convert:
                pages = extract_pages_from_pdf(b"fake pdf bytes")

                assert len(pages) == 1
                assert pages[0] == ""
                mock_convert.assert_called_once()

    def test_handles_empty_pdf(self):
        """Test that PDFs with no pages return empty list."""
        mock_reader = MagicMock()
        mock_reader.pages = []

        with patch("services.pdf.PdfReader", return_value=mock_reader):
            pages = extract_pages_from_pdf(b"fake pdf bytes")

            assert pages == []


class TestParsePage:
    @pytest.mark.asyncio
    async def test_parse_page_success(self, db_mock):
        """Test successful page parsing with no prior cache."""
        with patch("tasks.extract_events_from_text", new_callable=AsyncMock) as mock_extract:
            with patch("tasks.send_events_to_endpoint", new_callable=AsyncMock):
                mock_extract.return_value = []

                result = await parse_page(
                    page_number=1,
                    page_text="This is test content for page 1",
                    blob_key="books/test.pdf",
                    book_id=123,
                    callback_url="http://example.com/books/123/events",
                    language="en"
                )

                assert result["page_number"] == 1
                assert result["status"] == "completed"
                assert result["char_count"] == 31

    @pytest.mark.asyncio
    async def test_parse_page_with_different_language(self, db_mock):
        """Test page parsing with different language."""
        with patch("tasks.extract_events_from_text", new_callable=AsyncMock) as mock_extract:
            with patch("tasks.send_events_to_endpoint", new_callable=AsyncMock):
                mock_extract.return_value = []

                result = await parse_page(
                    page_number=5,
                    page_text="Inhalt auf Deutsch",
                    blob_key="books/german.pdf",
                    book_id=456,
                    callback_url="http://example.com/books/456/events",
                    language="de"
                )

                assert result["page_number"] == 5
                assert result["status"] == "completed"
                assert result["char_count"] == 18

    @pytest.mark.asyncio
    async def test_parse_page_empty_content(self, db_mock):
        """Test parsing a page with empty content is skipped."""
        result = await parse_page(
            page_number=10,
            page_text="",
            blob_key="books/test.pdf",
            book_id=123,
            callback_url="http://example.com/books/123/events",
            language="en"
        )

        assert result["page_number"] == 10
        assert result["status"] == "skipped"
        assert result["char_count"] == 0

    @pytest.mark.asyncio
    async def test_parse_page_saves_text_and_events(self, db_mock):
        """Test that page text and events are saved to cache on first run."""
        mock_events = [{"name": "Event", "date": "2020", "geo": None}]

        with patch("tasks.extract_events_from_text", new_callable=AsyncMock) as mock_extract:
            with patch("tasks.send_events_to_endpoint", new_callable=AsyncMock):
                mock_extract.return_value = mock_events

                await parse_page(
                    page_number=1,
                    page_text="Historical content",
                    blob_key="books/test.pdf",
                    book_id=123,
                    callback_url="http://example.com/books/123/events",
                    language="en"
                )

                db_mock["save_page_text"].assert_called_once_with(
                    "books/test.pdf", 1, 123, "Historical content"
                )
                db_mock["save_page_events"].assert_called_once_with(
                    "books/test.pdf", 1, mock_events
                )
                db_mock["mark_page_sent"].assert_called_once_with("books/test.pdf", 1)

    @pytest.mark.asyncio
    async def test_parse_page_cached_sent_skips_entirely(self, db_mock):
        """Test that a page with status 'sent' is skipped entirely."""
        db_mock["get_page_cache"].return_value = {
            "status": "sent",
            "events": [{"name": "Cached Event", "date": "1900", "geo": None}],
        }

        with patch("tasks.extract_events_from_text", new_callable=AsyncMock) as mock_extract:
            result = await parse_page(
                page_number=1,
                page_text="Some text",
                blob_key="books/test.pdf",
                book_id=123,
                callback_url="http://example.com/books/123/events",
                language="en"
            )

            assert result["status"] == "cached_sent"
            assert result["events_count"] == 1
            mock_extract.assert_not_called()
            db_mock["save_page_text"].assert_not_called()

    @pytest.mark.asyncio
    async def test_parse_page_cached_events_skips_yandex_gpt(self, db_mock):
        """Test that cached events_ready status skips YandexGPT call."""
        cached_events = [{"name": "Cached Event", "date": "1900", "geo": "City"}]
        db_mock["get_page_cache"].return_value = {
            "status": "events_ready",
            "events": cached_events,
        }

        with patch("tasks.extract_events_from_text", new_callable=AsyncMock) as mock_extract:
            with patch("tasks.send_events_to_endpoint", new_callable=AsyncMock) as mock_send:
                result = await parse_page(
                    page_number=2,
                    page_text="Some historical text here",
                    blob_key="books/test.pdf",
                    book_id=123,
                    callback_url="http://example.com/books/123/events",
                    language="en"
                )

                assert result["status"] == "completed"
                mock_extract.assert_not_called()
                mock_send.assert_called_once_with(
                    cached_events, 123, "books/test.pdf", 2,
                    "http://example.com/books/123/events"
                )


class TestGetBookLocationEvents:
    @pytest.mark.asyncio
    async def test_full_flow_success(self):
        """Test the complete book processing flow."""
        mock_pdf_content = b"fake pdf content"
        mock_pages = ["Page 1 text", "Page 2 text", "Page 3 text"]

        with patch("tasks.download_book_from_s3", return_value=mock_pdf_content) as mock_download:
            with patch("tasks.extract_pages_from_pdf", return_value=mock_pages):
                with patch("tasks.parse_page", new_callable=AsyncMock) as mock_parse:
                    mock_parse.side_effect = [
                        {"page_number": 1, "status": "completed", "char_count": 11, "events_count": 0, "events": []},
                        {"page_number": 2, "status": "completed", "char_count": 11, "events_count": 0, "events": []},
                        {"page_number": 3, "status": "completed", "char_count": 11, "events_count": 0, "events": []},
                    ]

                    results = await get_book_location_events(
                        blob_key="books/test.pdf",
                        book_id=123,
                        callback_url="http://example.com/books/123/events",
                        language="en"
                    )

                    mock_download.assert_called_once_with("books/test.pdf")
                    assert mock_parse.call_count == 3
                    assert len(results) == 3
                    assert results[0]["page_number"] == 1
                    assert results[1]["page_number"] == 2
                    assert results[2]["page_number"] == 3

    @pytest.mark.asyncio
    async def test_passes_language_to_parse_page(self):
        """Test that language is passed correctly to parse_page."""
        with patch("tasks.download_book_from_s3", return_value=b"pdf"):
            with patch("tasks.extract_pages_from_pdf", return_value=["Page content"]):
                with patch("tasks.parse_page", new_callable=AsyncMock) as mock_parse:
                    mock_parse.return_value = {"page_number": 1, "status": "completed", "char_count": 12, "events_count": 0, "events": []}

                    await get_book_location_events(
                        blob_key="books/french.pdf",
                        book_id=789,
                        callback_url="http://example.com/books/789/events",
                        language="fr"
                    )

                    mock_parse.assert_called_once_with(
                        page_number=1,
                        page_text="Page content",
                        blob_key="books/french.pdf",
                        book_id=789,
                        callback_url="http://example.com/books/789/events",
                        language="fr"
                    )

    @pytest.mark.asyncio
    async def test_default_language(self):
        """Test that default language 'en' is used when not specified."""
        with patch("tasks.download_book_from_s3", return_value=b"pdf"):
            with patch("tasks.extract_pages_from_pdf", return_value=["Content"]):
                with patch("tasks.parse_page", new_callable=AsyncMock) as mock_parse:
                    mock_parse.return_value = {"page_number": 1, "status": "completed", "char_count": 7, "events_count": 0, "events": []}

                    await get_book_location_events(
                        blob_key="books/test.pdf",
                        book_id=123,
                        callback_url="http://example.com/books/123/events"
                    )

                    call_kwargs = mock_parse.call_args[1]
                    assert call_kwargs["language"] == "en"

    @pytest.mark.asyncio
    async def test_s3_client_error_propagates(self):
        """Test that S3 ClientError is propagated."""
        with patch("tasks.download_book_from_s3") as mock_download:
            mock_download.side_effect = ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "Not found"}},
                "GetObject"
            )

            with pytest.raises(ClientError):
                await get_book_location_events(
                    blob_key="nonexistent/file.pdf",
                    book_id=123,
                    callback_url="http://example.com/books/123/events",
                    language="en"
                )

    @pytest.mark.asyncio
    async def test_empty_book_returns_empty_results(self):
        """Test that a book with no pages returns empty results."""
        with patch("tasks.download_book_from_s3", return_value=b"empty pdf"):
            with patch("tasks.extract_pages_from_pdf", return_value=[]):
                results = await get_book_location_events(
                    blob_key="books/empty.pdf",
                    book_id=123,
                    callback_url="http://example.com/books/123/events",
                    language="en"
                )

                assert results == []

    @pytest.mark.asyncio
    async def test_parse_page_error_recorded_in_results(self):
        """Test that parse_page errors are recorded as 'failed' and do not abort the book."""
        with patch("tasks.download_book_from_s3", return_value=b"pdf"):
            with patch("tasks.extract_pages_from_pdf", return_value=["Page 1", "Page 2"]):
                with patch("tasks.parse_page", new_callable=AsyncMock) as mock_parse:
                    mock_parse.side_effect = [
                        Exception("Processing failed"),
                        {"page_number": 2, "status": "completed", "char_count": 6, "events_count": 0, "events": []},
                    ]

                    results = await get_book_location_events(
                        blob_key="books/test.pdf",
                        book_id=123,
                        callback_url="http://example.com/books/123/events",
                        language="en"
                    )

                    assert len(results) == 2
                    assert results[0]["status"] == "failed"
                    assert results[0]["error"] == "Processing failed"
                    assert results[1]["status"] == "completed"


class TestYandexGPTConfiguration:
    def test_get_yandex_api_key_from_env(self):
        """Test that get_yandex_api_key returns value from environment."""
        with patch.dict("os.environ", {"YANDEX_API_KEY": "test-api-key"}):
            api_key = get_yandex_api_key()
            assert api_key == "test-api-key"

    def test_get_yandex_api_key_default(self):
        """Test that get_yandex_api_key returns empty string by default."""
        with patch.dict("os.environ", {}, clear=True):
            api_key = get_yandex_api_key()
            assert api_key == ""

    def test_get_yandex_folder_id_from_env(self):
        """Test that get_yandex_folder_id returns value from environment."""
        with patch.dict("os.environ", {"YANDEX_FOLDER_ID": "test-folder-id"}):
            folder_id = get_yandex_folder_id()
            assert folder_id == "test-folder-id"

    def test_get_yandex_folder_id_default(self):
        """Test that get_yandex_folder_id returns empty string by default."""
        with patch.dict("os.environ", {}, clear=True):
            folder_id = get_yandex_folder_id()
            assert folder_id == ""


class TestExtractEventsFromText:
    @pytest.mark.asyncio
    async def test_extract_events_success(self):
        """Test successful event extraction from YandexGPT."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "result": {
                "alternatives": [
                    {
                        "message": {
                            "text": '[{"name": "Battle of Waterloo", "date": "1815-06-18", "geo": "Waterloo, Belgium"}]'
                        }
                    }
                ]
            }
        }

        with patch.dict("os.environ", {"YANDEX_API_KEY": "test-key", "YANDEX_FOLDER_ID": "test-folder"}):
            with patch("services.yandex_gpt.httpx.AsyncClient") as mock_client:
                mock_context = MagicMock()
                mock_context.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
                mock_client.return_value = mock_context

                events = await extract_events_from_text("Some historical text", "en")

                assert len(events) == 1
                assert events[0]["name"] == "Battle of Waterloo"
                assert events[0]["date"] == "1815-06-18"
                assert events[0]["geo"] == "Waterloo, Belgium"

    @pytest.mark.asyncio
    async def test_extract_events_no_credentials(self):
        """Test that empty list is returned when credentials are missing."""
        with patch.dict("os.environ", {}, clear=True):
            events = await extract_events_from_text("Some text", "en")
            assert events == []

    @pytest.mark.asyncio
    async def test_extract_events_empty_response(self):
        """Test handling of empty events array."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "result": {
                "alternatives": [
                    {
                        "message": {
                            "text": "[]"
                        }
                    }
                ]
            }
        }

        with patch.dict("os.environ", {"YANDEX_API_KEY": "test-key", "YANDEX_FOLDER_ID": "test-folder"}):
            with patch("services.yandex_gpt.httpx.AsyncClient") as mock_client:
                mock_context = MagicMock()
                mock_context.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
                mock_client.return_value = mock_context

                events = await extract_events_from_text("No events in this text", "en")

                assert events == []

    @pytest.mark.asyncio
    async def test_extract_events_with_null_geo(self):
        """Test extraction with null geo field."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "result": {
                "alternatives": [
                    {
                        "message": {
                            "text": '[{"name": "Signing of Declaration", "date": "1776-07-04", "geo": null}]'
                        }
                    }
                ]
            }
        }

        with patch.dict("os.environ", {"YANDEX_API_KEY": "test-key", "YANDEX_FOLDER_ID": "test-folder"}):
            with patch("services.yandex_gpt.httpx.AsyncClient") as mock_client:
                mock_context = MagicMock()
                mock_context.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
                mock_client.return_value = mock_context

                events = await extract_events_from_text("Historical text", "en")

                assert len(events) == 1
                assert events[0]["geo"] is None


class TestSendEventsToEndpoint:
    @pytest.mark.asyncio
    async def test_send_events_success(self):
        """Test successful sending of events to endpoint."""
        events = [{"name": "Test Event", "date": "2020-01-01", "geo": "Test Location"}]

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()

        with patch("services.events.httpx.AsyncClient") as mock_client:
            mock_context = MagicMock()
            mock_context.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
            mock_client.return_value = mock_context

            await send_events_to_endpoint(events, 123, "books/test.pdf", 1, "https://example.com/books/123/events")

            mock_context.__aenter__.return_value.post.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_events_no_callback_url(self):
        """Test that function returns early when callback_url not provided."""
        events = [{"name": "Test Event", "date": "2020-01-01", "geo": "Test Location"}]

        await send_events_to_endpoint(events, 123, "books/test.pdf", 1, "")

    @pytest.mark.asyncio
    async def test_send_events_empty_events_list(self):
        """Test that function returns early when events list is empty."""
        with patch("services.events.httpx.AsyncClient") as mock_client:
            await send_events_to_endpoint([], 123, "books/test.pdf", 1, "https://example.com/books/123/events")

            mock_client.return_value.__aenter__.return_value.post.assert_not_called()


class TestParsePageWithYandexGPT:
    @pytest.mark.asyncio
    async def test_parse_page_with_events(self, db_mock):
        """Test parse_page successfully extracts and sends events."""
        mock_events = [{"name": "Test Event", "date": "2020-01-01", "geo": "Test City"}]

        with patch("tasks.extract_events_from_text", new_callable=AsyncMock) as mock_extract:
            with patch("tasks.send_events_to_endpoint", new_callable=AsyncMock) as mock_send:
                mock_extract.return_value = mock_events

                result = await parse_page(
                    page_number=1,
                    page_text="Some historical content with events",
                    blob_key="books/test.pdf",
                    book_id=123,
                    callback_url="http://example.com/books/123/events",
                    language="en"
                )

                assert result["page_number"] == 1
                assert result["status"] == "completed"
                assert result["events_count"] == 1
                assert result["events"] == mock_events

                mock_extract.assert_called_once_with("Some historical content with events", "en")
                mock_send.assert_called_once_with(mock_events, 123, "books/test.pdf", 1, "http://example.com/books/123/events")

    @pytest.mark.asyncio
    async def test_parse_page_skips_empty_text(self, db_mock):
        """Test that parse_page skips processing for whitespace-only text."""
        result = await parse_page(
            page_number=5,
            page_text="   ",
            blob_key="books/test.pdf",
            book_id=123,
            callback_url="http://example.com/books/123/events",
            language="en"
        )

        assert result["page_number"] == 5
        assert result["status"] == "skipped"
        assert result["events_count"] == 0

    @pytest.mark.asyncio
    async def test_parse_page_no_events_found(self, db_mock):
        """Test parse_page when no events are extracted."""
        with patch("tasks.extract_events_from_text", new_callable=AsyncMock) as mock_extract:
            with patch("tasks.send_events_to_endpoint", new_callable=AsyncMock) as mock_send:
                mock_extract.return_value = []

                result = await parse_page(
                    page_number=2,
                    page_text="Some content without events",
                    blob_key="books/test.pdf",
                    book_id=123,
                    callback_url="http://example.com/books/123/events",
                    language="en"
                )

                assert result["events_count"] == 0
                assert result["events"] == []
                mock_send.assert_not_called()
