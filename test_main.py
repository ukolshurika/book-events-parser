import pytest
from unittest.mock import patch, AsyncMock
from fastapi.testclient import TestClient

from main import app


@pytest.fixture
def client():
    with patch("main.init_db", new_callable=AsyncMock), \
         patch("main.close_db", new_callable=AsyncMock), \
         patch("main.cleanup_old_records", new_callable=AsyncMock):
        with TestClient(app) as c:
            yield c


class TestBookEndpoint:
    def test_post_book_success(self, client):
        """Test successful POST /book request returns OK."""
        with patch("main.get_book_location_events"):
            response = client.post(
                "/book",
                json={
                    "blob_key": "books/test-book.pdf",
                    "book_id": 123,
                    "callback_url": "http://example.com/books/123/events",
                }
            )

            assert response.status_code == 200
            assert response.json() == {"status": "OK"}

    def test_post_book_with_language(self, client):
        """Test POST /book with explicit language parameter."""
        with patch("main.get_book_location_events"):
            response = client.post(
                "/book",
                json={
                    "blob_key": "books/german-book.pdf",
                    "book_id": 789,
                    "callback_url": "http://example.com/books/789/events",
                    "language": "de",
                }
            )

            assert response.status_code == 200
            assert response.json() == {"status": "OK"}

    def test_post_book_default_language(self, client):
        """Test POST /book uses default language 'en' when not specified."""
        with patch("main.get_book_location_events"):
            response = client.post(
                "/book",
                json={
                    "blob_key": "books/test.pdf",
                    "book_id": 123,
                    "callback_url": "http://example.com/books/123/events",
                }
            )

            assert response.status_code == 200

    def test_post_book_missing_blob_key(self, client):
        """Test POST /book with missing blob_key returns 422."""
        response = client.post(
            "/book",
            json={"book_id": 123, "callback_url": "http://example.com/events"}
        )

        assert response.status_code == 422

    def test_post_book_missing_book_id(self, client):
        """Test POST /book with missing book_id returns 422."""
        response = client.post(
            "/book",
            json={"blob_key": "books/test.pdf", "callback_url": "http://example.com/events"}
        )

        assert response.status_code == 422

    def test_post_book_missing_callback_url(self, client):
        """Test POST /book with missing callback_url returns 422."""
        response = client.post(
            "/book",
            json={"blob_key": "books/test.pdf", "book_id": 123}
        )

        assert response.status_code == 422

    def test_post_book_empty_body(self, client):
        """Test POST /book with empty body returns 422."""
        response = client.post("/book", json={})

        assert response.status_code == 422

    def test_post_book_invalid_json(self, client):
        """Test POST /book with invalid JSON returns 422."""
        response = client.post(
            "/book",
            content="not json",
            headers={"Content-Type": "application/json"}
        )

        assert response.status_code == 422
