import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from fastapi.testclient import TestClient
import io

from main import app


@pytest.fixture
def client():
    return TestClient(app)


class TestBookEndpoint:
    def test_post_book_success(self, client):
        """Test successful POST /book request returns OK."""
        with patch("main.get_book_location_events") as mock_task:
            response = client.post(
                "/book",
                json={"s3_key": "books/test-book.pdf", "user_id": "user123"}
            )

            assert response.status_code == 200
            assert response.json() == {"status": "OK"}

    def test_post_book_triggers_background_task(self, client):
        """Test that POST /book triggers the background task with correct args."""
        with patch("main.get_book_location_events") as mock_task:
            response = client.post(
                "/book",
                json={"s3_key": "books/another-book.pdf", "user_id": "user456"}
            )

            assert response.status_code == 200
            # Background task is called with the provided arguments (including default language)
            mock_task.assert_called_once_with("books/another-book.pdf", "user456", "en")

    def test_post_book_with_language(self, client):
        """Test POST /book with explicit language parameter."""
        with patch("main.get_book_location_events") as mock_task:
            response = client.post(
                "/book",
                json={"s3_key": "books/german-book.pdf", "user_id": "user789", "language": "de"}
            )

            assert response.status_code == 200
            assert response.json() == {"status": "OK"}
            mock_task.assert_called_once_with("books/german-book.pdf", "user789", "de")

    def test_post_book_default_language(self, client):
        """Test POST /book uses default language 'en' when not specified."""
        with patch("main.get_book_location_events") as mock_task:
            response = client.post(
                "/book",
                json={"s3_key": "books/test.pdf", "user_id": "user123"}
            )

            assert response.status_code == 200
            # Verify default language is "en"
            call_args = mock_task.call_args[0]
            assert call_args[2] == "en"

    def test_post_book_missing_s3_key(self, client):
        """Test POST /book with missing s3_key returns 422."""
        response = client.post(
            "/book",
            json={"user_id": "user123"}
        )

        assert response.status_code == 422

    def test_post_book_missing_user_id(self, client):
        """Test POST /book with missing user_id returns 422."""
        response = client.post(
            "/book",
            json={"s3_key": "books/test.pdf"}
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
