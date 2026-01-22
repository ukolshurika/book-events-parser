# book-events-parser

FastAPI service that processes PDF books from S3, extracts historical events using YandexGPT, and posts them to a configured endpoint.

## Features

- Downloads PDF books from AWS S3
- Extracts text from PDF pages
- Uses YandexGPT API to extract historical events with dates and locations
- Posts extracted events to a configured endpoint
- Fully tested with pytest

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Copy the example environment file and fill in your credentials:

```bash
cp .env.example .env
```

Edit `.env` and add your credentials:

```bash
# AWS S3 Configuration
AWS_BUCKET=your-s3-bucket-name

# YandexGPT API Configuration
YANDEX_API_KEY=your-yandex-api-key
YANDEX_FOLDER_ID=your-yandex-folder-id

# Events Endpoint Configuration
EVENTS_ENDPOINT=https://your-website.com/books/events
```

### 3. Run the Application

```bash
uvicorn main:app --reload
```

The API will be available at `http://localhost:8000`

## API Endpoints

### POST /book

Processes a book from S3 and extracts historical events.

**Request Body:**
```json
{
  "s3_key": "books/my-book.pdf",
  "user_id": "user123",
  "language": "en"
}
```

**Response:**
```json
{
  "status": "OK"
}
```

## Event Extraction

The service extracts events in the following JSON format:

```json
[
  {
    "name": "Battle of Waterloo",
    "date": "1815-06-18",
    "geo": "Waterloo, Belgium"
  },
  {
    "name": "Declaration of Independence",
    "date": "1776-07-04",
    "geo": null
  }
]
```

- `name`: Event name
- `date`: Event date (if available)
- `geo`: Event location (null if not mentioned in text)

## Testing

Run the test suite:

```bash
pytest -v
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `AWS_BUCKET` | S3 bucket name | `history-prism-dev` |
| `YANDEX_API_KEY` | YandexGPT API key | (required) |
| `YANDEX_FOLDER_ID` | YandexGPT folder ID | (required) |
| `EVENTS_ENDPOINT` | URL to POST extracted events | (required) |
