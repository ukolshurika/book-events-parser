import os


def get_bucket_name() -> str:
    """Returns the S3 bucket name from environment variable."""
    return os.environ.get("AWS_BUCKET", "history-prism-dev")


def get_yandex_api_key() -> str:
    """Returns the YandexGPT API key from environment variable."""
    return os.environ.get("YANDEX_API_KEY", "")


def get_yandex_folder_id() -> str:
    """Returns the YandexGPT folder ID from environment variable."""
    return os.environ.get("YANDEX_FOLDER_ID", "")


def get_events_endpoint() -> str:
    """Returns the events POST endpoint URL from environment variable."""
    return os.environ.get("EVENTS_ENDPOINT", "")
