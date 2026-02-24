import boto3
import logging

from config import get_bucket_name

logger = logging.getLogger(__name__)


def get_s3_client():
    """Returns a boto3 S3 client configured for Yandex Cloud Object Storage."""
    return boto3.client(
        "s3",
        endpoint_url="https://storage.yandexcloud.net",
        region_name="ru-central1",
    )


def download_book_from_s3(s3_key: str) -> bytes:
    """
    Downloads a book file from S3 bucket.

    Args:
        s3_key: The S3 object key for the book file

    Returns:
        The file content as bytes
    """
    s3_client = get_s3_client()
    bucket_name = get_bucket_name()

    logger.info(f"Downloading book from s3://{bucket_name}/{s3_key}")

    response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
    file_content = response["Body"].read()

    logger.info(f"Downloaded {len(file_content)} bytes from S3")

    return file_content
