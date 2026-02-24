import logging

from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from tasks import get_book_location_events

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# Load environment variables from .env file
load_dotenv()

app = FastAPI()


class BookRequest(BaseModel):
    blob_key: str
    book_id: int
    callback_url: str
    language: str = "en"


@app.post("/book")
async def create_book(request: BookRequest, background_tasks: BackgroundTasks):
    """
    Receives blob_key, book_id, and callback_url, then triggers async task to get book location events.
    """
    background_tasks.add_task(
        get_book_location_events,
        request.blob_key,
        request.book_id,
        request.callback_url,
        request.language
    )
    return {"status": "OK"}
