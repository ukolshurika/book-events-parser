import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from db import init_db, close_db, cleanup_old_records
from tasks import get_book_location_events

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()


async def _periodic_cleanup():
    while True:
        await asyncio.sleep(86400)  # раз в сутки
        try:
            await cleanup_old_records()
        except Exception as e:
            logger.error(f"Periodic cleanup error: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    cleanup_task = asyncio.create_task(_periodic_cleanup())
    yield
    cleanup_task.cancel()
    await close_db()


app = FastAPI(lifespan=lifespan)


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
