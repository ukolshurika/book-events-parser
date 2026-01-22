from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from tasks import get_book_location_events

# Load environment variables from .env file
load_dotenv()

app = FastAPI()


class BookRequest(BaseModel):
    s3_key: str
    user_id: str
    language: str = "en"


@app.post("/book")
async def create_book(request: BookRequest, background_tasks: BackgroundTasks):
    """
    Receives S3 key and user_id, then triggers async task to get book location events.
    """
    background_tasks.add_task(get_book_location_events, request.s3_key, request.user_id, request.language)
    return {"status": "OK"}
