import json
import logging

import httpx

from config import get_yandex_api_key, get_yandex_folder_id

logger = logging.getLogger(__name__)


async def extract_events_from_text(page_text: str, language: str) -> list[dict]:
    """
    Sends page text to YandexGPT API to extract events with dates and locations.

    Args:
        page_text: The text content of the page
        language: Language code for processing

    Returns:
        List of events with structure: [{name: str, date: str, geo: str|None}]
    """
    api_key = get_yandex_api_key()
    folder_id = get_yandex_folder_id()

    if not api_key or not folder_id:
        logger.warning("YandexGPT API credentials not configured")
        return []

    url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"

    prompt = f"""Извлеки все исторические события из следующего текста. Используй только факты из текста. Будь точен и краток.
Для каждого события определи:
- Название события
- Дату события (если указана)
- Место события/географию (если указано, иначе null)

Верни ТОЛЬКО JSON-массив следующей структуры:
[{{"name": "Название события", "date": "Дата события", "geo": "Место события"}}, {{"name": "Второе событие", "date": "Дата второго события", "geo": null}}]

Если события не найдены, верни пустой массив: []

Текст для анализа:
{page_text}"""

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Api-Key {api_key}"
    }

    payload = {
        "modelUri": f"gpt://{folder_id}/yandexgpt-lite/latest",
        "completionOptions": {
            "stream": False,
            "temperature": 0,
            "maxTokens": 2000
        },
        "messages": [
            {
                "role": "user",
                "text": prompt
            }
        ]
    }

    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(url, headers=headers, json=payload)
            response.raise_for_status()

            result = response.json()

            # Extract the text response from YandexGPT
            if "result" in result and "alternatives" in result["result"]:
                alternatives = result["result"]["alternatives"]
                if alternatives and len(alternatives) > 0:
                    message_text = alternatives[0].get("message", {}).get("text", "")

                    # Try to parse JSON from the response
                    # Clean up the response in case there's markdown or extra text
                    message_text = message_text.strip()

                    # Find JSON array in the response
                    start_idx = message_text.find("[")
                    end_idx = message_text.rfind("]")

                    if start_idx != -1 and end_idx != -1:
                        json_str = message_text[start_idx:end_idx + 1]
                        events = json.loads(json_str)
                        logger.info(f"Extracted {len(events)} events from text")
                        return events
                    else:
                        logger.warning("No JSON array found in YandexGPT response")
                        return []

            logger.warning("Unexpected response format from YandexGPT")
            return []

    except httpx.HTTPError as e:
        logger.error(f"HTTP error calling YandexGPT API: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON from YandexGPT response: {e}")
        return []
    except Exception as e:
        logger.error(f"Error calling YandexGPT API: {e}")
        raise
