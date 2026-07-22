import json
import logging
import urllib.error
import urllib.request
from typing import Any, Dict, Optional, Tuple
from app.config import settings

logger = logging.getLogger("analytics_service.services.llm")

def openrouter_chat_completion(
    prompt: str,
    model: Optional[str] = None,
    max_tokens: Optional[int] = None,
    timeout: Optional[int] = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    Makes a synchronous HTTP request to OpenRouter to generate content.
    Independent of Temporal and can be called from CLI/scripts.

    model/max_tokens/timeout override the global settings for this call only;
    omit (or pass None) to fall back to OPENROUTER_MODEL/LLM_MAX_TOKENS/LLM_TIMEOUT_SECONDS.

    Returns (content, usage) — usage is OpenRouter's raw `usage` object (exact
    prompt_tokens/completion_tokens/total_tokens, cost, and provider-specific
    breakdowns), or {} in the unexpected case the response omitted it.
    """
    api_key = settings.OPENROUTER_API_KEY
    model = model or settings.OPENROUTER_MODEL
    max_tokens = max_tokens or settings.LLM_MAX_TOKENS
    timeout = timeout or settings.LLM_TIMEOUT_SECONDS

    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY environment variable is not set")

    # Log the raw prompt to the terminal console under DEBUG level to avoid cluttering standard logs
    logger.debug(f"\n=================== [LLM CALL] RAW PROMPT SENT TO MODEL '{model}' ===================\n{prompt}\n=================================================================================")

    request_body = {
        "model": model,
        "temperature": settings.LLM_TEMPERATURE,
        "max_tokens": max_tokens,
        "messages": [
            {
                "role": "user",
                "content": prompt,
            }
        ],
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://github.com/shikshalokam/analytics-temporal-poc",
        "X-Title": "analytics-temporal-poc",
    }

    request = urllib.request.Request(
        f"{settings.OPENROUTER_BASE_URL.rstrip('/')}/chat/completions",
        data=json.dumps(request_body).encode("utf-8"),
        headers=headers,
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            response_data = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8", errors="replace")
        logger.error(f"OpenRouter HTTP Error {e.code}: {error_body}")
        raise RuntimeError(f"OpenRouter request failed with HTTP {e.code}: {error_body}") from e
    except Exception as e:
        logger.error(f"OpenRouter Connection Error: {e}")
        raise RuntimeError(f"Failed to connect to OpenRouter: {e}") from e

    try:
        choice = response_data["choices"][0]
        message = choice["message"]
        content = message.get("content")
    except (KeyError, IndexError, TypeError) as e:
        raise RuntimeError(f"Unexpected OpenRouter response shape: {response_data}") from e

    if isinstance(content, list):
        content = "\n".join(str(part.get("text", part)) for part in content)

    if not content:
        raise RuntimeError("OpenRouter response did not include content.")

    usage = response_data.get("usage") or {}
    return content.strip(), usage


def split_llm_usage(usage: Optional[Dict[str, Any]]) -> Tuple[int, int, Dict[str, Any]]:
    """
    Splits an OpenRouter `usage` object into (prompt_tokens, completion_tokens, meta_data)
    for llm_logs: the first two map to their own dedicated columns (total_tokens is a
    DB-generated column, derived automatically from those two — not stored directly).
    Everything else (cost, cached/reasoning token breakdowns, etc.) goes into meta_data
    for the full audit trail.
    """
    usage = usage or {}
    prompt_tokens = int(usage.get("prompt_tokens") or 0)
    completion_tokens = int(usage.get("completion_tokens") or 0)
    meta_data = {k: v for k, v in usage.items() if k not in ("prompt_tokens", "completion_tokens", "total_tokens")}
    return prompt_tokens, completion_tokens, meta_data
