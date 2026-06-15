import logging
from app.services.llm import openrouter_chat_completion

logger = logging.getLogger("analytics_service.services.pii")

def mask_pii_text(text: str) -> str:
    """
    Analyzes text and returns a version with masked PII elements.
    Generic logic without any Temporal bindings.
    """
    if not text:
        return ""

    prompt = f"""
    You are a highly secure PII (Personally Identifiable Information) detection and masking assistant.
    Analyze the following text and mask any occurrences of:
    - Names of individuals (e.g., replace with [NAME])
    - Phone numbers (e.g., replace with [PHONE])
    - Specific school or village names (e.g., replace with [SCHOOL] or [LOCATION])

    Text to analyze:
    ---
    {text}
    ---

    Return ONLY the raw masked text. Do not add any conversational introductions, markdown code blocks, or extra notes.
    """
    
    logger.info("Triggering OpenRouter PII masking request")
    masked_text = openrouter_chat_completion(prompt)
    return masked_text
