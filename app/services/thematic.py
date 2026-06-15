import json
import logging
from typing import Dict, Any
from app.services.llm import openrouter_chat_completion

logger = logging.getLogger("analytics_service.services.thematic")

def extract_thematic_analysis(text: str) -> Dict[str, Any]:
    """
    Performs thematic analysis on text and returns theme metadata.
    """
    if not text:
        return {}

    prompt = f"""
    You are an expert educational researcher performing thematic analysis on school improvement reports.
    Extract the core objective from the following text and categorize it under a generalized, concise theme name (e.g., "Digital Infrastructure", "Parental Engagement", "Foundational Literacy").
    Ensure that the theme name NEVER includes names of individuals, schools, or villages.

    Text to analyze:
    ---
    {text}
    ---

    Your output MUST be a JSON object formatted exactly as:
    {{
        "theme_name": "Proposed Theme Name",
        "theme_definition": "Boundary description defining the theme",
        "keywords": ["keyword1", "keyword2", "keyword3"],
        "confidence_score": 0.95
    }}

    Return ONLY this raw JSON object. Do not include markdown code block syntax (like ```json ... ```).
    """

    logger.info("Triggering OpenRouter thematic analysis request")
    response_text = openrouter_chat_completion(prompt)

    # Clean up markdown code blocks if the LLM output includes them
    if response_text.startswith("```"):
        lines = response_text.splitlines()
        if lines[0].startswith("```json") or lines[0].startswith("```"):
            lines = lines[1:-1]
        response_text = "\n".join(lines).strip()

    try:
        theme_data = json.loads(response_text)
        return theme_data
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse theme analysis JSON from LLM: {response_text}. Error: {e}")
        raise RuntimeError(f"Invalid theme analysis JSON payload: {response_text}") from e
