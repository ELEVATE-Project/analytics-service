"""
Pure core of the feedback metadata extraction: no database, no network, no Temporal.

One LLM call per stored feedback session receives the COMPLETE conversation (Mitra or Google Forms, any
questionnaire version) and returns, as JSON, a value for each configured metadata field plus a whole-conversation
PII flag. This module

  * parses and validates the extraction config (`parse_extraction_config`),
  * turns `feedbacks` rows into the conversation text (`build_conversation`),
  * renders the prompt templates (`render_prompt`),
  * validates the model's answer (`parse_model_response`): the answer is UNTRUSTED, it is parsed as strict JSON
    and every field is checked against the allowed values; a value outside them is never coerced, it is kept as
    'unmapped' with the raw value,
  * and runs the whole sequence with an injected LLM callable (`extract`), so everything is unit-testable.

The respondents include children and the conversation is DATA, never instructions: it is delimited with an id
derived from its own text, every message is collapsed to one line, and the template tells the model to ignore
instructions inside it. Nothing here logs conversation text, and no error message contains it.
"""
import hashlib
import json
import logging
import math
import re
from dataclasses import dataclass, replace
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from app.services.feedback_extraction_constants import (
    EXTRACTION_CONFIG_SCHEMA_VERSION,
    FIELD_KINDS as _CONSTANT_FIELD_KINDS,
    KIND_CATEGORY,
    FREE_TEXT_PLACEHOLDERS,
    KIND_FREE_TEXT,
    MAX_BOT_MESSAGE_CHARS,
    MAX_FREE_TEXT_VALUE_CHARS,
    MAX_PII_TYPES,
    MAX_RAW_VALUE_CHARS,
    MAX_RESPONDENT_MESSAGE_CHARS,
    PII_MIN_DIGIT_RUN,
    PII_TYPE_DESCRIPTIONS,
    PII_TYPES,
    STATUS_EXTRACTED,
    STATUS_NOT_PROVIDED,
    STATUS_UNMAPPED,
)

logger = logging.getLogger("analytics_service.feedback_extraction")

# The engine owns the 'integer' kind (a whole number inside [min, max], for example an age). Defined here so the
# parser does not depend on the order in which the seed constants are edited; if the constants module also lists
# it, the tuple below stays free of duplicates.
KIND_INTEGER = "integer"
FIELD_KINDS: Tuple[str, ...] = tuple(dict.fromkeys((*_CONSTANT_FIELD_KINDS, KIND_INTEGER)))
# The bounds of an integer field and the longest digit string of a model value that is even looked at: a number with
# more digits than this is out of any sensible range and becomes 'unmapped' without being converted.
MAX_INTEGER_BOUND = 10 ** 15
_MAX_INTEGER_DIGITS = 18
SENSITIVE_RULE = "(sensitive: fill only when the respondent states it explicitly; never infer)"


class ExtractionConfigError(ValueError):
    """The extraction config (a config_version.config JSON) is malformed."""


class PromptRenderError(ValueError):
    """A prompt template uses a variable that does not exist (or the config cannot fill one)."""


class ExtractionOutputError(ValueError):
    """The model's answer is not what the output contract allows. The message never contains model or respondent text."""

    def __init__(self, message: str):
        super().__init__(message)
        # Token usage of the call that produced the bad answer (set by `extract`), so it can still be logged.
        self.usage: Dict[str, Any] = {}


# ------------------------------------------------------------------------------------------------ config

@dataclass(frozen=True)
class FieldSpec:
    name: str
    kind: str                         # 'category' | 'free_text' | 'integer'
    description: str
    categories: Tuple[str, ...] = ()  # allowed values (category fields only)
    guidance: Optional[Dict[str, str]] = None  # category -> how to recognise it (shown to the model)
    provisional: bool = False         # product has not confirmed the bands
    min_value: Optional[int] = None   # integer fields only: smallest accepted value (inclusive)
    max_value: Optional[int] = None   # integer fields only: largest accepted value (inclusive)
    sensitive: bool = False           # prompt rule only (fill when stated explicitly); never changes validation


@dataclass(frozen=True)
class ExtractionConfig:
    persona: str
    session_type: str
    fields: Tuple[FieldSpec, ...]
    pii_types: Tuple[str, ...]
    llm: Dict[str, Any]               # optional model / max_tokens / timeout_seconds, each None when unset
    prompt_name: Optional[str] = None  # exact prompts.name of this persona's prompt; None = the generic prompt

    def field_names(self) -> Tuple[str, ...]:
        return tuple(f.name for f in self.fields)


_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_TOP_KEYS = {"schema_version", "persona", "session_type", "source_note", "fields", "pii", "llm", "prompt_name"}
_FIELD_KEYS = {"name", "kind", "description", "categories", "guidance", "provisional", "min", "max", "sensitive"}
_LLM_KEYS = {"model", "max_tokens", "timeout_seconds"}


def _norm(text: str) -> str:
    """Comparison key of a band: case-insensitive, whitespace collapsed. Used only to find the canonical spelling."""
    return re.sub(r"\s+", " ", text).strip().casefold()


def _is_text(value: Any) -> bool:
    return isinstance(value, str) and len(value.strip()) > 0


def _is_positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _is_plain_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def parse_extraction_config(raw: Any) -> ExtractionConfig:
    """Validates a config_version.config JSON (a dict or its JSON text) and returns it parsed. Never defaults silently."""
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except ValueError as exc:
            raise ExtractionConfigError("extraction config is not valid JSON") from exc
    if not isinstance(raw, dict):
        raise ExtractionConfigError("extraction config must be a JSON object")
    unknown = sorted(set(raw) - _TOP_KEYS)
    if len(unknown) > 0:
        raise ExtractionConfigError(f"extraction config has unknown key(s): {unknown}")
    if raw.get("schema_version") != EXTRACTION_CONFIG_SCHEMA_VERSION or isinstance(raw.get("schema_version"), bool):
        raise ExtractionConfigError(f"extraction config schema_version must be {EXTRACTION_CONFIG_SCHEMA_VERSION}")
    persona = raw.get("persona")
    session_type = raw.get("session_type")
    if not _is_text(persona):
        raise ExtractionConfigError("extraction config needs a non-empty persona")
    if not _is_text(session_type):
        raise ExtractionConfigError("extraction config needs a non-empty session_type")

    prompt_name: Optional[str] = None
    if "prompt_name" in raw:
        prompt_name = raw["prompt_name"]
        if not _is_text(prompt_name) or prompt_name != prompt_name.strip():
            raise ExtractionConfigError(
                "extraction config prompt_name must be a non-empty string without surrounding spaces "
                "(the exact name of a prompts row) or be left out")

    raw_fields = raw.get("fields")
    if not isinstance(raw_fields, list) or len(raw_fields) == 0:
        raise ExtractionConfigError("extraction config needs a non-empty list of fields")
    fields: List[FieldSpec] = []
    seen_names = set()
    for index, item in enumerate(raw_fields):
        where = f"fields[{index}]"
        if not isinstance(item, dict):
            raise ExtractionConfigError(f"{where} must be an object")
        unknown = sorted(set(item) - _FIELD_KEYS)
        if len(unknown) > 0:
            raise ExtractionConfigError(f"{where} has unknown key(s): {unknown}")
        name = item.get("name")
        if not isinstance(name, str) or _NAME_RE.match(name) is None:
            raise ExtractionConfigError(f"{where}.name must match {_NAME_RE.pattern}")
        if name in seen_names:
            raise ExtractionConfigError(f"duplicate field name {name!r}")
        seen_names.add(name)
        kind = item.get("kind")
        if kind not in FIELD_KINDS:
            raise ExtractionConfigError(f"{where}.kind must be one of {FIELD_KINDS}")
        if not _is_text(item.get("description")):
            raise ExtractionConfigError(f"{where}.description must be a non-empty string")
        provisional = item.get("provisional", False)
        if not isinstance(provisional, bool):
            raise ExtractionConfigError(f"{where}.provisional must be a boolean")
        sensitive = item.get("sensitive", False)
        if not isinstance(sensitive, bool):
            raise ExtractionConfigError(f"{where}.sensitive must be a boolean")
        min_value: Optional[int] = None
        max_value: Optional[int] = None
        categories: Tuple[str, ...] = ()
        guidance = None
        if kind == KIND_CATEGORY:
            cats = item.get("categories")
            if not isinstance(cats, list) or len(cats) == 0:
                raise ExtractionConfigError(f"{where}.categories must be a non-empty list for a category field")
            seen_keys = set()
            for cat in cats:
                if not _is_text(cat) or cat != cat.strip():
                    raise ExtractionConfigError(f"{where}.categories must be non-empty strings without surrounding spaces")
                if _norm(cat) in seen_keys:
                    raise ExtractionConfigError(f"{where}.categories has a duplicate value (case-insensitive)")
                seen_keys.add(_norm(cat))
            categories = tuple(cats)
            guidance = item.get("guidance")
            if guidance is not None:
                if not isinstance(guidance, dict) or any(
                        not isinstance(k, str) or not _is_text(v) for k, v in guidance.items()):
                    raise ExtractionConfigError(f"{where}.guidance must map a category to a non-empty string")
                stray = sorted(set(guidance) - set(categories))
                if len(stray) > 0:
                    raise ExtractionConfigError(f"{where}.guidance names value(s) that are not categories: {stray}")
                guidance = dict(guidance)
            if "min" in item or "max" in item:
                raise ExtractionConfigError(f"{where}: a category field has no min or max")
        elif kind == KIND_INTEGER:
            if "categories" in item or "guidance" in item:
                raise ExtractionConfigError(f"{where}: an integer field has no categories or guidance")
            min_value, max_value = item.get("min"), item.get("max")
            if not _is_plain_int(min_value) or not _is_plain_int(max_value):
                raise ExtractionConfigError(f"{where}: an integer field needs integer min and max (not booleans)")
            if abs(min_value) > MAX_INTEGER_BOUND or abs(max_value) > MAX_INTEGER_BOUND:
                raise ExtractionConfigError(f"{where}: min and max must be within +-{MAX_INTEGER_BOUND}")
            if min_value > max_value:
                raise ExtractionConfigError(f"{where}: min must not be greater than max")
        else:
            if "categories" in item or "guidance" in item:
                raise ExtractionConfigError(f"{where}: a free_text field has no categories or guidance")
            if "min" in item or "max" in item:
                raise ExtractionConfigError(f"{where}: a free_text field has no min or max")
        fields.append(FieldSpec(name, kind, item["description"].strip(), categories, guidance, provisional,
                                min_value, max_value, sensitive))

    pii_raw = raw.get("pii")
    pii_types: Tuple[str, ...] = PII_TYPES
    if pii_raw is not None:
        if not isinstance(pii_raw, dict) or set(pii_raw) != {"types"}:
            raise ExtractionConfigError("pii must be an object with exactly the key 'types'")
        types = pii_raw["types"]
        if not isinstance(types, list) or len(types) == 0 or any(not _is_text(t) or t != t.strip().lower() for t in types):
            raise ExtractionConfigError("pii.types must be a non-empty list of lower-case strings")
        if len(set(types)) != len(types) or "other" not in types:
            raise ExtractionConfigError("pii.types must be unique and include 'other'")
        pii_types = tuple(types)

    llm_raw = raw.get("llm")
    llm: Dict[str, Any] = {"model": None, "max_tokens": None, "timeout_seconds": None}
    if llm_raw is not None:
        if not isinstance(llm_raw, dict):
            raise ExtractionConfigError("llm must be an object")
        unknown = sorted(set(llm_raw) - _LLM_KEYS)
        if len(unknown) > 0:
            raise ExtractionConfigError(f"llm has unknown key(s): {unknown}")
        if llm_raw.get("model") is not None and not _is_text(llm_raw["model"]):
            raise ExtractionConfigError("llm.model must be a non-empty string or null")
        for key in ("max_tokens", "timeout_seconds"):
            if llm_raw.get(key) is not None and not _is_positive_int(llm_raw[key]):
                raise ExtractionConfigError(f"llm.{key} must be a positive integer or null")
        for key in _LLM_KEYS:
            if key in llm_raw:
                llm[key] = llm_raw[key]
    return ExtractionConfig(persona.strip(), session_type.strip(), tuple(fields), pii_types, llm, prompt_name)


# ------------------------------------------------------------------------------------------------ conversation

ROLE_BOT = "Bot"
ROLE_RESPONDENT = "Respondent"
# Control characters become SPACES (not nothing), so "Asha\x0bKumar" stays two words.
_CONTROL_RE = re.compile(r"[\x00-\x08\x0b-\x1f\x7f]")
_SPACE_RE = re.compile(r"\s+")
_TRUNCATED = " [truncated]"


@dataclass(frozen=True)
class Turn:
    sequence: int
    role: str
    text: str


@dataclass(frozen=True)
class Conversation:
    turns: Tuple[Turn, ...]
    text: str                 # one line per turn: "[sequence] Role: text"
    conversation_id: str      # derived from the text; delimits it in the prompt
    respondent_answers: int   # number of non-blank respondent answers
    oversized_respondent_message: bool = False  # a respondent message longer than MAX_RESPONDENT_MESSAGE_CHARS


def _clean(value: Any) -> str:
    """One line, control characters turned into spaces, whitespace collapsed; None and non-strings are blank. Never cut."""
    if not isinstance(value, str):
        return ""
    return _SPACE_RE.sub(" ", _CONTROL_RE.sub(" ", value)).strip()


def _cap_bot(text: str) -> str:
    """Bot messages are the questionnaire's own text and carry no respondent PII: a very long one is cut."""
    if len(text) > MAX_BOT_MESSAGE_CHARS:
        return text[:MAX_BOT_MESSAGE_CHARS] + _TRUNCATED
    return text


def build_conversation(rows: Sequence[Mapping[str, Any]], closing_keys: Sequence[str] = ()) -> Conversation:
    """
    Builds the conversation text from the `feedbacks` rows of one session (any order; sorted by `sequence`).

    A row is an answer (Respondent) exactly when `parent_id` is set; every other row is a Bot row (question,
    probe, opening message, closing message). The English column is `translated_message`: a bot row shows it
    (falling back to `message`); a respondent row shows the original `message` and, when it differs, the English
    translation, so the model sees both. A bot row whose `question_key` is one of `closing_keys` (the thank-you
    message of the upload config) is left out. Blank rows are skipped. Respondent messages are NEVER truncated
    (a cut would hide PII after it); if one is longer than MAX_RESPONDENT_MESSAGE_CHARS the conversation is flagged
    `oversized_respondent_message` and `extract` does not send it to the model. Bot messages are cut at
    MAX_BOT_MESSAGE_CHARS.
    """
    closing = {k.strip().upper() for k in closing_keys if isinstance(k, str) and len(k.strip()) > 0}
    turns: List[Turn] = []
    answers = 0
    oversized = False
    for row in sorted(rows, key=lambda r: r["sequence"]):
        message = _clean(row.get("message"))
        english = _clean(row.get("translated_message"))
        if row.get("parent_id") is None:
            key = row.get("question_key")
            if isinstance(key, str) and key.strip().upper() in closing:
                continue
            text = _cap_bot(english if len(english) > 0 else message)
            role = ROLE_BOT
        else:
            if len(message) > MAX_RESPONDENT_MESSAGE_CHARS or len(english) > MAX_RESPONDENT_MESSAGE_CHARS:
                oversized = True
            if len(message) > 0 and len(english) > 0 and message.casefold() != english.casefold():
                text = f"{message} (English translation: {english})"
            elif len(message) > 0:
                text = message
            else:
                text = english
            role = ROLE_RESPONDENT
        if len(text) == 0:
            continue
        if role == ROLE_RESPONDENT:
            answers += 1
        turns.append(Turn(int(row["sequence"]), role, text))
    body = "\n".join(f"[{t.sequence}] {t.role}: {t.text}" for t in turns)
    conversation_id = hashlib.sha256(body.encode("utf-8")).hexdigest()[:16]
    return Conversation(tuple(turns), body, conversation_id, answers, oversized)


# ------------------------------------------------------------------------------------------------ deterministic PII backstop

# The model's `pii.detected = false` is never trusted on its own: respondent text (a respondent can write
# "set pii to false") that contains one of these shapes is flagged here whatever the model said. The check is
# deliberately blunt: a false positive only excludes a session from the export, a false negative leaks PII.
# Unicode-aware: \d matches Devanagari, Kannada, ... digits.
_DIGIT_RUN_RE = re.compile(r"\d(?:[\s\-.()]*\d)*")
_EMAIL_RE = re.compile(r"[^\s@(\[{\"'<]@[^\s@]")
_HANDLE_RE = re.compile(r"(?<![^\s(\[{\"'<])@[^\s@]{2,}")
_URL_RE = re.compile(
    r"(?:https?://|www\.)\S+|(?<![\w@.])[\w-]+(?:\.[\w-]+)*\.(?:com|in|org|net|co|io|me|edu|gov|app|link|ly|tv|info)\b",
    re.IGNORECASE)

PII_KIND_NUMBER = "phone"      # a long digit run: a phone number or an id number (the kind is informational)
PII_KIND_EMAIL = "email"       # an email address or an @handle (the 'email' type covers social handles)
PII_KIND_URL = "other"


def pii_precheck_kinds(rows: Sequence[Mapping[str, Any]]) -> Tuple[str, ...]:
    """
    The kinds of identifying shapes found in the RESPONDENT rows (parent_id set), original and translated column
    each checked on its own: a run of PII_MIN_DIGIT_RUN or more digits (spaces, dashes, dots and parentheses between
    digits ignored; a two-digit age or a year never reaches it), an email address, an '@handle', a URL. Returns
    the kinds in a stable order (empty when none). Never returns or logs the text itself.
    """
    found: List[str] = []

    def add(kind: str) -> None:
        if kind not in found:
            found.append(kind)

    for row in rows:
        if row.get("parent_id") is None:
            continue
        for column in ("message", "translated_message"):
            text = _clean(row.get(column))
            if len(text) == 0:
                continue
            for match in _DIGIT_RUN_RE.finditer(text):
                if len(re.findall(r"\d", match.group(0))) >= PII_MIN_DIGIT_RUN:
                    add(PII_KIND_NUMBER)
                    break
            if _EMAIL_RE.search(text) is not None or _HANDLE_RE.search(text) is not None:
                add(PII_KIND_EMAIL)
            without_email = re.sub(r"\S*@\S*", " ", text)
            if _URL_RE.search(without_email) is not None:
                add(PII_KIND_URL)
    return tuple(found)


def apply_pii_precheck(result: "ExtractionResult", rows: Sequence[Mapping[str, Any]],
                       config: ExtractionConfig) -> "ExtractionResult":
    """Forces pii_detected TRUE (and adds the matching types) when the deterministic pre-check finds something."""
    kinds = pii_precheck_kinds(rows)
    if len(kinds) == 0:
        return result
    types = list(result.pii_types)
    for kind in kinds:
        label = kind if kind in config.pii_types else "other"
        if label not in types:
            types.append(label)
    if not result.pii_detected:
        # Only the kinds are logged, never the text: how often the model said false where the pre-check disagrees.
        logger.info("PII pre-check overrode the model's pii.detected=false, kinds=%s", list(kinds))
    return replace(result, pii_detected=True, pii_types=tuple(types))


# ------------------------------------------------------------------------------------------------ prompt

_VAR_RE = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")


def _render_fields(config: ExtractionConfig) -> str:
    lines: List[str] = []
    for number, spec in enumerate(config.fields, start=1):
        if spec.kind == KIND_CATEGORY:
            lines.append(f"{number}. {spec.name} (choose exactly one allowed value): {spec.description}")
            lines.append("   Allowed values: " + "; ".join(spec.categories))
            if spec.guidance is not None and len(spec.guidance) > 0:
                lines.append("   How to recognise them: " + "; ".join(f"{k} = {v}" for k, v in spec.guidance.items()))
        elif spec.kind == KIND_INTEGER:
            lines.append(
                f"{number}. {spec.name} (a whole number from {spec.min_value} to {spec.max_value}; return it as a "
                f"JSON number, null when not stated): {spec.description}")
        else:
            lines.append(f"{number}. {spec.name} (free text): {spec.description}")
        if spec.sensitive:
            lines.append("   " + SENSITIVE_RULE)
    return "\n".join(lines)


def _render_pii_types(config: ExtractionConfig) -> str:
    return "\n".join(f"- {t}: {PII_TYPE_DESCRIPTIONS.get(t, 'as named')}" for t in config.pii_types)


def _render_output_format(config: ExtractionConfig) -> str:
    example = {
        "fields": {n: {"value": None, "raw": None, "confidence": None} for n in config.field_names()},
        "pii": {"detected": False, "types": []},
    }
    return json.dumps(example, indent=2)


def _fill(template: str, variables: Mapping[str, str]) -> str:
    """Single pass: a variable's value is never scanned again, so text inside it cannot inject another variable."""
    def replace(match: "re.Match[str]") -> str:
        name = match.group(1)
        if name not in variables:
            raise PromptRenderError(f"prompt template uses an unknown variable {{{{{name}}}}}")
        return variables[name]
    return _VAR_RE.sub(replace, template)


def render_prompt(system_template: str, user_template: str, config: ExtractionConfig, conversation: Conversation) -> str:
    """
    The full prompt (system part, blank line, user part) as one string, the way the other activities send it.
    Variables: persona, fields, field_names, pii_types, output_format, conversation_id, conversation.
    """
    variables = {
        "persona": config.persona,
        "fields": _render_fields(config),
        "field_names": ", ".join(config.field_names()),
        "pii_types": _render_pii_types(config),
        "output_format": _render_output_format(config),
        "conversation_id": conversation.conversation_id,
        "conversation": conversation.text,
    }
    return _fill(system_template, variables) + "\n\n" + _fill(user_template, variables)


# ------------------------------------------------------------------------------------------------ model answer

@dataclass(frozen=True)
class FieldResult:
    field_name: str
    value: Optional[str]       # the band / canonical value; None when not provided or unmapped
    raw_value: Optional[str]   # as the model returned it; kept for unmapped
    status: str                # 'extracted' | 'not_provided' | 'unmapped'
    confidence: Optional[float]


@dataclass(frozen=True)
class ExtractionResult:
    fields: Tuple[FieldResult, ...]
    pii_detected: bool
    pii_types: Tuple[str, ...]


@dataclass(frozen=True)
class ExtractionRun:
    result: ExtractionResult
    usage: Dict[str, Any]
    llm_called: bool           # False when no call was made (see skip_reason)
    conversation_turns: int
    skip_reason: Optional[str] = None   # SKIP_NO_ANSWER | SKIP_OVERSIZED when llm_called is False, else None


SKIP_NO_ANSWER = "no_respondent_answer"
SKIP_OVERSIZED = "oversized_respondent_message"


def empty_result(config: ExtractionConfig) -> ExtractionResult:
    """A conversation with no respondent answer: nothing was provided and there is nothing that could be PII."""
    return ExtractionResult(
        tuple(FieldResult(n, None, None, STATUS_NOT_PROVIDED, None) for n in config.field_names()), False, ())


def oversized_result(config: ExtractionConfig) -> ExtractionResult:
    """
    A respondent message longer than MAX_RESPONDENT_MESSAGE_CHARS is not sent to the model (it would have to be cut,
    and PII after the cut would be lost). The session is flagged pii_detected TRUE with type 'other', so it is
    excluded from the export and a person can look at it. The respondent did answer, so the fields are 'unmapped'.
    """
    return ExtractionResult(
        tuple(FieldResult(n, None, None, STATUS_UNMAPPED, None) for n in config.field_names()), True, ("other",))


def _strip_fences(text: str) -> str:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        lines = cleaned.splitlines()
        lines = lines[1:]
        if len(lines) > 0 and lines[-1].strip() == "```":
            lines = lines[:-1]
        cleaned = "\n".join(lines).strip()
    return cleaned


def _no_duplicates(pairs: List[Tuple[str, Any]]) -> Dict[str, Any]:
    keys = [k for k, _ in pairs]
    if len(set(keys)) != len(keys):
        raise ValueError("duplicate key")
    return dict(pairs)


def _reject_constant(_name: str) -> Any:
    raise ValueError("non-finite number")


def _text_or_none(value: Any, where: str) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ExtractionOutputError(f"{where} must be a string or null")
    stripped = _SPACE_RE.sub(" ", value).strip()
    return None if len(stripped) == 0 else stripped


def _is_placeholder(text: str) -> bool:
    """'N/A', 'null', 'Not provided', 'unknown', ...: case-insensitive, spaces and surrounding punctuation ignored."""
    key = _SPACE_RE.sub(" ", text.replace("\u2019", "'")).strip(" \t.,;:!?\"'()[]{}<>-_*~`").casefold()
    return len(key) == 0 or key in FREE_TEXT_PLACEHOLDERS


_INTEGER_TEXT_RE = re.compile(r"-?[0-9]{1,%d}" % _MAX_INTEGER_DIGITS)


def _integer_candidate(value: Any) -> Tuple[Optional[int], str]:
    """(the integer the model's value stands for or None, the model's value as short text for the raw column)."""
    if isinstance(value, bool):
        return None, "true" if value else "false"
    if isinstance(value, int):
        text = str(value) if len(str(value)) <= MAX_RAW_VALUE_CHARS else str(value)[:MAX_RAW_VALUE_CHARS]
        return (value if len(str(abs(value))) <= _MAX_INTEGER_DIGITS else None), text
    if isinstance(value, float):
        text = repr(value)
        if math.isfinite(value) and value.is_integer() and abs(value) < 10 ** _MAX_INTEGER_DIGITS:
            return int(value), text
        return None, text
    text = value[:MAX_RAW_VALUE_CHARS]
    if _INTEGER_TEXT_RE.fullmatch(value) is not None:     # ASCII digits only, optional leading minus, no exponent or point
        return int(value), text
    return None, text


def _map_integer(spec: FieldSpec, value: Any, raw: Optional[str], confidence: Optional[float]) -> FieldResult:
    """
    An integer field: null / blank / a placeholder is not_provided (or unmapped with the respondent's words when
    `raw` holds something that is not a placeholder); a JSON integer, an integral float or a numeric string inside
    [min, max] is extracted with the canonical decimal string as value; anything else (text, 17.5, out of range,
    a boolean) is unmapped with the raw kept.
    """
    if isinstance(value, str) and _is_placeholder(value):
        value = None
    if value is None:
        if raw is not None and _is_placeholder(raw):
            raw = None
        if raw is None:
            return FieldResult(spec.name, None, None, STATUS_NOT_PROVIDED, confidence)
        return FieldResult(spec.name, None, raw, STATUS_UNMAPPED, confidence)
    number, model_text = _integer_candidate(value)
    kept_raw = raw if raw is not None else model_text
    if number is None or number < spec.min_value or number > spec.max_value:
        return FieldResult(spec.name, None, kept_raw, STATUS_UNMAPPED, confidence)
    return FieldResult(spec.name, str(number), kept_raw, STATUS_EXTRACTED, confidence)


def _map_field(spec: FieldSpec, entry: Mapping[str, Any]) -> FieldResult:
    where = f"field {spec.name!r}"
    unknown = set(entry) - {"value", "raw", "confidence"}
    if len(unknown) > 0:
        raise ExtractionOutputError(f"{where} has {len(unknown)} unexpected key(s)")
    if "value" not in entry:
        raise ExtractionOutputError(f"{where} has no 'value'")
    if spec.kind == KIND_INTEGER:
        value_in = entry["value"]
        if isinstance(value_in, (dict, list)):
            raise ExtractionOutputError(f"{where} value must be a number, a numeric string or null")
        if isinstance(value_in, (bool, int, float)):
            # Judged in _map_integer: a boolean is no number, a float is only an integer when it is exactly one (17.0).
            integer_text: Any = value_in
        else:
            integer_text = _text_or_none(value_in, f"{where} value")
        value = None
    else:
        integer_text = None
        value = _text_or_none(entry["value"], f"{where} value")
    raw = _text_or_none(entry.get("raw"), f"{where} raw")
    confidence = entry.get("confidence")
    if confidence is not None:
        # Optional and informational: a malformed one (85, "high", true) is ignored (stored NULL), it does not fail
        # the whole session. Unknown keys still fail (above).
        if isinstance(confidence, bool) or not isinstance(confidence, (int, float)) \
                or not math.isfinite(confidence) or confidence < 0 or confidence > 1:
            confidence = None
        else:
            confidence = float(confidence)
    if raw is not None and len(raw) > MAX_RAW_VALUE_CHARS:
        raw = raw[:MAX_RAW_VALUE_CHARS]

    if spec.kind == KIND_FREE_TEXT:
        # A placeholder ("N/A", "null", "Not provided", ...) is the model saying "no answer", not an answer.
        if value is not None and _is_placeholder(value):
            value = None
            if raw is not None and _is_placeholder(raw):
                raw = None
        elif value is None and raw is not None and _is_placeholder(raw):
            raw = None

    if spec.kind == KIND_INTEGER:
        return _map_integer(spec, integer_text, raw, confidence)

    if value is None:
        if raw is None:
            return FieldResult(spec.name, None, None, STATUS_NOT_PROVIDED, confidence)
        # The respondent answered but the model found no value: kept as unmapped with the words, not dropped.
        return FieldResult(spec.name, None, raw, STATUS_UNMAPPED, confidence)

    if spec.kind == KIND_CATEGORY:
        canonical = {_norm(c): c for c in spec.categories}.get(_norm(value))
        if canonical is not None:
            return FieldResult(spec.name, canonical, raw if raw is not None else value, STATUS_EXTRACTED, confidence)
        return FieldResult(spec.name, None, raw if raw is not None else value[:MAX_RAW_VALUE_CHARS],
                           STATUS_UNMAPPED, confidence)

    # free_text: taken as given (whitespace collapsed); a value too long to be a name of a place is not trusted.
    if len(value) > MAX_FREE_TEXT_VALUE_CHARS:
        return FieldResult(spec.name, None, raw if raw is not None else value[:MAX_RAW_VALUE_CHARS],
                           STATUS_UNMAPPED, confidence)
    return FieldResult(spec.name, value, raw if raw is not None else value, STATUS_EXTRACTED, confidence)


def parse_model_response(text: Any, config: ExtractionConfig) -> ExtractionResult:
    """
    Strictly validates the model's answer. Accepted shape (markdown fences around it are tolerated):

        {"fields": {"<name>": {"value": str|null, "raw": str|null, "confidence": number|null}, ...},
         "pii": {"detected": bool, "types": [str, ...]}}

    (for an 'integer' field "value" may also be a JSON number)

    Every configured field must be present and nothing else may be. Raises ExtractionOutputError for non-JSON,
    wrong types, unknown or missing keys, and a PII flag that contradicts its types; the message never contains
    the model's text. A field value outside the allowed values is NOT an error: it becomes status 'unmapped'.
    """
    if not isinstance(text, str):
        raise ExtractionOutputError("model answer is not text")
    try:
        data = json.loads(_strip_fences(text), object_pairs_hook=_no_duplicates, parse_constant=_reject_constant)
    except (ValueError, RecursionError) as exc:   # RecursionError: absurdly deep nesting
        raise ExtractionOutputError("model answer is not valid JSON (strict, no duplicate keys)") from exc
    if not isinstance(data, dict):
        raise ExtractionOutputError("model answer must be a JSON object")
    if set(data) != {"fields", "pii"}:
        raise ExtractionOutputError("model answer must have exactly the keys 'fields' and 'pii'")

    fields = data["fields"]
    if not isinstance(fields, dict):
        raise ExtractionOutputError("'fields' must be an object")
    expected = set(config.field_names())
    if set(fields) != expected:
        missing = len(expected - set(fields))
        extra = len(set(fields) - expected)
        raise ExtractionOutputError(f"'fields' must name exactly the configured fields ({missing} missing, {extra} unknown)")
    results: List[FieldResult] = []
    for spec in config.fields:
        entry = fields[spec.name]
        if not isinstance(entry, dict):
            raise ExtractionOutputError(f"field {spec.name!r} must be an object")
        results.append(_map_field(spec, entry))

    pii = data["pii"]
    if not isinstance(pii, dict) or not set(pii) <= {"detected", "types"} or "detected" not in pii:
        raise ExtractionOutputError("'pii' must be an object with 'detected' and optionally 'types'")
    detected = pii["detected"]
    if not isinstance(detected, bool):
        raise ExtractionOutputError("pii.detected must be true or false")
    raw_types = pii.get("types", [])
    if raw_types is None:
        raw_types = []
    if not isinstance(raw_types, list) or len(raw_types) > MAX_PII_TYPES or any(not isinstance(t, str) for t in raw_types):
        raise ExtractionOutputError("pii.types must be a short list of strings")
    types: List[str] = []
    for item in raw_types:
        label = _SPACE_RE.sub("_", item.strip().lower())
        if len(label) == 0:
            continue
        if label not in config.pii_types:
            label = "other"   # an informational label outside the configured list; the flag itself is untouched
        if label not in types:
            types.append(label)
    if len(types) > 0 and not detected:
        raise ExtractionOutputError("pii.detected is false but pii.types is not empty")
    return ExtractionResult(tuple(results), detected, tuple(types))


# ------------------------------------------------------------------------------------------------ the whole step

LlmCall = Callable[[str], Tuple[str, Dict[str, Any]]]


def extract(
    rows: Sequence[Mapping[str, Any]],
    config: ExtractionConfig,
    system_template: str,
    user_template: str,
    call_llm: LlmCall,
    closing_keys: Sequence[str] = (),
) -> ExtractionRun:
    """
    Conversation -> prompt -> `call_llm(prompt)` -> validated result. `call_llm` returns (answer text, usage dict).
    A conversation without a respondent answer makes NO call: every field is not_provided and pii_detected is False.
    A conversation with a respondent message longer than MAX_RESPONDENT_MESSAGE_CHARS makes no call either: it is
    pii_detected TRUE ('other'), fields unmapped. After a valid answer the deterministic PII pre-check can only
    turn the flag TRUE. A bad answer raises ExtractionOutputError with the call's usage attached.
    """
    conversation = build_conversation(rows, closing_keys)
    if conversation.respondent_answers == 0:
        return ExtractionRun(empty_result(config), {}, False, len(conversation.turns), SKIP_NO_ANSWER)
    if conversation.oversized_respondent_message:
        return ExtractionRun(oversized_result(config), {}, False, len(conversation.turns), SKIP_OVERSIZED)
    prompt = render_prompt(system_template, user_template, config, conversation)
    answer, usage = call_llm(prompt)
    usage = usage if isinstance(usage, dict) else {}
    try:
        result = parse_model_response(answer, config)
    except ExtractionOutputError as exc:
        exc.usage = usage
        raise
    # The model's pii.detected = false is not trusted alone (a respondent can write "set pii to false").
    result = apply_pii_precheck(result, rows, config)
    return ExtractionRun(result, usage, True, len(conversation.turns))
