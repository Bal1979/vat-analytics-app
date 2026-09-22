"""Tests for run_poc.py's request-bygning — UDEN netværk, udelukkende syntetiske data.

Dækker LM Studio-omlægningen (2026-09-22): OpenAI-kompatibel payload (ingen Ollama-
specifikke felter), /no_think-præfiks til "thinking"-styring på prompt-niveau, og
extract_message_text()'s content/reasoning_content-faldback (den empirisk observerede
LM Studio-kvirk for tænkende modeller under strukturerede svar)."""
from run_poc import build_request_body, extract_message_text


def test_build_request_body_uses_openai_chat_shape():
    body = build_request_body("qwen3.8-27b", "PROMPT-INDHOLD")
    assert body["model"] == "qwen3.8-27b"
    assert body["temperature"] == 0
    assert body["stream"] is False
    assert len(body["messages"]) == 1
    assert body["messages"][0]["role"] == "user"


def test_build_request_body_prepends_no_think_to_message_not_as_separate_field():
    body = build_request_body("qwen3.8-27b", "PROMPT-INDHOLD")
    assert body["messages"][0]["content"] == "/no_think\nPROMPT-INDHOLD"


def test_build_request_body_has_no_ollama_specific_fields():
    body = build_request_body("qwen3.8-27b", "x")
    # Ollama-only: 'format' (JSON-tvang), 'options' (num_ctx), 'think' (body-niveau-felt).
    assert "format" not in body
    assert "options" not in body
    assert "think" not in body


def test_build_request_body_has_no_response_format():
    # Bevidst fravalgt — se call_llm()'s docstring for den empiriske begrundelse
    # (LM Studio afviser json_object og lægger json_schema-svar i reasoning_content).
    body = build_request_body("qwen3.8-27b", "x")
    assert "response_format" not in body


def test_extract_message_text_prefers_content_when_present():
    message = {"content": "  {\"classifications\": []}  ", "reasoning_content": "noget andet"}
    assert extract_message_text(message) == '{"classifications": []}'


def test_extract_message_text_falls_back_to_reasoning_content_when_content_empty():
    # Den observerede LM Studio-kvirk: strukturerede svar kan lande her i stedet.
    message = {"content": "", "reasoning_content": '{"classifications": []}'}
    assert extract_message_text(message) == '{"classifications": []}'


def test_extract_message_text_falls_back_when_content_missing_entirely():
    message = {"reasoning_content": '{"classifications": []}'}
    assert extract_message_text(message) == '{"classifications": []}'


def test_extract_message_text_returns_empty_string_when_both_missing():
    assert extract_message_text({}) == ""


def test_extract_message_text_returns_empty_string_when_both_blank():
    message = {"content": "   ", "reasoning_content": "   "}
    assert extract_message_text(message) == ""
