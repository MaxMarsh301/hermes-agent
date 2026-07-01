from __future__ import annotations

import json

import pytest

import plugins.image_advanced as image_advanced


class TestSchema:
    def test_schema_exposes_advanced_fields(self):
        props = image_advanced.IMAGE_GENERATE_ADVANCED_SCHEMA["parameters"]["properties"]

        assert props["mode"]["enum"] == ["generate", "edit", "compose"]
        assert props["aspect_ratio"]["enum"] == ["landscape", "square", "portrait"]
        assert props["output_format"]["enum"] == ["png", "jpg", "webp"]
        assert props["input_images"]["items"]["required"] == ["source"]
        assert "provider_options" in props


class TestNormalization:
    def test_normalize_request_applies_defaults_and_lowercases(self):
        normalized = image_advanced._normalize_request(
            {
                "prompt": "  Neon cat  ",
                "mode": "EDIT",
                "style_profile": "CINEMATIC",
                "quality_profile": "HIGH",
                "aspect_ratio": "PORTRAIT",
                "provider_options": None,
                "input_images": [{"source": " /tmp/cat.png ", "role": " BASE "}],
            }
        )

        assert normalized["prompt"] == "Neon cat"
        assert normalized["mode"] == "edit"
        assert normalized["style_profile"] == "cinematic"
        assert normalized["quality_profile"] == "high"
        assert normalized["aspect_ratio"] == "portrait"
        assert normalized["provider_options"] == {}
        assert normalized["num_images"] == 1
        assert normalized["input_images"] == [{"source": "/tmp/cat.png", "role": "base"}]


class TestValidation:
    def test_edit_requires_input_images(self):
        result = image_advanced._validate_request(
            image_advanced._normalize_request({"prompt": "edit this", "mode": "edit"})
        )

        assert result["ok"] is False
        assert result["code"] == "input_images_required"
        assert result["field"] == "input_images"

    def test_compose_requires_two_images(self):
        result = image_advanced._validate_request(
            image_advanced._normalize_request(
                {
                    "prompt": "combine them",
                    "mode": "compose",
                    "input_images": [{"source": "/tmp/one.png", "role": "subject"}],
                }
            )
        )

        assert result["ok"] is False
        assert result["code"] == "compose_requires_multiple_images"

    def test_rejects_non_absolute_and_non_http_input_sources(self):
        result = image_advanced._validate_request(
            image_advanced._normalize_request(
                {
                    "prompt": "edit this",
                    "mode": "edit",
                    "input_images": [{"source": "relative/path.png", "role": "base"}],
                }
            )
        )

        assert result["ok"] is False
        assert result["code"] == "invalid_input_image_source"

    def test_preserve_flags_are_rejected_until_semantics_exist(self):
        result = image_advanced._validate_request(
            image_advanced._normalize_request(
                {
                    "prompt": "edit this",
                    "mode": "edit",
                    "preserve_subject": True,
                    "input_images": [{"source": "/tmp/one.png", "role": "base"}],
                }
            )
        )

        assert result["ok"] is False
        assert result["code"] == "unsupported_control"

    def test_model_capability_mismatch_is_rejected(self):
        result = image_advanced._validate_request(
            image_advanced._normalize_request(
                {
                    "prompt": "edit this",
                    "mode": "edit",
                    "model": "fal-ai/flux-2-pro",
                    "input_images": [{"source": "/tmp/one.png", "role": "base"}],
                }
            )
        )

        assert result["ok"] is False
        assert result["code"] == "unsupported_mode_for_model"
        assert "edit" in result["message"]

    def test_valid_generate_request_passes_validation(self):
        result = image_advanced._validate_request(
            image_advanced._normalize_request(
                {
                    "prompt": "generate a poster",
                    "mode": "generate",
                    "style_profile": "design",
                    "quality_profile": "high",
                }
            )
        )

        assert result == {"ok": True}

    def test_openai_and_grok_aliases_are_rejected_as_unknown_models(self):
        for model in ("openai-image", "grok-image"):
            result = image_advanced._validate_request(
                image_advanced._normalize_request(
                    {
                        "prompt": "generate a poster",
                        "mode": "generate",
                        "model": model,
                    }
                )
            )

            assert result["ok"] is False
            assert result["code"] == "unknown_model"
            assert result["field"] == "model"
            assert f"Unknown model '{model}'" in result["message"]


class TestCapabilityAndProfiles:
    def test_capability_map_exposes_mode_support(self):
        caps = image_advanced.MODEL_CAPABILITIES["fal-ai/nano-banana-pro"]

        assert caps["modes"] == {"generate", "edit", "compose"}
        assert caps["edit_model"] == "fal-ai/nano-banana-pro/edit"

    def test_only_real_fal_models_are_exposed(self):
        assert "openai-image" not in image_advanced.MODEL_CAPABILITIES
        assert "grok-image" not in image_advanced.MODEL_CAPABILITIES
        assert image_advanced.MODEL_CAPABILITIES["fal-ai/flux-2-pro"]["provider"] == "fal"

    def test_profile_maps_include_expected_entries(self):
        assert image_advanced.STYLE_PROFILE_MAP["cinematic"]["preferred_model"] == "fal-ai/flux-2-pro"
        assert image_advanced.STYLE_PROFILE_MAP["design"]["preferred_model"] == "fal-ai/recraft/v4/pro/text-to-image"
        assert image_advanced.QUALITY_PROFILE_MAP["fast"]["preferred_model"] == "fal-ai/z-image/turbo"
        assert image_advanced.QUALITY_PROFILE_MAP["high"]["preferred_model"] == "fal-ai/flux-2-pro"


class TestHandlerValidationResponse:
    def test_handler_returns_structured_validation_error(self):
        payload = json.loads(
            image_advanced._handle_image_generate_advanced(
                {"prompt": "edit this", "mode": "edit"}
            )
        )

        assert payload["success"] is False
        assert payload["error_type"] == "validation_error"
        assert payload["validation"]["code"] == "input_images_required"
        assert payload["received"]["mode"] == "edit"

    def test_handler_returns_execution_error_shape_for_valid_request_without_backend(self):
        payload = json.loads(
            image_advanced._handle_image_generate_advanced(
                {
                    "prompt": "generate a poster",
                    "style_profile": "design",
                    "quality_profile": "high",
                }
            )
        )

        assert payload["success"] is False
        assert payload["normalized_request"]["style_profile"] == "design"
        assert payload["normalized_request"]["quality_profile"] == "high"
        assert payload["error_type"]

    def test_handler_passthroughs_provider_error_payload(self, monkeypatch):
        class _Request:
            def get(self):
                return {
                    "success": False,
                    "error": "FAL returned no image data",
                    "error_type": "empty_response",
                    "provider": "fal",
                    "provider_response": {"data": []},
                }

        monkeypatch.setattr(image_advanced, "_ensure_fal_credentials_loaded", lambda: None)
        monkeypatch.setattr(image_advanced, "_submit_fal_request", lambda model, arguments: _Request())

        payload = json.loads(
            image_advanced._handle_image_generate_advanced(
                {
                    "prompt": "generate a poster",
                    "mode": "generate",
                    "model": "gpt-image-1",
                }
            )
        )

        assert payload["success"] is False
        assert payload["error_type"] == "validation_error"
        assert payload["validation"]["code"] == "unknown_model"
        assert payload["validation"]["field"] == "model"
