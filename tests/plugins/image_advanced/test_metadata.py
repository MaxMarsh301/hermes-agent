from __future__ import annotations

import json
import os

import plugins.image_advanced as image_advanced


class _FakeHandler:
    def __init__(self, result):
        self._result = result

    def get(self):
        return self._result


class TestExecutionMetadata:
    def test_handler_loads_fal_key_from_hermes_env_store(self, monkeypatch):
        from hermes_cli import config as hermes_config

        monkeypatch.delenv("FAL_KEY", raising=False)
        monkeypatch.setattr(hermes_config, "get_env_value", lambda key: "test-fal-key" if key == "FAL_KEY" else None)

        def fake_submit(model, arguments):
            assert os.environ.get("FAL_KEY") == "test-fal-key"
            return _FakeHandler(
                {
                    "images": [
                        {
                            "url": "https://cdn.example.com/final.png",
                            "width": 1024,
                            "height": 1024,
                        }
                    ]
                }
            )

        monkeypatch.setattr(image_advanced, "_submit_fal_request", fake_submit)

        payload = json.loads(
            image_advanced._handle_image_generate_advanced(
                {
                    "prompt": "A cat in a spacesuit",
                    "mode": "generate",
                    "model": "fal-ai/nano-banana-pro",
                }
            )
        )

        assert payload["success"] is True

    def test_handler_returns_success_with_effective_parameters(self, monkeypatch):
        monkeypatch.setattr(
            image_advanced,
            "_submit_fal_request",
            lambda model, arguments: _FakeHandler(
                {
                    "images": [
                        {
                            "url": "https://cdn.example.com/final.png",
                            "width": 1024,
                            "height": 1024,
                        }
                    ]
                }
            ),
        )

        payload = json.loads(
            image_advanced._handle_image_generate_advanced(
                {
                    "prompt": "A polished poster",
                    "style_profile": "design",
                    "quality_profile": "high",
                    "provider_options": {"bogus": True},
                }
            )
        )

        assert payload["success"] is True
        assert payload["image"] == "https://cdn.example.com/final.png"
        assert payload["metadata"]["mode"] == "generate"
        assert payload["metadata"]["effective_model"] == "fal-ai/recraft/v4/pro/text-to-image"
        assert payload["metadata"]["submit_model"] == "fal-ai/recraft/v4/pro/text-to-image"
        assert payload["metadata"]["selection_source"] == "style_profile"
        assert payload["metadata"]["ignored_provider_options"] == ["bogus", "num_inference_steps"]
        assert payload["metadata"]["effective_parameters"]["aspect_ratio"] == "landscape"

    def test_handler_reports_fallback_info_when_profiles_cannot_serve_edit_mode(self, monkeypatch):
        monkeypatch.setattr(
            image_advanced,
            "_submit_fal_request",
            lambda model, arguments: _FakeHandler(
                {
                    "images": [
                        {
                            "url": "https://cdn.example.com/edited.png",
                            "width": 1024,
                            "height": 1024,
                        }
                    ]
                }
            ),
        )
        monkeypatch.setattr(
            image_advanced,
            "_source_to_data_uri",
            lambda source: f"data:image/png;base64,from-{source.split('/')[-1]}",
        )

        payload = json.loads(
            image_advanced._handle_image_generate_advanced(
                {
                    "prompt": "Turn this into a print ad",
                    "mode": "edit",
                    "style_profile": "design",
                    "quality_profile": "high",
                    "input_images": [{"source": "/tmp/base.png", "role": "base"}],
                }
            )
        )

        assert payload["success"] is True
        assert payload["metadata"]["effective_model"] == "fal-ai/nano-banana-pro"
        assert payload["metadata"]["submit_model"] == "fal-ai/nano-banana-pro/edit"
        assert payload["metadata"]["selection_source"] == "mode_default"
        assert payload["metadata"]["fallbacks"][0]["reason"] == "unsupported_mode_for_profile_model"
        assert payload["metadata"]["effective_parameters"]["input_image_count"] == 1
