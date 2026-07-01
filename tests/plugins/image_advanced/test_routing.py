from __future__ import annotations

import plugins.image_advanced as image_advanced


class TestModelSelection:
    def test_explicit_model_wins_over_profiles(self):
        routed = image_advanced._route_request(
            image_advanced._normalize_request(
                {
                    "prompt": "poster",
                    "model": "fal-ai/flux-2/klein/9b",
                    "style_profile": "design",
                    "quality_profile": "high",
                }
            )
        )

        assert routed["effective_model"] == "fal-ai/flux-2/klein/9b"
        assert routed["selection_source"] == "explicit_model"

    def test_style_profile_selects_model_for_generate(self):
        routed = image_advanced._route_request(
            image_advanced._normalize_request(
                {
                    "prompt": "brand poster",
                    "style_profile": "design",
                    "quality_profile": "fast",
                }
            )
        )

        assert routed["effective_model"] == "fal-ai/recraft/v4/pro/text-to-image"
        assert routed["selection_source"] == "style_profile"

    def test_quality_profile_selects_model_when_style_absent(self):
        routed = image_advanced._route_request(
            image_advanced._normalize_request(
                {
                    "prompt": "quick draft",
                    "quality_profile": "fast",
                }
            )
        )

        assert routed["effective_model"] == "fal-ai/z-image/turbo"
        assert routed["selection_source"] == "quality_profile"

    def test_edit_mode_falls_back_to_mode_default_when_profiles_pick_generate_only_models(self):
        routed = image_advanced._route_request(
            image_advanced._normalize_request(
                {
                    "prompt": "edit this photo",
                    "mode": "edit",
                    "style_profile": "design",
                    "quality_profile": "high",
                    "input_images": [{"source": "/tmp/photo.png", "role": "base"}],
                }
            )
        )

        assert routed["effective_model"] == "fal-ai/nano-banana-pro"
        assert routed["selection_source"] == "mode_default"
        assert routed["fallbacks"][0]["reason"] == "unsupported_mode_for_profile_model"

class TestPayloadBuilders:
    def test_generate_payload_filters_unsupported_provider_options(self):
        routed = image_advanced._route_request(
            image_advanced._normalize_request(
                {
                    "prompt": "dramatic city skyline",
                    "model": "fal-ai/flux-2-pro",
                    "provider_options": {
                        "guidance_scale": 7.5,
                        "resolution": "4K",
                        "bogus": "drop-me",
                    },
                }
            )
        )

        payload = image_advanced._build_execution_plan(routed)

        assert payload["submit_model"] == "fal-ai/flux-2-pro"
        assert payload["arguments"]["guidance_scale"] == 7.5
        assert "resolution" not in payload["arguments"]
        assert payload["ignored_provider_options"] == ["bogus", "resolution"]

    def test_edit_payload_uses_edit_endpoint_and_image_urls(self):
        routed = image_advanced._route_request(
            image_advanced._normalize_request(
                {
                    "prompt": "replace the sky with aurora",
                    "mode": "edit",
                    "input_images": [{"source": "/tmp/photo.png", "role": "base"}],
                    "provider_options": {"resolution": "4K", "bogus": True},
                }
            )
        )

        payload = image_advanced._build_execution_plan(routed)

        assert payload["submit_model"] == "fal-ai/nano-banana-pro/edit"
        assert payload["arguments"]["image_urls"] == ["/tmp/photo.png"]
        assert payload["arguments"]["resolution"] == "4K"
        assert payload["ignored_provider_options"] == ["bogus"]

    def test_compose_payload_uses_multiple_images(self):
        routed = image_advanced._route_request(
            image_advanced._normalize_request(
                {
                    "prompt": "place the mug on the desk",
                    "mode": "compose",
                    "input_images": [
                        {"source": "/tmp/mug.png", "role": "subject"},
                        {"source": "https://example.com/desk.png", "role": "background"},
                    ],
                }
            )
        )

        payload = image_advanced._build_execution_plan(routed)

        assert payload["submit_model"] == "fal-ai/nano-banana-pro/edit"
        assert payload["arguments"]["image_urls"] == ["/tmp/mug.png", "https://example.com/desk.png"]
        assert payload["arguments"]["num_images"] == 1

class TestProviderExecution:
    def test_provider_plan_dispatches_through_registry_provider(self, monkeypatch):
        captured = {}

        class _Provider:
            def generate(self, **kwargs):
                captured.update(kwargs)
                return {
                    "success": True,
                    "image": "/tmp/openai.png",
                    "images": [{"url": "/tmp/openai.png"}],
                }

        monkeypatch.setattr(image_advanced.image_gen_registry, "get_provider", lambda name: _Provider())

        plan = {
            "provider": "openai",
            "submit_model": "gpt-image-2-medium",
            "request": {"prompt": "poster", "aspect_ratio": "portrait"},
            "arguments": {},
        }

        result = image_advanced._execute_provider_plan(plan)

        assert result["success"] is True
        assert captured == {"prompt": "poster", "aspect_ratio": "portrait"}

    def test_provider_plan_temporarily_overrides_model_env(self, monkeypatch):
        monkeypatch.setenv("OPENAI_IMAGE_MODEL", "old-value")
        seen = []

        class _Provider:
            def generate(self, **kwargs):
                import os

                seen.append(os.environ.get("OPENAI_IMAGE_MODEL"))
                return {
                    "success": True,
                    "image": "/tmp/openai.png",
                    "images": [{"url": "/tmp/openai.png"}],
                }

        monkeypatch.setattr(image_advanced.image_gen_registry, "get_provider", lambda name: _Provider())

        plan = {
            "provider": "openai",
            "submit_model": "gpt-image-2-medium",
            "request": {"prompt": "poster", "aspect_ratio": "portrait"},
            "arguments": {},
        }

        image_advanced._execute_provider_plan(plan)

        assert seen == ["gpt-image-2-medium"]
