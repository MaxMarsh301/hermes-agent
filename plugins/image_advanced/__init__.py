"""Advanced image workflow plugin.

Stage 3/4 implementation adds:
- deterministic model routing
- provider-safe payload builders
- execution via the existing FAL helpers
- structured metadata with effective parameters and fallback notes
"""

from __future__ import annotations

import base64
from contextlib import contextmanager
import json
import mimetypes
import os
from pathlib import Path
from typing import Any, Dict, Iterable
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from agent import image_gen_registry
from agent.image_gen_provider import DEFAULT_ASPECT_RATIO, VALID_ASPECT_RATIOS, resolve_aspect_ratio
from tools.image_generation_tool import FAL_MODELS, _build_fal_payload, _submit_fal_request, check_fal_api_key

TOOL_NAME = "image_generate_advanced"
TOOLSET = "image_gen"
VALID_MODES = ("generate", "edit", "compose")
VALID_OUTPUT_FORMATS = ("png", "jpg", "webp")
VALID_IMAGE_ROLES = {
    "base",
    "reference",
    "subject",
    "background",
    "style",
    "overlay",
}

PROVIDER_REQUIRED_ENV_VARS: Dict[str, tuple[str, ...]] = {
    "openai": ("OPENAI_API_KEY",),
    "xai": ("XAI_API_KEY", "XAI_BASE_URL"),
}

PROVIDER_MODEL_ENV_OVERRIDES: Dict[str, str] = {
    "openai": "OPENAI_IMAGE_MODEL",
    "xai": "XAI_IMAGE_MODEL",
}

PROVIDER_GENERATE_SUPPORTS: Dict[str, set[str]] = {
    "openai": set(),
    "xai": set(),
}

STYLE_PROFILE_MAP: Dict[str, Dict[str, Any]] = {
    "cinematic": {
        "preferred_model": "fal-ai/flux-2-pro",
        "provider_options": {"guidance_scale": 4.5},
        "notes": "Photoreal, dramatic lighting, movie-poster leaning output.",
    },
    "photoreal": {
        "preferred_model": "fal-ai/flux-2-pro",
        "provider_options": {},
        "notes": "High-fidelity natural imagery.",
    },
    "design": {
        "preferred_model": "fal-ai/recraft/v4/pro/text-to-image",
        "provider_options": {},
        "notes": "Brand, poster, and product-design oriented output.",
    },
    "typography": {
        "preferred_model": "fal-ai/ideogram/v3",
        "provider_options": {},
        "notes": "Text-forward imagery and title cards.",
    },
}

QUALITY_PROFILE_MAP: Dict[str, Dict[str, Any]] = {
    "fast": {
        "preferred_model": "fal-ai/z-image/turbo",
        "provider_options": {"num_inference_steps": 8},
        "notes": "Lowest latency, rougher output acceptable.",
    },
    "balanced": {
        "preferred_model": "fal-ai/flux-2/klein/9b",
        "provider_options": {"num_inference_steps": 4},
        "notes": "Reasonable compromise between cost, speed, and quality.",
    },
    "high": {
        "preferred_model": "fal-ai/flux-2-pro",
        "provider_options": {"num_inference_steps": 50},
        "notes": "Prioritize output quality over latency.",
    },
}

MODEL_CAPABILITIES: Dict[str, Dict[str, Any]] = {
    "fal-ai/z-image/turbo": {
        "provider": "fal",
        "modes": {"generate"},
    },
    "fal-ai/flux-2/klein/9b": {
        "provider": "fal",
        "modes": {"generate"},
    },
    "fal-ai/flux-2-pro": {
        "provider": "fal",
        "modes": {"generate"},
    },
    "fal-ai/ideogram/v3": {
        "provider": "fal",
        "modes": {"generate"},
    },
    "fal-ai/recraft/v4/pro/text-to-image": {
        "provider": "fal",
        "modes": {"generate"},
    },
    "fal-ai/nano-banana-pro": {
        "provider": "fal",
        "modes": {"generate", "edit", "compose"},
        "edit_model": "fal-ai/nano-banana-pro/edit",
        "compose_model": "fal-ai/nano-banana-pro/edit",
    },
}

MODE_DEFAULT_MODEL: Dict[str, str] = {
    "generate": "fal-ai/flux-2-pro",
    "edit": "fal-ai/nano-banana-pro",
    "compose": "fal-ai/nano-banana-pro",
}

ADVANCED_EDIT_SUPPORTS = {
    "prompt",
    "image_urls",
    "num_images",
    "aspect_ratio",
    "output_format",
    "safety_tolerance",
    "seed",
    "sync_mode",
    "resolution",
    "limit_generations",
}

IMAGE_GENERATE_ADVANCED_SCHEMA: Dict[str, Any] = {
    "name": TOOL_NAME,
    "description": (
        "Advanced image workflow tool for text-to-image generation, editing, and "
        "multi-image composition. Supports explicit model selection, style and "
        "quality profiles, and returns effective generation parameters with the result."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "mode": {
                "type": "string",
                "enum": list(VALID_MODES),
                "description": "Requested workflow mode.",
            },
            "prompt": {
                "type": "string",
                "description": "Primary generation or editing instruction.",
            },
            "aspect_ratio": {
                "type": "string",
                "enum": list(VALID_ASPECT_RATIOS),
                "description": "Requested output aspect ratio.",
            },
            "style_profile": {
                "type": "string",
                "description": "High-level style hint such as cinematic, photoreal, design, or typography.",
            },
            "quality_profile": {
                "type": "string",
                "description": "High-level speed/quality hint such as fast, balanced, or high.",
            },
            "model": {
                "type": "string",
                "description": "Explicit model identifier. Takes priority over profiles.",
            },
            "seed": {
                "type": "integer",
                "description": "Optional deterministic seed.",
            },
            "num_images": {
                "type": "integer",
                "minimum": 1,
                "maximum": 4,
                "description": "Number of images requested.",
            },
            "output_format": {
                "type": "string",
                "enum": list(VALID_OUTPUT_FORMATS),
                "description": "Requested image output format.",
            },
            "input_images": {
                "type": "array",
                "description": "Input images for edit or compose modes.",
                "items": {
                    "type": "object",
                    "properties": {
                        "source": {
                            "type": "string",
                            "description": "Absolute path or HTTP(S) URL to an input image.",
                        },
                        "role": {
                            "type": "string",
                            "description": "Optional semantic role such as base, subject, style, or background.",
                        },
                    },
                    "required": ["source"],
                },
            },
            "preserve_subject": {
                "type": "boolean",
                "description": "Whether to preserve the main subject where supported.",
            },
            "preserve_composition": {
                "type": "boolean",
                "description": "Whether to preserve the original composition where supported.",
            },
            "provider_options": {
                "type": "object",
                "description": "Provider- or model-specific overrides.",
            },
        },
        "required": ["prompt"],
    },
}


def _tool_available() -> bool:
    _ensure_fal_credentials_loaded()
    return check_fal_api_key()


def _ensure_fal_credentials_loaded() -> None:
    """Mirror the built-in image tool's credential lookup for direct FAL usage.

    Hermes often stores FAL credentials in ``~/.hermes/.env`` without exporting
    them into the parent shell. The bundled ``image_generate`` path resolves
    those values via ``hermes_cli.config.get_env_value``; this plugin should do
    the same before calling ``fal_client`` through the shared submit helper.
    """
    keys = ("FAL_KEY", "FAL_API_KEY", "FAL_KEY_ID", "FAL_KEY_SECRET")
    missing_keys = [key for key in keys if not (os.getenv(key) or "").strip()]
    if not missing_keys:
        return

    try:
        from hermes_cli.config import get_env_value
    except Exception:
        return

    for key in missing_keys:
        value = get_env_value(key)
        if isinstance(value, str) and value.strip():
            os.environ[key] = value.strip()


def _ensure_provider_credentials_loaded(provider_name: str) -> None:
    keys = PROVIDER_REQUIRED_ENV_VARS.get(provider_name, ())
    if not keys:
        return

    missing_keys = [key for key in keys if not (os.getenv(key) or "").strip()]
    if not missing_keys:
        return

    try:
        from hermes_cli.config import get_env_value
    except Exception:
        return

    for key in missing_keys:
        value = get_env_value(key)
        if isinstance(value, str) and value.strip():
            os.environ[key] = value.strip()


@contextmanager
def _temporary_env_override(key: str | None, value: str | None):
    if not key:
        yield
        return

    previous = os.environ.get(key)
    had_previous = key in os.environ
    if value is None:
        os.environ.pop(key, None)
    else:
        os.environ[key] = value

    try:
        yield
    finally:
        if had_previous:
            os.environ[key] = previous or ""
        else:
            os.environ.pop(key, None)


def _clean_string(value: Any, *, lowercase: bool = False) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    return cleaned.lower() if lowercase else cleaned


def _normalize_input_images(value: Any) -> list[Dict[str, str]]:
    if not isinstance(value, list):
        return []

    normalized: list[Dict[str, str]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        source = _clean_string(item.get("source"))
        if not source:
            continue
        normalized_item: Dict[str, str] = {"source": source}
        role = _clean_string(item.get("role"), lowercase=True)
        if role:
            normalized_item["role"] = role
        normalized.append(normalized_item)
    return normalized


def _normalize_request(args: Dict[str, Any]) -> Dict[str, Any]:
    normalized: Dict[str, Any] = {
        "mode": _clean_string(args.get("mode"), lowercase=True) or "generate",
        "prompt": _clean_string(args.get("prompt")) or "",
        "aspect_ratio": resolve_aspect_ratio(args.get("aspect_ratio")),
        "style_profile": _clean_string(args.get("style_profile"), lowercase=True),
        "quality_profile": _clean_string(args.get("quality_profile"), lowercase=True),
        "model": _clean_string(args.get("model")),
        "seed": args.get("seed"),
        "num_images": args.get("num_images", 1),
        "output_format": _clean_string(args.get("output_format"), lowercase=True) or "png",
        "input_images": _normalize_input_images(args.get("input_images")),
        "preserve_subject": bool(args.get("preserve_subject", False)),
        "preserve_composition": bool(args.get("preserve_composition", False)),
        "provider_options": args.get("provider_options") if isinstance(args.get("provider_options"), dict) else {},
    }
    return normalized


def _validation_result(*, code: str, field: str, message: str, details: Dict[str, Any] | None = None) -> Dict[str, Any]:
    payload = {
        "ok": False,
        "code": code,
        "field": field,
        "message": message,
    }
    if details:
        payload["details"] = details
    return payload


def _validate_profiles(request: Dict[str, Any]) -> Dict[str, Any]:
    style_profile = request.get("style_profile")
    if style_profile and style_profile not in STYLE_PROFILE_MAP:
        return _validation_result(
            code="unknown_style_profile",
            field="style_profile",
            message=(
                f"Unsupported style_profile '{style_profile}'. "
                f"Supported values: {', '.join(sorted(STYLE_PROFILE_MAP))}."
            ),
        )

    quality_profile = request.get("quality_profile")
    if quality_profile and quality_profile not in QUALITY_PROFILE_MAP:
        return _validation_result(
            code="unknown_quality_profile",
            field="quality_profile",
            message=(
                f"Unsupported quality_profile '{quality_profile}'. "
                f"Supported values: {', '.join(sorted(QUALITY_PROFILE_MAP))}."
            ),
        )

    return {"ok": True}


def _validate_input_images(mode: str, input_images: list[Dict[str, str]]) -> Dict[str, Any]:
    if mode == "edit" and not input_images:
        return _validation_result(
            code="input_images_required",
            field="input_images",
            message="edit mode requires at least one input image.",
        )
    if mode == "compose" and len(input_images) < 2:
        return _validation_result(
            code="compose_requires_multiple_images",
            field="input_images",
            message="compose mode requires at least two input images.",
        )

    for index, image in enumerate(input_images):
        source = image.get("source", "")
        parsed = urlparse(source)
        is_http_url = parsed.scheme in {"http", "https"} and bool(parsed.netloc)
        is_absolute_path = Path(source).is_absolute()
        if not (is_http_url or is_absolute_path):
            return _validation_result(
                code="invalid_input_image_source",
                field=f"input_images[{index}].source",
                message="input image sources must be absolute local paths or http(s) URLs.",
            )

        role = image.get("role")
        if role and role not in VALID_IMAGE_ROLES:
            return _validation_result(
                code="invalid_input_image_role",
                field=f"input_images[{index}].role",
                message=(
                    f"Unsupported role '{role}'. Supported roles: "
                    f"{', '.join(sorted(VALID_IMAGE_ROLES))}."
                ),
            )

    return {"ok": True}


def _validate_model_capabilities(request: Dict[str, Any]) -> Dict[str, Any]:
    model = request.get("model")
    mode = request["mode"]
    if not model:
        return {"ok": True}

    caps = MODEL_CAPABILITIES.get(model)
    if caps is None:
        return _validation_result(
            code="unknown_model",
            field="model",
            message=(
                f"Unknown model '{model}' for image_generate_advanced. "
                f"Known models: {', '.join(sorted(MODEL_CAPABILITIES))}."
            ),
        )

    supported_modes: Iterable[str] = caps.get("modes", set())
    if mode not in supported_modes:
        return _validation_result(
            code="unsupported_mode_for_model",
            field="model",
            message=f"Model '{model}' does not support mode '{mode}'.",
            details={"supported_modes": sorted(supported_modes)},
        )

    return {"ok": True}


def _validate_request(request: Dict[str, Any]) -> Dict[str, Any]:
    mode = request.get("mode")
    if mode not in VALID_MODES:
        return _validation_result(
            code="invalid_mode",
            field="mode",
            message=f"mode must be one of: {', '.join(VALID_MODES)}.",
        )

    if not request.get("prompt"):
        return _validation_result(
            code="prompt_required",
            field="prompt",
            message="prompt is required and must be a non-empty string.",
        )

    num_images = request.get("num_images")
    if not isinstance(num_images, int) or not (1 <= num_images <= 4):
        return _validation_result(
            code="invalid_num_images",
            field="num_images",
            message="num_images must be an integer between 1 and 4.",
        )

    output_format = request.get("output_format")
    if output_format not in VALID_OUTPUT_FORMATS:
        return _validation_result(
            code="invalid_output_format",
            field="output_format",
            message=f"output_format must be one of: {', '.join(VALID_OUTPUT_FORMATS)}.",
        )

    if request.get("seed") is not None and not isinstance(request.get("seed"), int):
        return _validation_result(
            code="invalid_seed",
            field="seed",
            message="seed must be an integer when provided.",
        )

    if request.get("preserve_subject"):
        return _validation_result(
            code="unsupported_control",
            field="preserve_subject",
            message="preserve_subject is not implemented for image_generate_advanced yet.",
        )

    if request.get("preserve_composition"):
        return _validation_result(
            code="unsupported_control",
            field="preserve_composition",
            message="preserve_composition is not implemented for image_generate_advanced yet.",
        )

    profiles_result = _validate_profiles(request)
    if not profiles_result["ok"]:
        return profiles_result

    input_images_result = _validate_input_images(request["mode"], request["input_images"])
    if not input_images_result["ok"]:
        return input_images_result

    model_result = _validate_model_capabilities(request)
    if not model_result["ok"]:
        return model_result

    return {"ok": True}


def _model_supports_mode(model: str, mode: str) -> bool:
    caps = MODEL_CAPABILITIES.get(model, {})
    return mode in caps.get("modes", set())


def _merge_profile_options(request: Dict[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    style_profile = request.get("style_profile")
    if style_profile:
        merged.update(STYLE_PROFILE_MAP.get(style_profile, {}).get("provider_options", {}))
    quality_profile = request.get("quality_profile")
    if quality_profile:
        merged.update(QUALITY_PROFILE_MAP.get(quality_profile, {}).get("provider_options", {}))
    merged.update(request.get("provider_options", {}))
    return merged


def _route_request(request: Dict[str, Any]) -> Dict[str, Any]:
    mode = request["mode"]
    fallbacks: list[Dict[str, Any]] = []
    effective_model = request.get("model")
    selection_source = "explicit_model" if effective_model else None

    if not effective_model:
        style_profile = request.get("style_profile")
        if style_profile:
            style_model = STYLE_PROFILE_MAP[style_profile]["preferred_model"]
            if _model_supports_mode(style_model, mode):
                effective_model = style_model
                selection_source = "style_profile"
            else:
                fallbacks.append(
                    {
                        "source": "style_profile",
                        "requested_model": style_model,
                        "reason": "unsupported_mode_for_profile_model",
                    }
                )

    if not effective_model:
        quality_profile = request.get("quality_profile")
        if quality_profile:
            quality_model = QUALITY_PROFILE_MAP[quality_profile]["preferred_model"]
            if _model_supports_mode(quality_model, mode):
                effective_model = quality_model
                selection_source = "quality_profile"
            else:
                fallbacks.append(
                    {
                        "source": "quality_profile",
                        "requested_model": quality_model,
                        "reason": "unsupported_mode_for_profile_model",
                    }
                )

    if not effective_model:
        effective_model = MODE_DEFAULT_MODEL[mode]
        selection_source = "mode_default"

    caps = MODEL_CAPABILITIES[effective_model]
    return {
        "request": request,
        "mode": mode,
        "effective_model": effective_model,
        "selection_source": selection_source,
        "provider": caps.get("provider", "fal"),
        "capabilities": caps,
        "provider_options": _merge_profile_options(request),
        "fallbacks": fallbacks,
    }


def _filter_supported_options(options: Dict[str, Any], supported: set[str]) -> tuple[Dict[str, Any], list[str]]:
    filtered: Dict[str, Any] = {}
    ignored: list[str] = []
    for key in sorted(options):
        value = options[key]
        if value is None:
            continue
        if key in supported:
            filtered[key] = value
        else:
            ignored.append(key)
    return filtered, ignored


def _build_generate_plan(route: Dict[str, Any]) -> Dict[str, Any]:
    if route["provider"] != "fal":
        return _build_provider_generate_plan(route)

    request = route["request"]
    model = route["capabilities"].get("fal_model", route["effective_model"])
    supports = set(FAL_MODELS[model]["supports"])

    universal_overrides: Dict[str, Any] = {}
    if request["num_images"] != 1 and "num_images" in supports:
        universal_overrides["num_images"] = request["num_images"]
    if request["output_format"] != "png" and "output_format" in supports:
        universal_overrides["output_format"] = request["output_format"]

    filtered_provider_options, ignored = _filter_supported_options(route["provider_options"], supports)
    arguments = _build_fal_payload(
        model,
        request["prompt"],
        request["aspect_ratio"],
        seed=request.get("seed"),
        overrides={**universal_overrides, **filtered_provider_options},
    )
    return {
        "submit_model": model,
        "arguments": arguments,
        "ignored_provider_options": ignored,
    }


def _build_provider_generate_plan(route: Dict[str, Any]) -> Dict[str, Any]:
    request = route["request"]
    provider_name = route["provider"]
    supports = PROVIDER_GENERATE_SUPPORTS.get(provider_name, set())

    arguments: Dict[str, Any] = {}
    filtered_provider_options, ignored = _filter_supported_options(route["provider_options"], supports)
    arguments.update(filtered_provider_options)

    if request.get("seed") is not None:
        if "seed" in supports:
            arguments["seed"] = request["seed"]
        else:
            ignored.append("seed")
    if request["num_images"] != 1:
        if "num_images" in supports:
            arguments["num_images"] = request["num_images"]
        else:
            ignored.append("num_images")
    if request["output_format"] != "png":
        if "output_format" in supports:
            arguments["output_format"] = request["output_format"]
        else:
            ignored.append("output_format")

    return {
        "submit_model": route["capabilities"].get("provider_model", route["effective_model"]),
        "arguments": arguments,
        "ignored_provider_options": sorted(set(ignored)),
    }


def _build_edit_like_plan(route: Dict[str, Any]) -> Dict[str, Any]:
    request = route["request"]
    caps = route["capabilities"]
    submit_model = caps.get(f"{route['mode']}_model", route["effective_model"])
    base_model_meta = FAL_MODELS[route["effective_model"]]

    candidate_arguments: Dict[str, Any] = {
        **base_model_meta.get("defaults", {}),
        "prompt": request["prompt"],
        "image_urls": [img["source"] for img in request["input_images"]],
        "num_images": request["num_images"],
        "aspect_ratio": base_model_meta["sizes"][request["aspect_ratio"]],
        "output_format": request["output_format"],
        "limit_generations": True,
        **route["provider_options"],
    }
    if request.get("seed") is not None:
        candidate_arguments["seed"] = request["seed"]

    arguments, ignored = _filter_supported_options(candidate_arguments, ADVANCED_EDIT_SUPPORTS)
    return {
        "submit_model": submit_model,
        "arguments": arguments,
        "ignored_provider_options": ignored,
    }


def _build_execution_plan(route: Dict[str, Any]) -> Dict[str, Any]:
    if route["mode"] == "generate":
        plan = _build_generate_plan(route)
    else:
        plan = _build_edit_like_plan(route)

    plan.update(
        {
            "mode": route["mode"],
            "effective_model": route["effective_model"],
            "selection_source": route["selection_source"],
            "fallbacks": route["fallbacks"],
            "provider": route["provider"],
            "request": route["request"],
        }
    )
    return plan


def _extract_images(result: Dict[str, Any]) -> list[Dict[str, Any]]:
    images = result.get("images") if isinstance(result, dict) else None
    if isinstance(images, list):
        return [img for img in images if isinstance(img, dict) and img.get("url")]
    image = result.get("image") if isinstance(result, dict) else None
    if isinstance(image, dict) and image.get("url"):
        return [image]
    return []


def _sanitize_backend_result(result: Any) -> Any:
    if result is None or isinstance(result, (str, int, float, bool)):
        return result
    if isinstance(result, dict):
        return {str(key): _sanitize_backend_result(value) for key, value in result.items()}
    if isinstance(result, list):
        return [_sanitize_backend_result(item) for item in result]
    if isinstance(result, tuple):
        return [_sanitize_backend_result(item) for item in result]
    return repr(result)


def _error_response(
    *,
    error: str,
    error_type: str,
    received: Dict[str, Any],
    normalized_request: Dict[str, Any],
    plan: Dict[str, Any] | None = None,
    provider_response: Any = None,
) -> str:
    payload: Dict[str, Any] = {
        "success": False,
        "error": error,
        "error_type": error_type,
        "received": received,
        "normalized_request": normalized_request,
    }

    if plan is not None:
        payload["execution"] = {
            "mode": plan.get("mode"),
            "provider": plan.get("provider"),
            "effective_model": plan.get("effective_model"),
            "submit_model": plan.get("submit_model"),
            "selection_source": plan.get("selection_source"),
            "ignored_provider_options": plan.get("ignored_provider_options"),
            "fallbacks": plan.get("fallbacks"),
        }

    if provider_response is not None:
        payload["provider_response"] = _sanitize_backend_result(provider_response)

    return json.dumps(payload)


def _provider_result_to_error_payload(
    *,
    result: Any,
    args: Dict[str, Any],
    normalized: Dict[str, Any],
    plan: Dict[str, Any],
) -> str | None:
    if not isinstance(result, dict):
        return None

    if result.get("success") is False:
        return _error_response(
            error=str(result.get("error") or "Image provider returned an error response"),
            error_type=str(result.get("error_type") or "provider_error"),
            received=args,
            normalized_request=normalized,
            plan=plan,
            provider_response=result,
        )

    return None


def _source_to_data_uri(source: str) -> str:
    parsed = urlparse(source)
    if parsed.scheme in {"http", "https"}:
        request = Request(source, headers={"User-Agent": "Hermes image_generate_advanced/1.0"})
        with urlopen(request, timeout=30) as response:
            payload = response.read()
            header_mime = response.headers.get_content_type()
        mime_type = header_mime if isinstance(header_mime, str) and header_mime.startswith("image/") else None
    else:
        payload = Path(source).read_bytes()
        guessed_mime, _ = mimetypes.guess_type(source)
        mime_type = guessed_mime if isinstance(guessed_mime, str) and guessed_mime.startswith("image/") else None

    if not mime_type:
        mime_type = "image/png"
    encoded = base64.b64encode(payload).decode("ascii")
    return f"data:{mime_type};base64,{encoded}"


def _materialize_plan_inputs(plan: Dict[str, Any]) -> Dict[str, Any]:
    if plan["mode"] == "generate":
        return plan

    arguments = dict(plan["arguments"])
    raw_sources = arguments.get("image_urls", [])
    arguments["image_urls"] = [_source_to_data_uri(source) for source in raw_sources]
    updated = dict(plan)
    updated["arguments"] = arguments
    return updated


def _execute_provider_plan(plan: Dict[str, Any]) -> Dict[str, Any]:
    provider_name = plan["provider"]
    _ensure_provider_credentials_loaded(provider_name)
    provider = image_gen_registry.get_provider(provider_name)
    if provider is None:
        raise ValueError(
            f"Image provider '{provider_name}' is not registered. "
            "Enable the matching bundled image backend and start a fresh session."
        )

    env_key = PROVIDER_MODEL_ENV_OVERRIDES.get(provider_name)
    with _temporary_env_override(env_key, plan.get("submit_model")):
        return provider.generate(
            prompt=plan["request"]["prompt"],
            aspect_ratio=plan["request"]["aspect_ratio"],
            **plan["arguments"],
        )


def _success_response(plan: Dict[str, Any], images: list[Dict[str, Any]]) -> str:
    request = plan["request"]
    metadata = {
        "mode": plan["mode"],
        "provider": plan["provider"],
        "effective_model": plan["effective_model"],
        "submit_model": plan["submit_model"],
        "selection_source": plan["selection_source"],
        "ignored_provider_options": plan["ignored_provider_options"],
        "fallbacks": plan["fallbacks"],
        "effective_parameters": {
            "aspect_ratio": request["aspect_ratio"],
            "output_format": plan["arguments"].get("output_format", request["output_format"]),
            "num_images": plan["arguments"].get("num_images", request["num_images"]),
            "seed": request.get("seed"),
            "style_profile": request.get("style_profile"),
            "quality_profile": request.get("quality_profile"),
            "input_image_count": len(request.get("input_images", [])),
        },
    }
    return json.dumps(
        {
            "success": True,
            "image": images[0]["url"],
            "images": images,
            "metadata": metadata,
        }
    )


def _handle_image_generate_advanced(args: Dict[str, Any], **_: Any) -> str:
    normalized = _normalize_request(args)
    validation = _validate_request(normalized)
    if not validation["ok"]:
        return json.dumps(
            {
                "success": False,
                "error": validation["message"],
                "error_type": "validation_error",
                "validation": validation,
                "received": normalized,
            }
        )

    try:
        route = _route_request(normalized)
        plan = _build_execution_plan(route)
        executable_plan = _materialize_plan_inputs(plan)
        if executable_plan["provider"] == "fal":
            _ensure_fal_credentials_loaded()
            result = _submit_fal_request(executable_plan["submit_model"], arguments=executable_plan["arguments"]).get()
        else:
            result = _execute_provider_plan(executable_plan)
        provider_error = _provider_result_to_error_payload(
            result=result,
            args=args,
            normalized=normalized,
            plan=executable_plan,
        )
        if provider_error is not None:
            return provider_error
        images = _extract_images(result)
        if not images:
            return _error_response(
                error="Invalid response from image backend — no images returned",
                error_type="empty_response",
                received=args,
                normalized_request=normalized,
                plan=executable_plan,
                provider_response=result,
            )
        return _success_response(executable_plan, images)
    except Exception as exc:
        return _error_response(
            error=str(exc),
            error_type=type(exc).__name__,
            received=args,
            normalized_request=normalized,
        )


def register(ctx) -> None:
    ctx.register_tool(
        name=TOOL_NAME,
        toolset=TOOLSET,
        schema=IMAGE_GENERATE_ADVANCED_SCHEMA,
        handler=_handle_image_generate_advanced,
        check_fn=_tool_available,
        description="Advanced image workflow tool with generate/edit/compose modes.",
        emoji="🧪",
    )
