# image_generate_advanced

Bundled Hermes plugin that exposes `image_generate_advanced` for richer image workflows than the built-in `image_generate` tool.

The plugin is a separate opt-in tool. Enabling it adds `image_generate_advanced`; it does **not** replace or reroute the built-in `image_generate` tool.

## Supported modes

- `generate` — create an image from a text prompt.
- `edit` — edit one input image.
- `compose` — combine two or more input images.

## Supported models

The current implementation supports explicit FAL model IDs only:

| Model | Modes | Notes |
| --- | --- | --- |
| `fal-ai/flux-2-pro` | `generate` | Default high-quality text-to-image model. |
| `fal-ai/flux-2/klein/9b` | `generate` | Balanced profile model. |
| `fal-ai/z-image/turbo` | `generate` | Fast profile model. |
| `fal-ai/ideogram/v3` | `generate` | Typography profile model. |
| `fal-ai/recraft/v4/pro/text-to-image` | `generate` | Design/profile model. |
| `fal-ai/nano-banana-pro` | `generate`, `edit`, `compose` | Edit/compose public model; submitted to FAL edit endpoint for edit/compose. |

Friendly aliases such as `openai-image` or `grok-image` are intentionally **not** part of the current contract. Unknown model names fail validation with `unknown_model`.

## Routing rules

Model selection is deterministic:

1. explicit `model`
2. `style_profile`
3. `quality_profile`
4. mode default

If a profile selects a model that cannot serve the requested mode, the tool falls back to the mode default and reports that fallback in `metadata.fallbacks`.

Mode defaults:

| Mode | Default model |
| --- | --- |
| `generate` | `fal-ai/flux-2-pro` |
| `edit` | `fal-ai/nano-banana-pro` |
| `compose` | `fal-ai/nano-banana-pro` |

Style profiles:

| Profile | Preferred model | Extra options |
| --- | --- | --- |
| `cinematic` | `fal-ai/flux-2-pro` | `guidance_scale: 4.5` |
| `photoreal` | `fal-ai/flux-2-pro` | none |
| `design` | `fal-ai/recraft/v4/pro/text-to-image` | none |
| `typography` | `fal-ai/ideogram/v3` | none |

Quality profiles:

| Profile | Preferred model | Extra options |
| --- | --- | --- |
| `fast` | `fal-ai/z-image/turbo` | `num_inference_steps: 8` |
| `balanced` | `fal-ai/flux-2/klein/9b` | `num_inference_steps: 4` |
| `high` | `fal-ai/flux-2-pro` | `num_inference_steps: 50` |

## Request examples

Generate:

```json
{
  "prompt": "Create a premium coffee poster",
  "mode": "generate",
  "style_profile": "design",
  "quality_profile": "high",
  "aspect_ratio": "portrait",
  "provider_options": {
    "background_color": "#111111"
  }
}
```

Edit:

```json
{
  "prompt": "Replace the background with a neon city",
  "mode": "edit",
  "input_images": [
    {"source": "/absolute/path/to/input.png", "role": "base"}
  ],
  "provider_options": {
    "resolution": "4K"
  }
}
```

Compose:

```json
{
  "prompt": "Place the sneaker on the marble pedestal",
  "mode": "compose",
  "input_images": [
    {"source": "/tmp/shoe.png", "role": "subject"},
    {"source": "https://example.com/pedestal.png", "role": "background"}
  ]
}
```

## Response metadata

Successful responses include:

- `metadata.mode`
- `metadata.effective_model`
- `metadata.submit_model`
- `metadata.selection_source`
- `metadata.ignored_provider_options`
- `metadata.fallbacks`
- `metadata.effective_parameters`

## Credentials

FAL credentials are resolved by the normal Hermes env lookup. Supported variable names are:

- `FAL_KEY`
- `FAL_API_KEY`
- `FAL_KEY_ID`
- `FAL_KEY_SECRET`

Do not document or log credential values.

## Current limits

- Edit/compose route through the Nano Banana edit endpoint.
- `input_images[*].source` must be either an absolute local path or an `http`/`https` URL.
- `preserve_subject` and `preserve_composition` are intentionally rejected for now instead of silently ignored.
- Input image roles are validated and preserved in the request contract, but execution is still order-based rather than role-aware.
- Capability and profile maps are explicit rather than dynamically discovered from providers.
