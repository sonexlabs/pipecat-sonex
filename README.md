# pipecat-sonex

> **SonexLabs Panini TTS** processor for [pipecat-ai](https://github.com/pipecat-ai/pipecat) real-time voice pipelines.

[![PyPI](https://img.shields.io/pypi/v/pipecat-sonex)](https://pypi.org/project/pipecat-sonex/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## What is this?

`pipecat-sonex` adds a `PaniniStreamingTTSProcessor` that connects any pipecat pipeline to the SonexLabs Panini TTS GPU cluster.

**Key features:**

- **Streaming with sentence splitting** — synthesis starts before the LLM finishes generating, reducing time-to-first-audio dramatically.
- **TTFB-gated inference lock** — the GPU is released as soon as inference finishes (on first response byte), so the next sentence begins GPU inference while the current WAV is still downloading. Saves ~300-680 ms per sentence on multi-sentence responses.
- **Endpoint rotation** — pass multiple Panini cluster URLs for automatic load distribution.
- **Telephony-ready** — set `sample_rate=8000` for Twilio/Exotel PCMU transports.

---

## Installation

```bash
pip install pipecat-sonex
# or
uv add pipecat-sonex
```

---

## Quick start

### WebRTC + OpenAI

```python
import asyncio
import os
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineTask
from pipecat.services.openai import OpenAILLMService
from pipecat.transports.network.small_webrtc import SmallWebRTCTransport
from pipecat_sonex import PaniniStreamingTTSProcessor

PANINI_ENDPOINTS = os.environ["PANINI_TTS_ENDPOINTS"].split(",")  # e.g. "http://host:8000"
SONEX_API_TOKEN  = os.environ["SONEX_API_TOKEN"]                  # vsk_...

async def main():
    transport = SmallWebRTCTransport(...)

    tts = PaniniStreamingTTSProcessor(
        api_token=SONEX_API_TOKEN,
        model="panini",
        voice="your-voice-id",   # from SonexLabs dashboard, or "auto"
        language="en",
        num_step=16,
        speed=1.0,
        sample_rate=24000,
        endpoints=PANINI_ENDPOINTS,
    )

    llm = OpenAILLMService(api_key=os.environ["OPENAI_API_KEY"], model="gpt-4o-mini")

    pipeline = Pipeline([
        transport.input(),
        llm,
        tts,
        transport.output(),
    ])

    runner = PipelineRunner()
    task = PipelineTask(pipeline)
    await runner.run(task)

asyncio.run(main())
```

### Telephony (Twilio / Exotel)

```python
tts = PaniniStreamingTTSProcessor(
    api_token=SONEX_API_TOKEN,
    model="panini",
    voice="your-voice-id",
    language="en",
    num_step=16,
    speed=1.0,
    sample_rate=8000,   # PCMU for telephony transports
    endpoints=PANINI_ENDPOINTS,
)
```

---

## Configuration reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `api_token` | `str` | *required* | SonexLabs API token (`vsk_...`) |
| `model` | `str` | `"panini"` | Panini model name |
| `voice` | `str` | `"auto"` | Voice ID from the SonexLabs dashboard |
| `language` | `str` | `""` | BCP-47 language tag (`"en"`, `"hi"`, `"te"`, …) |
| `num_step` | `int` | `16` | Diffusion sampling steps (higher = better quality, slower) |
| `speed` | `float` | `1.0` | Speech rate multiplier |
| `sample_rate` | `int` | `24000` | Output PCM sample rate in Hz |
| `endpoints` | `List[str]` | *required* | One or more Panini cluster URLs |

---

## Environment variables (recommended pattern)

```bash
SONEX_API_TOKEN=vsk_...
PANINI_TTS_ENDPOINTS=http://157.15.202.177:8000
```

---

## Links

- [SonexLabs docs](https://docs.sonexlabs.com)
- [API reference](https://docs.sonexlabs.com/api-reference)
- [pipecat-ai](https://github.com/pipecat-ai/pipecat)

---

## License

MIT — see [LICENSE](LICENSE).
