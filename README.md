# pipecat-sonex

> **SonexLabs TTS** for [pipecat-ai](https://github.com/pipecat-ai/pipecat) real-time voice pipelines.

[![PyPI](https://img.shields.io/pypi/v/pipecat-sonex)](https://pypi.org/project/pipecat-sonex/)
[![Python](https://img.shields.io/pypi/pyversions/pipecat-sonex)](https://pypi.org/project/pipecat-sonex/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

`pipecat-sonex` provides `SonexTTSService` — a pipecat-native TTS service that connects any pipecat pipeline to the [SonexLabs](https://sonexlabs.com) Panini TTS API.

`SonexTTSService` extends pipecat's official `TTSService` base class, so it works exactly like `CartesiaTTSService`, `ElevenLabsTTSService`, and other first-party services:

- The base class handles **sentence aggregation**, **LLM token buffering**, **interruption recovery**, **TTSStartedFrame / TTSStoppedFrame**, and **metrics**.
- You only need to configure credentials and voice.
- Voice and language can be **changed mid-conversation** via `TTSUpdateSettingsFrame`.

---

## Streaming and connection reuse

Requests go to SonexLabs' `/v1/speech/stream` endpoint, so audio is delivered as chunked HTTP as soon as each sentence is generated instead of waiting for the full utterance, reducing time-to-first-audio. The underlying `aiohttp.ClientSession` uses a `TCPConnector` with `keepalive_timeout` set, so connections are pooled and reused across requests rather than reconnecting for every sentence.

---

## Installation

```bash
pip install pipecat-sonex
# or
uv add pipecat-sonex
```

---

## Quick start

### 1. List available voices

```bash
curl https://api.sonexlabs.com/v1/voices \
  -H "Authorization: Bearer $SONEX_API_KEY"
```

Copy a `voice_id` from the response — it is **required**.

### 2. Add to your pipeline

```python
from pipecat_sonex import SonexTTSService

tts = SonexTTSService(
    api_key="vsk_...",
    voice="9b8fsavyez",     # Diya (English), from GET /v1/voices — required, no default
)
```

Language is optional — leave it unset and Panini auto-detects from the input text. For telephony (Twilio/Exotel/Vobiz), pass `sample_rate=8000` — see [Constructor parameters](#constructor-parameters).

Drop it into any pipeline the same way as any other pipecat TTS service:

```python
pipeline = Pipeline([
    transport.input(),
    stt,
    context_aggregator.user(),
    llm,
    tts,                    # ← SonexTTSService here
    transport.output(),
    context_aggregator.assistant(),
])
```

---

## Constructor parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `api_key` | `str` | — | SonexLabs API key (`vsk_...`) |
| `voice` | `str` | — | **Required.** Voice ID from `GET /v1/voices` |
| `language` | `str` | `""` | Optional. Leave unset to auto-detect. |
| `speed` | `float` | `1.0` | Speech rate multiplier (practical range: 0.75–1.5) |
| `sample_rate` | `int` | `24000` | Pipeline output rate in Hz. Not sent to the API — Panini always returns audio at its native rate; pipecat resamples to this value. Use `8000` for Twilio/Exotel telephony. |
| `endpoint` | `str` | `https://api.sonexlabs.com` | API base URL |
| `settings` | `SonexTTSSettings` | `None` | Runtime-updatable settings (takes precedence) |

---

## Runtime settings update

Change voice or language mid-conversation:

```python
from pipecat.frames.frames import TTSUpdateSettingsFrame
from pipecat_sonex import SonexTTSSettings

await task.queue_frames([
    TTSUpdateSettingsFrame(delta=SonexTTSSettings(voice="lkwlapu6ab"))  # Priya, from GET /v1/voices
])
```

---

## Examples

All examples are in the [`examples/`](examples/) directory.  Copy `.env.example` to `.env` and fill in your credentials before running.

### WebRTC + OpenAI LLM

Full browser-to-server voice bot using SmallWebRTC transport:

```bash
pip install "pipecat-ai[openai,deepgram,silero,webrtc]" pipecat-sonex python-dotenv fastapi uvicorn
python examples/webrtc_openai.py
# Open http://localhost:7860 and click Connect
```

### Twilio telephony

```bash
pip install "pipecat-ai[openai,deepgram,silero,twilio]" pipecat-sonex python-dotenv fastapi uvicorn
python examples/telephony_twilio.py
# Expose with ngrok, point Twilio webhook to https://<host>/incoming-call
```

### Exotel telephony

```bash
pip install "pipecat-ai[openai,deepgram,silero]" pipecat-sonex python-dotenv fastapi uvicorn
python examples/telephony_exotel.py
# Expose with ngrok, set Exotel Passthru URL to wss://<host>/ws/exotel
```

### Vobiz telephony

`VobizFrameSerializer` is bundled in `pipecat_sonex.vobiz` — no extra package needed:

```bash
pip install "pipecat-ai[openai,deepgram,silero]" pipecat-sonex python-dotenv fastapi uvicorn
python examples/telephony_vobiz.py
# Set Vobiz WebSocket URL to wss://<host>/ws/vobiz
```

### LiveKit

For a LiveKit room-based voice bot example, see the separate [`pipecat-sonex-livekit`](https://github.com/sonexlabs/pipecat-sonex-livekit) repo.

---

## API reference

See the full API documentation at [docs.sonexlabs.com/api-reference](https://docs.sonexlabs.com/api-reference).

---

## License

MIT — see [LICENSE](LICENSE).