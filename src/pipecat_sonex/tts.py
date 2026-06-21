"""
SonexTTSService
===============
A pipecat-ai ``TTSService`` that synthesises speech via the SonexLabs Panini
TTS API (``POST /v1/audio/speech``).

Extends the official ``TTSService`` base class so it plugs into any pipecat
pipeline exactly like CartesiaTTSService, ElevenLabsTTSService, etc.  The
base class handles sentence aggregation, LLM token buffering, interruption
recovery, metrics and TTSStartedFrame / TTSStoppedFrame bookkeeping — this
class only needs to implement ``run_tts``.

Runtime settings (voice, language, speed) can be changed mid-conversation via
``TTSUpdateSettingsFrame(delta=SonexTTSService.Settings(voice="new-voice-id"))``.
"""

from __future__ import annotations

import re
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from typing import Optional

import aiohttp
from loguru import logger

from pipecat.frames.frames import ErrorFrame, Frame, TTSAudioRawFrame
from pipecat.services.settings import NOT_GIVEN, TTSSettings, _NotGiven
from pipecat.services.tts_service import TTSService

try:
    from pipecat.utils.tracing.service_decorators import traced_tts
except ImportError:
    def traced_tts(fn):  # type: ignore[misc]
        return fn


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


@dataclass
class SonexTTSSettings(TTSSettings):
    """Runtime-updatable settings for ``SonexTTSService``.

    All fields default to ``NOT_GIVEN`` so partial deltas only override the
    fields explicitly set by the caller.

    Parameters
    ----------
    voice:
        Voice ID from the SonexLabs voice library, e.g. ``"en-US-male-1"``.
        Required — there is no default voice.
    language:
        BCP-47 language tag, e.g. ``"en"``, ``"hi"``, ``"te"``.  Omit to
        let Panini auto-detect from the input text.
    speed:
        Speech rate multiplier.  ``1.0`` is normal speed; ``0.75``–``1.5``
        are practical limits.
    """

    speed: float | None | _NotGiven = field(default_factory=lambda: NOT_GIVEN)


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class SonexTTSService(TTSService):
    """SonexLabs Panini TTS service for pipecat pipelines.

    Sends synthesised speech requests to ``POST /v1/audio/speech`` on the
    SonexLabs API, streams back the WAV response, strips the WAV header, and
    yields ``TTSAudioRawFrame`` objects for pipecat's audio pipeline.

    Parameters
    ----------
    api_key:
        SonexLabs API key (``vsk_...``).
    voice:
        Voice ID from the SonexLabs voice library.  **Required** — obtain a
        valid ID from ``GET /v1/voices`` or the SonexLabs dashboard.
    language:
        BCP-47 language tag (e.g. ``"en"``, ``"hi"``).  Optional.
    speed:
        Speech rate multiplier.  Defaults to ``1.0``.
    sample_rate:
        PCM sample rate in Hz for the pipecat pipeline.  Defaults to
        ``24000`` (WebRTC).  Use ``8000`` for Twilio / Exotel telephony.
    endpoint:
        SonexLabs API base URL.  Defaults to ``https://api.sonexlabs.com``.
    settings:
        Optional ``SonexTTSSettings`` instance for runtime-updatable
        overrides.  Takes precedence over individual keyword arguments.

    Example
    -------
    ::

        tts = SonexTTSService(
            api_key=os.environ["SONEX_API_KEY"],
            voice="en-US-male-1",
            language="en",
            sample_rate=8000,   # for Twilio telephony
        )
    """

    Settings = SonexTTSSettings
    _settings: SonexTTSSettings

    _DEFAULT_ENDPOINT = "https://api.sonexlabs.com"

    def __init__(
        self,
        *,
        api_key: str,
        voice: str,
        language: str = "",
        speed: float = 1.0,
        sample_rate: int = 24000,
        endpoint: str = _DEFAULT_ENDPOINT,
        settings: Optional[SonexTTSSettings] = None,
        **kwargs,
    ):
        default_settings = self.Settings(
            voice=voice,
            language=language or None,
            speed=speed,
        )
        if settings is not None:
            default_settings.apply_update(settings)

        super().__init__(
            sample_rate=sample_rate,
            push_start_frame=True,
            push_stop_frames=True,
            push_text_frames=True,
            settings=default_settings,
            **kwargs,
        )

        self._api_key = api_key
        self._endpoint = endpoint.rstrip("/")
        self._http_session: Optional[aiohttp.ClientSession] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def can_generate_metrics(self) -> bool:
        return True

    async def start(self, frame):
        await super().start(frame)
        connector = aiohttp.TCPConnector(
            limit=16,
            ttl_dns_cache=300,
            keepalive_timeout=30,
        )
        self._http_session = aiohttp.ClientSession(connector=connector)

    async def stop(self, frame):
        await super().stop(frame)
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()
            self._http_session = None

    async def cancel(self, frame):
        await super().cancel(frame)

    # ------------------------------------------------------------------
    # Text helpers
    # ------------------------------------------------------------------

    def _sanitize(self, text: str) -> str:
        """Strip markdown characters that Panini would speak literally."""
        cleaned = (text or "").strip()
        cleaned = re.sub(r'[*_`#~]', '', cleaned)
        cleaned = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', cleaned)
        cleaned = cleaned.replace("\n", " ").replace("\t", " ")
        while "  " in cleaned:
            cleaned = cleaned.replace("  ", " ")
        return cleaned.strip()

    # ------------------------------------------------------------------
    # Core synthesis — called by TTSService for each aggregated sentence
    # ------------------------------------------------------------------

    @traced_tts
    async def run_tts(self, text: str, context_id: str) -> AsyncGenerator[Frame, None]:
        """Synthesise *text* and yield ``TTSAudioRawFrame`` objects.

        The base ``TTSService`` class calls this once per aggregated sentence.
        It handles sentence buffering, interruption recovery, and
        TTSStartedFrame / TTSStoppedFrame — this method only needs to yield
        audio frames (and ``ErrorFrame`` on failure).
        """
        logger.debug(f"{self}: Generating TTS [{text}]")

        cleaned = self._sanitize(text)
        if not cleaned or not any(ch.isalnum() for ch in cleaned):
            return

        speed = self._settings.speed
        if speed is NOT_GIVEN or speed is None:
            speed = 1.0

        payload: dict = {
            "input": cleaned,
            "response_format": "wav",
            "sample_rate": 24000,   # Panini native rate; pipecat resamples if needed
            "speed": float(speed),
        }

        voice = self._settings.voice
        if voice:
            payload["voice"] = voice

        language = self._settings.language
        if language:
            payload["language"] = language

        await self.start_tts_usage_metrics(text)

        try:
            assert self._http_session is not None, "SonexTTSService: session not initialised (start() not called)"

            async with self._http_session.post(
                f"{self._endpoint}/v1/audio/speech",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=30),
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    "Content-Type": "application/json",
                    "Accept": "audio/wav",
                },
            ) as response:
                if response.status >= 400:
                    body = (await response.text())[:300]
                    yield ErrorFrame(error=f"SonexTTS error ({response.status}): {body}")
                    return

                # TTFB — response headers arrived, GPU inference is complete
                await self.stop_ttfb_metrics()

                async def _iter_chunks() -> AsyncGenerator[bytes, None]:
                    async for chunk in response.content.iter_chunked(8192):
                        yield chunk

                # strip_wav_header=True: the base class strips the 44-byte WAV header
                # from the first chunk and auto-detects the source sample rate,
                # then resamples to self.sample_rate if they differ.
                async for frame in self._stream_audio_frames_from_iterator(
                    _iter_chunks(),
                    strip_wav_header=True,
                    context_id=context_id,
                ):
                    yield frame

        except Exception as exc:
            yield ErrorFrame(error=f"SonexTTS error: {exc}")
