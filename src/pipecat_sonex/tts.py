"""
SonexTTSService
===============
A pipecat-ai ``TTSService`` that synthesises speech via the SonexLabs Panini
TTS API (``POST /v1/speech/stream``).

Extends the official ``TTSService`` base class so it plugs into any pipecat
pipeline exactly like CartesiaTTSService, ElevenLabsTTSService, etc.  The
base class handles sentence aggregation, LLM token buffering, interruption
recovery, metrics and TTSStartedFrame / TTSStoppedFrame bookkeeping — this
class only needs to implement ``run_tts``.

The streaming endpoint returns audio as chunked HTTP as soon as each
sentence is ready rather than waiting for the entire utterance, so playback
can start sooner. The underlying ``aiohttp.ClientSession`` uses a
``TCPConnector`` with ``keepalive_timeout`` set, so connections are reused
across requests instead of reconnecting for every sentence.

Runtime settings (voice, language, speed) can be changed mid-conversation via
``TTSUpdateSettingsFrame(delta=SonexTTSService.Settings(voice="new-voice-id"))``.
"""

from __future__ import annotations

import re
from collections.abc import AsyncGenerator, AsyncIterator
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
        Voice ID from the SonexLabs voice library, e.g. ``"72ly9crx9v"``
        (list available voices with ``GET /v1/voices``).
        Required — there is no default voice.
    language:
        Language code, e.g. ``"en"``, ``"hi"``, ``"te"``. Leave unset to
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

    Sends synthesised speech requests to ``POST /v1/speech/stream`` on the
    SonexLabs API, streams back the WAV response chunk-by-chunk as it's
    generated, strips the WAV header, and yields ``TTSAudioRawFrame``
    objects for pipecat's audio pipeline.

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
        Pipeline output rate in Hz. Not sent to the API — Panini always
        returns audio at its native rate; pipecat resamples to this value.
        Defaults to ``24000`` (WebRTC). Use ``8000`` for Twilio / Exotel
        telephony.
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
            voice="72ly9crx9v",  # from GET /v1/voices
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
    # WAV parsing helper
    # ------------------------------------------------------------------
    #
    # CHANGED (2026-08-01): the pipecat-ai base TTSService, when called with
    # strip_wav_header=True, blindly strips exactly the first 44 bytes off
    # the response and reads the sample rate from a fixed byte offset. That
    # only works for the minimal/canonical WAV header. If the response ever
    # includes any extra chunk before "data" (LIST/INFO, fact, extended
    # "fmt "), real audio doesn't start at byte 44 and leftover header bytes
    # get played back as if they were audio samples.
    #
    # This turned out NOT to be the actual cause of the noise heard during
    # testing (see the endpoint fix below for the real cause), but it's
    # still a correctness improvement worth keeping: it parses the real
    # RIFF/WAVE chunk structure instead of guessing a fixed offset, so it
    # stays correct even if SonexLabs' response format ever changes to
    # include extra chunks.
    #
    # ORIGINAL call site (kept here for reference, no longer used):
    #
    #     async def _iter_chunks() -> AsyncGenerator[bytes, None]:
    #         async for chunk in response.content.iter_chunked(8192):
    #             yield chunk
    #
    #     # strip_wav_header=True: the base class strips the 44-byte WAV header
    #     # from the first chunk and auto-detects the source sample rate,
    #     # then resamples to self.sample_rate if they differ.
    #     async for frame in self._stream_audio_frames_from_iterator(
    #         _iter_chunks(),
    #         strip_wav_header=True,
    #         context_id=context_id,
    #     ):
    #         yield frame

    @staticmethod
    async def _split_wav_header(
        iterator: AsyncIterator[bytes],
    ) -> tuple[int, bytes, AsyncIterator[bytes]]:
        """Consume *iterator* until the WAV ``data`` sub-chunk is located.

        Walks the RIFF/WAVE chunk list properly (chunk id + chunk size,
        skipping any chunks that aren't "fmt " or "data") rather than
        assuming a fixed 44-byte header. Returns ``(sample_rate,
        leftover_pcm_bytes, iterator)``.
        """
        buf = bytearray()
        sample_rate: Optional[int] = None

        async for chunk in iterator:
            buf.extend(chunk)

            if len(buf) < 12:
                continue
            if buf[0:4] != b"RIFF" or buf[8:12] != b"WAVE":
                logger.warning(
                    "SonexTTSService: response did not start with a RIFF/WAVE "
                    "header; passing bytes through unparsed."
                )
                return 24000, bytes(buf), iterator

            pos = 12
            while len(buf) >= pos + 8:
                chunk_id = bytes(buf[pos:pos + 4])
                chunk_size = int.from_bytes(buf[pos + 4:pos + 8], "little")
                body_start = pos + 8

                if chunk_id == b"fmt ":
                    if len(buf) < body_start + 16:
                        break
                    sample_rate = int.from_bytes(buf[body_start + 4:body_start + 8], "little")

                if chunk_id == b"data":
                    if len(buf) < body_start:
                        break
                    leftover = bytes(buf[body_start:])
                    return sample_rate or 24000, leftover, iterator

                pos = body_start + chunk_size + (chunk_size % 2)

        return sample_rate or 24000, bytes(buf), iterator

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

        # CHANGED (2026-08-01): field names corrected to match SonexLabs' actual
        # documented API schema (SpeechRequest in their OpenAPI spec). The
        # original field names below don't exist in their schema at all, so
        # requests were silently malformed — see the endpoint fix below for
        # how this was diagnosed.
        #
        # ORIGINAL:
        #     payload: dict = {
        #         "input": cleaned,
        #         "response_format": "wav",
        #         "sample_rate": 24000,   # Panini native rate; pipecat resamples if needed
        #         "speed": float(speed),
        #     }
        #     voice = self._settings.voice
        #     if voice:
        #         payload["voice"] = voice
        #     language = self._settings.language
        #     if language:
        #         payload["language"] = language
        payload: dict = {
            "text": cleaned,
            "output_format": "wav",
            "speed": float(speed),
        }

        voice = self._settings.voice
        if voice:
            payload["voice_id"] = voice

        language = self._settings.language
        if language:
            payload["language"] = language

        await self.start_tts_usage_metrics(text)

        try:
            assert self._http_session is not None, "SonexTTSService: session not initialised (start() not called)"

            # CHANGED (2026-08-01): corrected endpoint path. The original path
            # (/v1/audio/speech) does not exist on SonexLabs' API — confirmed
            # by checking their published OpenAPI spec, where the real
            # text-to-speech route is documented as POST /v1/speech. Hitting
            # the wrong path returned an HTTP 200 with an HTML page (not a
            # proper 404), so pipecat never saw an error — it just streamed
            # the HTML bytes into the audio pipeline as if they were PCM
            # samples, which is what produced the noisy/garbled audio heard
            # during testing.
            #
            # ORIGINAL:
            #     async with self._http_session.post(
            #         f"{self._endpoint}/v1/audio/speech",
            #         json=payload,
            #         ...
            async with self._http_session.post(
                f"{self._endpoint}/v1/speech/stream",
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

                # CHANGED (2026-08-01): use the proper WAV chunk parser above
                # instead of strip_wav_header=True's fixed 44-byte assumption.
                # See _split_wav_header() docstring for the full explanation.
                sample_rate, leftover, remaining = await self._split_wav_header(_iter_chunks())

                async def _pcm_stream() -> AsyncGenerator[bytes, None]:
                    if leftover:
                        yield leftover
                    async for chunk in remaining:
                        yield chunk

                async for frame in self._stream_audio_frames_from_iterator(
                    _pcm_stream(),
                    strip_wav_header=False,
                    in_sample_rate=sample_rate,
                    context_id=context_id,
                ):
                    yield frame

        except Exception as exc:
            yield ErrorFrame(error=f"SonexTTS error: {exc}")