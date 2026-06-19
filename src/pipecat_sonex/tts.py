"""
PaniniStreamingTTSProcessor
===========================
A pipecat-ai FrameProcessor that proxies text to the SonexLabs Panini TTS
GPU cluster and streams the resulting PCM audio downstream.

Key design decisions
--------------------
* **Inference lock** — a single asyncio.Lock serialises requests to the GPU so
  consecutive sentences do not race for VRAM and cause CUDA OOM errors.
* **TTFB lock release** — the lock is released immediately when the first byte
  of the HTTP response arrives (i.e. when GPU inference is complete), not when
  the full WAV body has been downloaded.  This lets the *next* sentence begin
  GPU inference while the current WAV is still transferring over the network,
  saving up to ~680 ms per sentence on multi-sentence responses.
* **Endpoint rotation** — pass multiple endpoint URLs to distribute load across
  several GPU nodes.
* **Sentence splitting** — LLM tokens are buffered until a sentence boundary
  (``!``, ``?``, ``…``, non-digit ``·``) is detected so that Panini can start
  inference before the LLM has finished generating.
"""

from __future__ import annotations

import asyncio
import re
import struct
import time
from typing import List, Optional

import aiohttp

from pipecat.frames.frames import (
    Frame,
    FrameDirection,
    LLMFullResponseEndFrame,
    LLMTextFrame,
    TTSAudioRawFrame,
    TTSSpeakFrame,
)
from pipecat.processors.frame_processor import FrameProcessor

# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

# Sentence boundary regex — split LLM output into speakable chunks as tokens
# stream in, reducing time-to-first-audio.
# (?<=\D\.) avoids splitting on numbered list items (1. 2. 3.) where the
# character before the period is a digit.
_SENTENCE_SPLIT_RE = re.compile(r'(?<=[!?…])\s+|(?<=\D\.)\s+')

# Shared aiohttp session (created lazily, shared across all processor instances)
_http_session: Optional[aiohttp.ClientSession] = None


def _get_http_session() -> aiohttp.ClientSession:
    global _http_session
    if _http_session is None or _http_session.closed:
        connector = aiohttp.TCPConnector(
            limit=64,
            limit_per_host=16,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
            keepalive_timeout=30,
        )
        _http_session = aiohttp.ClientSession(connector=connector)
    return _http_session


def _safe_str(value, fallback: str) -> str:
    return value if isinstance(value, str) and value.strip() else fallback


def _safe_float(value, fallback: float) -> float:
    try:
        if value is None or value == "":
            return fallback
        return float(value)
    except Exception:
        return fallback


def _safe_int(value, fallback: int) -> int:
    try:
        if value is None or value == "":
            return fallback
        return int(float(value))
    except Exception:
        return fallback


# ---------------------------------------------------------------------------
# Main processor
# ---------------------------------------------------------------------------


class PaniniStreamingTTSProcessor(FrameProcessor):
    """Panini TTS streaming processor over HTTP with endpoint rotation for load balancing.

    Parameters
    ----------
    api_token:
        SonexLabs API token (``vsk_...``).  Sent as ``Authorization: Bearer``
        in every request to the Panini cluster.
    model:
        Panini model name, e.g. ``"panini"``.
    voice:
        Voice ID as configured in the SonexLabs dashboard, or ``"auto"`` to
        use the GPU server's built-in default speaker.
    language:
        BCP-47 language tag, e.g. ``"en"``, ``"hi"``, ``"te"``.  Optional —
        omit to let Panini auto-detect from the input text.
    num_step:
        Diffusion sampling steps.  Higher = better quality, slower inference.
        Default ``16`` is a good balance for real-time telephony.
    speed:
        Speech rate multiplier.  ``1.0`` is normal speed.
    sample_rate:
        Output PCM sample rate in Hz.  Panini always synthesises at 24 kHz;
        pipecat resamples downstream if this differs.  Set to ``8000`` for
        Twilio/Exotel PCMU telephony transports.
    endpoints:
        One or more Panini cluster URLs.  Requests are round-robin distributed
        across all URLs.  Example::

            endpoints=["http://157.15.202.177:8000", "http://157.15.202.178:8000"]
    """

    def __init__(
        self,
        *,
        api_token: str,
        model: str = "panini",
        voice: str = "auto",
        language: str = "",
        num_step: int = 16,
        speed: float = 1.0,
        sample_rate: int = 24000,
        endpoints: List[str],
    ):
        super().__init__(name="PaniniStreamingTTSProcessor")
        self._api_token = api_token
        self._model = model
        self._voice = voice
        self._language = language
        self._num_step = num_step
        self._speed = speed
        self._sample_rate = sample_rate

        if not endpoints:
            raise ValueError(
                "PaniniStreamingTTSProcessor requires at least one entry in 'endpoints'."
            )
        self._endpoints = endpoints
        self._current_endpoint_idx = 0
        self._llm_buffer = ""

        # Serialise all TTS requests: the GPU has one runner so concurrent
        # requests race for VRAM, cause CUDA OOM, and produce no audio.
        self._request_lock: asyncio.Lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_next_endpoint(self) -> str:
        """Return next endpoint in round-robin order."""
        endpoint = self._endpoints[self._current_endpoint_idx]
        self._current_endpoint_idx = (self._current_endpoint_idx + 1) % len(self._endpoints)
        return endpoint

    def _sanitize_tts_text(self, text: str) -> str:
        """Strip markdown and control characters that Panini would speak literally."""
        cleaned = (text or "").strip()
        cleaned = re.sub(r'[*_`#~]', '', cleaned)
        cleaned = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', cleaned)
        cleaned = "".join(ch for ch in cleaned if ch.isprintable() or ch in {"\n", "\t"})
        cleaned = cleaned.replace("\n", " ").replace("\t", " ")
        while "  " in cleaned:
            cleaned = cleaned.replace("  ", " ")
        return cleaned.strip()

    def _is_speakable_text(self, text: str) -> bool:
        cleaned = self._sanitize_tts_text(text)
        if not cleaned:
            return False
        return any(ch.isalnum() for ch in cleaned)

    async def _stream_audio_chunks(self, text: str):
        """Yield ``(pcm_bytes, sample_rate, channels)`` tuples, preceded by a
        ``(None, None, None)`` TTFB sentinel that signals the inference lock
        can be released."""
        if not self._voice:
            self._voice = "auto"

        cleaned_text = self._sanitize_tts_text(text)
        if not self._is_speakable_text(cleaned_text):
            return

        timeout = aiohttp.ClientTimeout(total=30)
        _panini_native_rate = 24000

        payload: dict = {
            "model": self._model,
            "input": cleaned_text,
            "num_step": self._num_step,
            "speed": self._speed,
            "response_format": "wav",
            "sample_rate": _panini_native_rate,
        }
        if self._voice and self._voice != "auto":
            payload["voice"] = self._voice
        if self._language:
            payload["language"] = self._language

        last_error = None
        _ttfb_signaled = False
        for attempt_idx in range(len(self._endpoints) * 3):
            endpoint = self._get_next_endpoint()
            t_start = time.monotonic()
            try:
                session = _get_http_session()
                async with session.post(
                    f"{endpoint}/v1/audio/speech",
                    json=payload,
                    timeout=timeout,
                    headers={
                        "Authorization": f"Bearer {self._api_token}",
                        "Content-Type": "application/json",
                        "Accept": "audio/wav",
                    },
                ) as response:
                    if response.status >= 400:
                        body = (await response.text())[:500]
                        raise RuntimeError(f"Panini TTS request failed ({response.status}): {body}")

                    # TTFB sentinel: GPU is done — release the lock so the next
                    # sentence can start inference while we download this WAV.
                    if not _ttfb_signaled:
                        yield (None, None, None)
                        _ttfb_signaled = True

                    wav_bytes = await response.content.read()

                    # Parse WAV header to extract PCM data and actual sample rate
                    detected_sample_rate = _panini_native_rate
                    fmt_marker = wav_bytes.find(b"fmt ")
                    if fmt_marker >= 0 and len(wav_bytes) >= fmt_marker + 24:
                        _, _, fmt_sample_rate, _, _, _ = struct.unpack(
                            "<HHIIHH", wav_bytes[fmt_marker + 8: fmt_marker + 24]
                        )
                        detected_sample_rate = int(fmt_sample_rate or _panini_native_rate)

                    data_marker = wav_bytes.find(b"data")
                    if data_marker < 0:
                        raise RuntimeError("Panini WAV response missing 'data' chunk")

                    pcm_bytes = wav_bytes[data_marker + 8:]
                    if len(pcm_bytes) % 2 != 0:
                        pcm_bytes = pcm_bytes + b'\x00'

                    if pcm_bytes:
                        yield pcm_bytes, detected_sample_rate, 1
                return  # success — stop retrying
            except Exception as exc:
                last_error = exc
                await asyncio.sleep(0.1)
                continue

        raise RuntimeError(
            f"All Panini TTS endpoints failed after {len(self._endpoints) * 3} attempts. "
            f"Last error: {last_error}"
        )

    async def _synthesize(self, text: str) -> int:
        """Acquire the inference lock, synthesise *text*, push TTSAudioRawFrames downstream.

        The lock is released when the TTFB sentinel arrives — i.e. as soon as
        GPU inference is complete and response headers arrive.  WAV body
        download happens with the lock already released so the next sentence
        can start GPU inference immediately.

        Returns the number of audio frames pushed downstream.
        """
        await self._request_lock.acquire()
        lock_released = False
        audio_chunks_pushed = 0
        try:
            async for item in self._stream_audio_chunks(text):
                if item[0] is None:
                    # TTFB sentinel — release the lock for the next sentence.
                    self._request_lock.release()
                    lock_released = True
                else:
                    audio_chunk, detected_rate, detected_channels = item
                    audio_chunks_pushed += 1
                    await self.push_frame(
                        TTSAudioRawFrame(
                            audio=audio_chunk,
                            sample_rate=detected_rate,
                            num_channels=detected_channels,
                        ),
                        FrameDirection.DOWNSTREAM,
                    )
        finally:
            if not lock_released:
                self._request_lock.release()
        return audio_chunks_pushed

    # ------------------------------------------------------------------
    # FrameProcessor interface
    # ------------------------------------------------------------------

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)

        if isinstance(frame, TTSSpeakFrame):
            text = _safe_str(getattr(frame, "text", ""), "")
            if text and self._is_speakable_text(text):
                await self._synthesize(text)

        elif isinstance(frame, LLMTextFrame):
            token = _safe_str(getattr(frame, "text", ""), "")
            if token:
                self._llm_buffer += token
                if _SENTENCE_SPLIT_RE.search(self._llm_buffer):
                    parts = _SENTENCE_SPLIT_RE.split(self._llm_buffer)
                    if len(parts) > 1:
                        complete = " ".join(parts[:-1]).strip()
                        self._llm_buffer = parts[-1]
                        if self._is_speakable_text(complete):
                            await self._synthesize(complete)

        elif isinstance(frame, LLMFullResponseEndFrame):
            text = self._llm_buffer.strip()
            self._llm_buffer = ""
            if self._is_speakable_text(text):
                await self._synthesize(text)

        await self.push_frame(frame, direction)
