"""
VobizFrameSerializer
====================
A pipecat ``FrameSerializer`` for the Vobiz bidirectional WebSocket JSON
protocol.

Vobiz sends 8 kHz μ-law (mulaw) audio encoded as base64 JSON messages
inbound.  This serializer converts inbound μ-law to PCM for the pipecat
pipeline, and converts outbound PCM back to 8 kHz μ-law for playback.

Usage
-----
::

    from pipecat_sonex.vobiz import VobizFrameSerializer
    from pipecat.transports.websocket.fastapi import (
        FastAPIWebsocketParams,
        FastAPIWebsocketTransport,
    )

    serializer = VobizFrameSerializer(stream_id=call_data["stream_id"])
    transport = FastAPIWebsocketTransport(
        websocket=ws,
        params=FastAPIWebsocketParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            add_wav_header=False,
            serializer=serializer,
        ),
    )

Note
----
This serializer matches the official ``pipecat-vobiz`` 0.0.2 implementation.
It is bundled here so ``pipecat-sonex`` works out of the box without an
additional package install.
"""

from __future__ import annotations

import base64
import json
from typing import Optional

from loguru import logger

from pipecat.audio.utils import create_stream_resampler, pcm_to_ulaw, ulaw_to_pcm
from pipecat.frames.frames import (
    AudioRawFrame,
    EndFrame,
    Frame,
    InputAudioRawFrame,
    InterruptionFrame,
)
from pipecat.serializers.base_serializer import FrameSerializer


class VobizFrameSerializer(FrameSerializer):
    """Pipecat FrameSerializer for the Vobiz bidirectional WebSocket JSON protocol.

    Parameters
    ----------
    stream_id:
        Vobiz ``streamId`` returned in the ``start`` event.  Pass an empty
        string if the stream ID is not yet known — it will be populated
        automatically when the first ``start`` event arrives.
    """

    class InputParams(FrameSerializer.InputParams):
        vobiz_sample_rate: int = 8000
        sample_rate: Optional[int] = None

    def __init__(self, stream_id: str = "", params: Optional[InputParams] = None):
        super().__init__(params or VobizFrameSerializer.InputParams())
        self._stream_id = stream_id
        self._vobiz_sample_rate: int = self._params.vobiz_sample_rate
        self._sample_rate: int = 0  # resolved in setup() from StartFrame
        self._input_resampler = create_stream_resampler()
        self._output_resampler = create_stream_resampler()

    async def setup(self, frame) -> None:
        self._sample_rate = self._params.sample_rate or frame.audio_in_sample_rate

    # ------------------------------------------------------------------
    # Deserialise: Vobiz → pipecat
    # ------------------------------------------------------------------

    async def deserialize(self, data) -> Optional[Frame]:  # type: ignore[override]
        if isinstance(data, bytes):
            try:
                data = data.decode("utf-8")
            except Exception:
                logger.warning(f"[vobiz] received non-UTF-8 bytes (len={len(data)}), skipping")
                return None

        try:
            msg = json.loads(data)
        except Exception as exc:
            logger.warning(f"[vobiz] JSON parse error: {exc} — data={str(data)[:80]}")
            return None

        event = msg.get("event")

        if event == "start":
            start = msg.get("start") or {}
            if not self._stream_id:
                self._stream_id = str(start.get("streamId") or "")
            return None

        if event == "media":
            media = msg.get("media") or {}
            payload_b64 = str(media.get("payload") or "")
            if not payload_b64:
                return None
            try:
                raw = base64.b64decode(payload_b64)
            except Exception:
                return None

            target_rate = self._sample_rate or self._vobiz_sample_rate
            pcm = await ulaw_to_pcm(raw, self._vobiz_sample_rate, target_rate, self._input_resampler)
            if not pcm:
                return None
            return InputAudioRawFrame(audio=pcm, sample_rate=target_rate, num_channels=1)

        return None

    # ------------------------------------------------------------------
    # Serialise: pipecat → Vobiz
    # ------------------------------------------------------------------

    async def serialize(self, frame: Frame) -> Optional[str]:  # type: ignore[override]
        if isinstance(frame, InterruptionFrame):
            return json.dumps({"event": "clearAudio", "streamId": self._stream_id})

        if isinstance(frame, AudioRawFrame):
            if not frame.audio:
                return None
            mulaw_audio = await pcm_to_ulaw(
                frame.audio, frame.sample_rate, self._vobiz_sample_rate, self._output_resampler
            )
            if not mulaw_audio:
                return None
            payload_b64 = base64.b64encode(mulaw_audio).decode("ascii")
            return json.dumps(
                {
                    "event": "playAudio",
                    "media": {
                        "contentType": "audio/x-mulaw",
                        "sampleRate": self._vobiz_sample_rate,
                        "payload": payload_b64,
                    },
                    "streamId": self._stream_id,
                }
            )

        if isinstance(frame, EndFrame):
            if self._stream_id:
                return json.dumps({"event": "stop", "streamId": self._stream_id})
            return None

        return None
