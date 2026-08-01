"""
WebRTC voice bot — SmallWebRTC + OpenAI LLM + SonexLabs TTS.

Architecture
------------
A FastAPI app exposes two endpoints:

  POST /offer   — WebRTC SDP offer/answer exchange (and ICE trickle updates)
  GET  /        — Minimal HTML page that opens the WebRTC session from a browser

When a browser connects, pipecat runs a full voice pipeline:

  Mic audio → STT (Deepgram) → LLM (OpenAI) → TTS (SonexLabs) → Speaker

Usage
-----
1. Copy and fill in credentials::

       cp .env.example .env

2. Install dependencies::

       pip install "pipecat-ai[openai,deepgram,silero,webrtc]" pipecat-sonex python-dotenv fastapi uvicorn

3. Run::

       python examples/webrtc_openai.py

4. Open http://localhost:7860 in your browser and click "Connect".

Environment variables
---------------------
SONEX_API_KEY       SonexLabs API key (vsk_...)
SONEX_VOICE_ID      Voice ID from GET /v1/voices (required)
SONEX_LANGUAGE      BCP-47 language tag, e.g. "en" (optional)
OPENAI_API_KEY      OpenAI API key
DEEPGRAM_API_KEY    Deepgram API key
"""

import asyncio
import os
# from contextlib import asynccontextmanager  # ORIGINAL: unused import, left commented instead of deleted

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
import uvicorn

from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
# CHANGED (2026-08-01): pipecat-ai removed OpenAILLMContext in newer releases
# (module pipecat.processors.aggregators.openai_llm_context no longer exists as of
# pipecat-ai 1.1.0+, confirmed by inspecting the installed wheel directly).
# It was replaced by a provider-agnostic LLMContext + LLMContextAggregatorPair.
# ORIGINAL:
# from pipecat.processors.aggregators.openai_llm_context import OpenAILLMContext
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import LLMContextAggregatorPair
from pipecat.services.deepgram.stt import DeepgramSTTService
from pipecat.services.openai.llm import OpenAILLMService
from pipecat.transports.base_transport import TransportParams
from pipecat.transports.smallwebrtc.connection import SmallWebRTCConnection
from pipecat.transports.smallwebrtc.transport import SmallWebRTCTransport
from pipecat.transports.smallwebrtc.request_handler import (
    IceCandidate,
    SmallWebRTCPatchRequest,
    SmallWebRTCRequest,
    SmallWebRTCRequestHandler,
)

from pipecat_sonex import SonexTTSService

load_dotenv()

# ---------------------------------------------------------------------------
# Global WebRTC handler (maintains pc_id → connection registry for ICE trickle)
# ---------------------------------------------------------------------------

_handler = SmallWebRTCRequestHandler(
    ice_servers=["stun:stun.l.google.com:19302"]
)


# ---------------------------------------------------------------------------
# Pipeline factory — called once per WebRTC connection
# ---------------------------------------------------------------------------

async def run_bot(connection: SmallWebRTCConnection) -> None:
    transport = SmallWebRTCTransport(
        webrtc_connection=connection,
        params=TransportParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            vad_analyzer=SileroVADAnalyzer(),
        ),
    )

    stt = DeepgramSTTService(api_key=os.environ["DEEPGRAM_API_KEY"])

    llm = OpenAILLMService(
        api_key=os.environ["OPENAI_API_KEY"],
        model="gpt-4o-mini",
    )

    tts = SonexTTSService(
        api_key=os.environ["SONEX_API_KEY"],
        voice=os.environ["SONEX_VOICE_ID"],           # required — get from GET /v1/voices
        language=os.environ.get("SONEX_LANGUAGE", "en"),
        sample_rate=24000,
    )

    messages = [{"role": "system", "content": "You are a helpful voice assistant. Keep responses concise."}]
    # CHANGED (2026-08-01): OpenAILLMContext + llm.create_context_aggregator() no longer
    # exist in current pipecat-ai. Replaced with the universal LLMContext API.
    # ORIGINAL:
    # context = OpenAILLMContext(messages)
    # context_aggregator = llm.create_context_aggregator(context)
    context = LLMContext(messages)
    context_aggregator = LLMContextAggregatorPair(context)

    pipeline = Pipeline([
        transport.input(),
        stt,
        context_aggregator.user(),
        llm,
        tts,
        transport.output(),
        context_aggregator.assistant(),
    ])

    runner = PipelineRunner()
    task = PipelineTask(pipeline, params=PipelineParams(allow_interruptions=True))

    @transport.event_handler("on_client_disconnected")
    async def on_disconnect(transport, client):
        await task.cancel()

    await runner.run(task)


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI()


@app.post("/offer")
async def offer(request: Request):
    """WebRTC signaling: SDP offer/answer + ICE trickle candidate updates."""
    body = await request.json()

    # ICE trickle: client sends {pc_id, candidates} after initial SDP exchange
    if "pc_id" in body and "candidates" in body:
        patch = SmallWebRTCPatchRequest(
            pc_id=body["pc_id"],
            candidates=[
                IceCandidate(
                    candidate=c["candidate"],
                    sdp_mid=c.get("sdpMid", ""),
                    sdp_mline_index=c.get("sdpMLineIndex", 0),
                )
                for c in body["candidates"]
            ],
        )
        await _handler.handle_patch_request(patch)
        return {"status": "ok"}

    # Initial SDP offer
    webrtc_request = SmallWebRTCRequest(
        sdp=body["sdp"],
        type=body.get("type", "offer"),
        pc_id=body.get("pc_id"),
    )

    async def on_connection(conn: SmallWebRTCConnection):
        asyncio.create_task(run_bot(conn))

    answer = await _handler.handle_web_request(webrtc_request, on_connection)
    return answer


@app.get("/", response_class=HTMLResponse)
async def index():
    """Minimal browser page that opens a WebRTC session."""
    return """<!DOCTYPE html>
<html>
<head><title>SonexLabs Voice Bot</title></head>
<body>
  <h2>SonexLabs WebRTC Voice Bot</h2>
  <button id="btn" onclick="connect()">Connect</button>
  <p id="status">Idle</p>
  <script>
    let pc;
    async function connect() {
      document.getElementById("status").textContent = "Connecting...";
      pc = new RTCPeerConnection({ iceServers: [{ urls: "stun:stun.l.google.com:19302" }] });
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      stream.getTracks().forEach(t => pc.addTrack(t, stream));
      pc.ontrack = e => { const a = new Audio(); a.srcObject = e.streams[0]; a.play(); };
      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);
      const res = await fetch("/offer", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sdp: offer.sdp, type: offer.type }),
      });
      const answer = await res.json();
      await pc.setRemoteDescription(answer);
      document.getElementById("status").textContent = "Connected";
    }
  </script>
</body>
</html>"""


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=7860)

# ORIGINAL: leftover/duplicate code found after this point in the file as received.
# Left here commented for reference — this caused a SyntaxError (`await` outside a
# function) and would have failed anyway even if syntactically valid, since `runner`
# and `pipeline` are local variables scoped inside run_bot() and don't exist here,
# and run_bot(None) would crash because run_bot expects a real SmallWebRTCConnection.
#     await runner.run(PipelineTask(pipeline, params=PipelineParams(allow_interruptions=True)))
#
#
# if __name__ == "__main__":
#     asyncio.run(run_bot(None))