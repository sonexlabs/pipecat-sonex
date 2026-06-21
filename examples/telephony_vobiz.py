"""
Vobiz telephony bot — FastAPI WebSocket + Vobiz media streams + SonexLabs TTS.

Vobiz sends 8 kHz μ-law audio encoded as base64 JSON over a WebSocket.
SonexLabs synthesises at 24 kHz; pipecat resamples to 8 kHz for playback.

Architecture
------------
  Vobiz PSTN call
    → WebSocket /ws/vobiz  (configure in your Vobiz application)
    → pipecat pipeline: STT → LLM → TTS → Vobiz

The ``VobizFrameSerializer`` is bundled in ``pipecat_sonex.vobiz`` — no
extra package install required.

Usage
-----
1. Copy and fill in credentials::

       cp .env.example .env

2. Install dependencies::

       pip install "pipecat-ai[openai,deepgram,silero]" pipecat-sonex python-dotenv fastapi uvicorn

3. Run::

       python examples/telephony_vobiz.py

4. Configure your Vobiz application's WebSocket URL to::

       wss://<your-host>/ws/vobiz

Environment variables
---------------------
SONEX_API_KEY       SonexLabs API key (vsk_...)
SONEX_VOICE_ID      Voice ID from GET /v1/voices (required)
SONEX_LANGUAGE      BCP-47 language tag, e.g. "hi" for Hindi (optional)
OPENAI_API_KEY      OpenAI API key
DEEPGRAM_API_KEY    Deepgram API key
"""

import asyncio
import os

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket
import uvicorn

from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.openai_llm_context import OpenAILLMContext
from pipecat.services.deepgram.stt import DeepgramSTTService
from pipecat.services.openai.llm import OpenAILLMService
from pipecat.transports.websocket.fastapi import FastAPIWebsocketParams, FastAPIWebsocketTransport

from pipecat_sonex import SonexTTSService
from pipecat_sonex.vobiz import VobizFrameSerializer

load_dotenv()

app = FastAPI()

VOBIZ_SAMPLE_RATE = 8000  # Vobiz uses 8 kHz μ-law


@app.websocket("/ws/vobiz")
async def websocket_vobiz(ws: WebSocket):
    await ws.accept()

    # Vobiz sends a JSON start event with streamId first.
    # VobizFrameSerializer.deserialize() handles the start event and populates
    # stream_id automatically — we can pass an empty string here.
    serializer = VobizFrameSerializer(stream_id="")

    transport = FastAPIWebsocketTransport(
        websocket=ws,
        params=FastAPIWebsocketParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            add_wav_header=False,
            vad_enabled=True,
            vad_analyzer=SileroVADAnalyzer(),
            vad_audio_passthrough=True,
            serializer=serializer,
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
        language=os.environ.get("SONEX_LANGUAGE", "hi"),   # Hindi default for Vobiz India
        sample_rate=VOBIZ_SAMPLE_RATE,                # pipecat resamples 24kHz→8kHz automatically
    )

    messages = [{"role": "system", "content": "You are a helpful phone assistant. Keep responses brief."}]
    context = OpenAILLMContext(messages)
    context_aggregator = llm.create_context_aggregator(context)

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
    task = PipelineTask(pipeline, params=PipelineParams(
        audio_in_sample_rate=VOBIZ_SAMPLE_RATE,
        audio_out_sample_rate=VOBIZ_SAMPLE_RATE,
        allow_interruptions=True,
    ))

    @transport.event_handler("on_client_disconnected")
    async def on_disconnect(transport, client):
        await task.cancel()

    await runner.run(task)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)

