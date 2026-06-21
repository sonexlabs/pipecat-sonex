"""
Exotel telephony bot — FastAPI WebSocket + Exotel media streams + SonexLabs TTS.

Exotel sends 8 kHz μ-law audio over a WebSocket (same wire format as Twilio).
SonexLabs synthesises at 24 kHz; pipecat resamples to 8 kHz for playback.

Architecture
------------
  Exotel PSTN call
    → WebSocket /ws/exotel  (configure in your Exotel applet as a Passthru URL)
    → pipecat pipeline: STT → LLM → TTS → Exotel

Usage
-----
1. Copy and fill in credentials::

       cp .env.example .env

2. Install dependencies::

       pip install "pipecat-ai[openai,deepgram,silero]" pipecat-sonex python-dotenv fastapi uvicorn

3. Run::

       python examples/telephony_exotel.py

4. Expose publicly (e.g. ngrok)::

       ngrok http 8080

5. In your Exotel applet, set the Passthru URL to::

       wss://<ngrok-host>/ws/exotel

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
from pipecat.runner.utils import parse_telephony_websocket
from pipecat.serializers.exotel import ExotelFrameSerializer
from pipecat.services.deepgram.stt import DeepgramSTTService
from pipecat.services.openai.llm import OpenAILLMService
from pipecat.transports.websocket.fastapi import FastAPIWebsocketParams, FastAPIWebsocketTransport

from pipecat_sonex import SonexTTSService

load_dotenv()

app = FastAPI()

EXOTEL_SAMPLE_RATE = 8000  # Exotel uses 8 kHz μ-law


@app.websocket("/ws/exotel")
async def websocket_exotel(ws: WebSocket):
    await ws.accept()

    # parse_telephony_websocket reads the Exotel start event to extract
    # stream_id and call_id before handing control to the pipeline.
    _, call_data = await parse_telephony_websocket(ws)

    serializer = ExotelFrameSerializer(
        stream_sid=call_data.get("stream_id", ""),
        call_sid=call_data.get("call_id", ""),
    )

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
        language=os.environ.get("SONEX_LANGUAGE", "hi"),   # Hindi default for Exotel India
        sample_rate=EXOTEL_SAMPLE_RATE,               # pipecat resamples 24kHz→8kHz automatically
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
        audio_in_sample_rate=EXOTEL_SAMPLE_RATE,
        audio_out_sample_rate=EXOTEL_SAMPLE_RATE,
        allow_interruptions=True,
    ))

    @transport.event_handler("on_client_disconnected")
    async def on_disconnect(transport, client):
        await task.cancel()

    await runner.run(task)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)

