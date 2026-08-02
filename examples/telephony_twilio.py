"""
Twilio telephony bot — FastAPI WebSocket + Twilio media streams + SonexLabs TTS.

Twilio sends 8 kHz μ-law audio over a WebSocket.  SonexLabs synthesises at
24 kHz; pipecat resamples to 8 kHz before sending back.

Architecture
------------
  Twilio PSTN call
    → POST /incoming-call  (returns TwiML that opens a media stream)
    → WebSocket /ws/twilio  (bidirectional audio stream)
    → pipecat pipeline: STT → LLM → TTS → Twilio

Usage
-----
1. Copy and fill in credentials::

       cp .env.example .env

2. Install dependencies::

       pip install "pipecat-ai[openai,deepgram,silero,twilio]" pipecat-sonex python-dotenv fastapi uvicorn

3. Run::

       python examples/telephony_twilio.py

4. Expose publicly (e.g. ngrok)::

       ngrok http 8080

5. In your Twilio console, set the phone number's Voice webhook to::

       https://<ngrok-host>/incoming-call   (HTTP POST)

Environment variables
---------------------
SONEX_API_KEY       SonexLabs API key (vsk_...)
SONEX_VOICE_ID      Voice ID from GET /v1/voices (required)
SONEX_LANGUAGE      BCP-47 language tag, e.g. "en" (optional)
OPENAI_API_KEY      OpenAI API key
DEEPGRAM_API_KEY    Deepgram API key
PUBLIC_HOST         Your public ngrok / domain hostname (no https://)
TWILIO_ACCOUNT_SID  Twilio account SID (optional — for auto-hangup)
TWILIO_AUTH_TOKEN   Twilio auth token  (optional — for auto-hangup)
"""

import asyncio
import os

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket
from fastapi.responses import PlainTextResponse
import uvicorn

from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
# CHANGED (2026-08-01): same fix as examples/webrtc_openai.py — pipecat-ai
# removed OpenAILLMContext in current releases (confirmed by inspecting the
# installed pipecat-ai wheel directly; not present in 1.1.0 or 1.6.0).
# Replaced with the provider-agnostic LLMContext + LLMContextAggregatorPair.
# ORIGINAL:
# from pipecat.processors.aggregators.openai_llm_context import OpenAILLMContext
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import LLMContextAggregatorPair
from pipecat.runner.utils import parse_telephony_websocket
from pipecat.serializers.twilio import TwilioFrameSerializer
from pipecat.services.deepgram.stt import DeepgramSTTService
from pipecat.services.openai.llm import OpenAILLMService
from pipecat.transports.websocket.fastapi import FastAPIWebsocketParams, FastAPIWebsocketTransport

from pipecat_sonex import SonexTTSService

load_dotenv()

app = FastAPI()

TWILIO_SAMPLE_RATE = 8000  # Twilio uses 8 kHz μ-law


@app.post("/incoming-call")
async def incoming_call():
    """Return TwiML that opens a bidirectional media stream to this server."""
    host = os.environ.get("PUBLIC_HOST", "your-ngrok-host.ngrok.io")
    twiml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="wss://{host}/ws/twilio"/>
  </Connect>
</Response>"""
    return PlainTextResponse(twiml, media_type="text/xml")


@app.websocket("/ws/twilio")
async def websocket_twilio(ws: WebSocket):
    await ws.accept()

    # parse_telephony_websocket reads the Twilio start event to extract
    # stream_sid and call_sid before handing control to the pipeline.
    _, call_data = await parse_telephony_websocket(ws)

    serializer = TwilioFrameSerializer(
        stream_sid=call_data.get("stream_id", ""),
        call_sid=call_data.get("call_id", ""),
        account_sid=os.environ.get("TWILIO_ACCOUNT_SID", ""),
        auth_token=os.environ.get("TWILIO_AUTH_TOKEN", ""),
        params=TwilioFrameSerializer.InputParams(auto_hang_up=False),
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
        language=os.environ.get("SONEX_LANGUAGE", "en"),
        sample_rate=TWILIO_SAMPLE_RATE,               # pipecat resamples 24kHz→8kHz automatically
    )

    messages = [{"role": "system", "content": "You are a helpful phone assistant. Keep responses brief."}]
    # CHANGED (2026-08-01): OpenAILLMContext + llm.create_context_aggregator() no
    # longer exist in current pipecat-ai. Replaced with the universal LLMContext API.
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
    task = PipelineTask(pipeline, params=PipelineParams(
        audio_in_sample_rate=TWILIO_SAMPLE_RATE,
        audio_out_sample_rate=TWILIO_SAMPLE_RATE,
        allow_interruptions=True,
    ))

    @transport.event_handler("on_client_disconnected")
    async def on_disconnect(transport, client):
        await task.cancel()

    await runner.run(task)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)