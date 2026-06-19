"""
WebRTC example using SmallWebRTC transport + OpenAI LLM + SonexLabs Panini TTS.

Usage
-----
cp .env.example .env   # fill in your credentials
python examples/webrtc_openai.py
"""

import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineTask
from pipecat.processors.aggregators.openai_llm_context import OpenAILLMContext
from pipecat.services.openai import OpenAILLMService
from pipecat.transports.network.small_webrtc import SmallWebRTCTransport, SmallWebRTCParams

from pipecat_sonex import PaniniStreamingTTSProcessor

PANINI_ENDPOINTS = os.environ["PANINI_TTS_ENDPOINTS"].split(",")
SONEX_API_TOKEN  = os.environ["SONEX_API_TOKEN"]
OPENAI_API_KEY   = os.environ["OPENAI_API_KEY"]


async def main():
    transport = SmallWebRTCTransport(
        params=SmallWebRTCParams(audio_in_enabled=True, audio_out_enabled=True),
    )

    llm = OpenAILLMService(api_key=OPENAI_API_KEY, model="gpt-4o-mini")

    messages = [{"role": "system", "content": "You are a helpful voice assistant. Keep responses concise."}]
    context = OpenAILLMContext(messages)
    context_aggregator = llm.create_context_aggregator(context)

    tts = PaniniStreamingTTSProcessor(
        api_token=SONEX_API_TOKEN,
        model="panini",
        voice="auto",
        language="en",
        num_step=16,
        speed=1.0,
        sample_rate=24000,
        endpoints=PANINI_ENDPOINTS,
    )

    pipeline = Pipeline([
        transport.input(),
        context_aggregator.user(),
        llm,
        tts,
        transport.output(),
        context_aggregator.assistant(),
    ])

    runner = PipelineRunner()
    task = PipelineTask(pipeline)
    await runner.run(task)


if __name__ == "__main__":
    asyncio.run(main())
