"""
Minimal SonexLabs TTS example — speaks one line and exits.

This is the foundational example: the smallest possible pipeline showing
SonexTTSService in action, modeled on pipecat's own
examples/getting-started/01-say-one-thing.py.

Usage
-----
1. Copy and fill in credentials::

       cp .env.example .env

2. Install dependencies::

       pip install "pipecat-ai[webrtc]" pipecat-sonex python-dotenv

3. Run::

       python examples/01_say_one_thing.py

4. Open http://localhost:7860/client/ and click "Connect".

Environment variables
---------------------
SONEX_API_KEY       SonexLabs API key (vsk_...)
SONEX_VOICE_ID      Voice ID from GET /v1/voices (required)
"""

import os

from dotenv import load_dotenv
from loguru import logger

from pipecat.frames.frames import EndFrame, TTSSpeakFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineWorker
from pipecat.runner.types import RunnerArguments
from pipecat.runner.utils import create_transport
from pipecat.transports.base_transport import BaseTransport, TransportParams
from pipecat.workers.runner import WorkerRunner

from pipecat_sonex import SonexTTSService

load_dotenv(override=True)

transport_params = {
    "webrtc": lambda: TransportParams(audio_out_enabled=True),
}


async def run_bot(transport: BaseTransport, runner_args: RunnerArguments):
    logger.info("Starting bot")

    tts = SonexTTSService(
        api_key=os.environ["SONEX_API_KEY"],
        voice=os.environ["SONEX_VOICE_ID"],
    )

    worker = PipelineWorker(
        Pipeline([tts, transport.output()]),
        idle_timeout_secs=runner_args.pipeline_idle_timeout_secs,
    )

    @transport.event_handler("on_client_connected")
    async def on_client_connected(transport, client):
        await worker.queue_frames([TTSSpeakFrame("Hello there!"), EndFrame()])

    runner = WorkerRunner(handle_sigint=runner_args.handle_sigint)
    await runner.add_workers(worker)
    await runner.run()


async def bot(runner_args: RunnerArguments):
    """Main bot entry point compatible with Pipecat Cloud."""
    transport = await create_transport(runner_args, transport_params)
    await run_bot(transport, runner_args)


if __name__ == "__main__":
    from pipecat.runner.run import main

    main()
