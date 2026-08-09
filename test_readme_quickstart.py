"""Verbatim reproduction of the README Quick Start snippet for pipecat-sonex,
to confirm it works exactly as documented, against the real API.
Not part of the repo -- throwaway harness, does not modify any repo files.
"""
import asyncio
import wave

import asyncio
import wave

import aiohttp

from pipecat_sonex import SonexTTSService
from pipecat.frames.frames import TTSAudioRawFrame, ErrorFrame


async def main():
    tts = SonexTTSService(
        api_key="vsk_1WHRBvQhZYyZIjAfMpP3oIfEctJYHKCravZCDrDTTlY",
        voice="72ly9crx9v",  # Alok
        language="en",
        sample_rate=24000,
    )

    # Bypass the full pipecat TaskManager/FrameProcessor lifecycle (which needs
    # a real Pipeline/PipelineTask) and exercise SonexTTSService's actual HTTP
    # synthesis path (run_tts) directly against the real SonexLabs API, the
    # same way the README's "Constructor parameters" + core synthesis flow work.
    tts._http_session = aiohttp.ClientSession()
    tts._sample_rate = 24000  # normally set by start() from StartFrame.audio_out_sample_rate

    audio = bytearray()
    sample_rate = 24000
    async for frame in tts.run_tts("Hello, this is a test of the SonexLabs pipecat plugin.", context_id="test-1"):
        if isinstance(frame, ErrorFrame):
            print("ERROR FRAME:", frame.error)
        elif isinstance(frame, TTSAudioRawFrame):
            audio.extend(frame.audio)
            sample_rate = frame.sample_rate
        else:
            print("frame:", type(frame).__name__)

    await tts._http_session.close()

    print(f"Collected {len(audio)} bytes of PCM audio at {sample_rate} Hz")
    with wave.open("pipecat_test_output.wav", "wb") as f:
        f.setnchannels(1)
        f.setsampwidth(2)
        f.setframerate(sample_rate)
        f.writeframes(bytes(audio))
    print("Saved to pipecat_test_output.wav")


asyncio.run(main())
