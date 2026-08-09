"""Full pipeline test of pipecat-sonex's SonexTTSService using pipecat's own
run_test() harness -- this exercises the exact documented pipeline usage
("Add to your pipeline" section of the README) end-to-end against the real
SonexLabs API. Not part of the repo -- throwaway harness, no repo files modified.
"""
import asyncio
import wave

from pipecat.frames.frames import TextFrame, TTSAudioRawFrame, TTSStartedFrame, TTSStoppedFrame, ErrorFrame
from pipecat.tests.utils import run_test, SleepFrame

from pipecat_sonex import SonexTTSService


async def main():
    tts = SonexTTSService(
        api_key="vsk_1WHRBvQhZYyZIjAfMpP3oIfEctJYHKCravZCDrDTTlY",
        voice="72ly9crx9v",  # Alok
        language="en",
        sample_rate=24000,
    )

    down_frames, up_frames = await run_test(
        tts,
        frames_to_send=[TextFrame("Hello, this is a test of the SonexLabs pipecat plugin."), SleepFrame(3)],
        expected_down_frames=None,
    )

    audio = bytearray()
    sample_rate = 24000
    for f in down_frames:
        print("DOWN FRAME:", type(f).__name__, getattr(f, "error", ""))
        if isinstance(f, TTSAudioRawFrame):
            audio.extend(f.audio)
            sample_rate = f.sample_rate

    print(f"Collected {len(audio)} bytes of PCM audio at {sample_rate} Hz")
    if audio:
        with wave.open("pipecat_test_output.wav", "wb") as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)
            wf.setframerate(sample_rate)
            wf.writeframes(bytes(audio))
        print("Saved to pipecat_test_output.wav")
    else:
        print("NO AUDIO RECEIVED")


asyncio.run(main())
