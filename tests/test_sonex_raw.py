"""
Standalone SonexLabs TTS test — bypasses pipecat entirely.

CHANGED (2026-08-01): corrected endpoint path (/v1/speech, not
/v1/audio/speech) and request field names (text/voice_id/output_format,
not input/voice/response_format) to match SonexLabs' documented API.

Sends one request directly to the Sonex API and saves the raw response to
test_output.wav. Play that file in a normal media player to confirm Sonex's
API itself returns clean audio, independent of pipecat.

Usage:
    python tests/test_sonex_raw.py
"""

import os

import requests
from dotenv import load_dotenv

load_dotenv()

api_key = os.environ["SONEX_API_KEY"]
voice_id = os.environ["SONEX_VOICE_ID"]

response = requests.post(
    "https://api.sonexlabs.com/v1/speech",
    json={
        "text": "Hello, this is a test of the text to speech system.",
        "voice_id": voice_id,
        "output_format": "wav",
        "speed": 1.0,
    },
    headers={
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "audio/wav",
    },
)

print("Status:", response.status_code)
print("Content-Type:", response.headers.get("content-type"))
print("Bytes received:", len(response.content))

if response.status_code >= 400:
    print("Error response body:", response.text[:500])
else:
    with open("test_output.wav", "wb") as f:
        f.write(response.content)
    print("Saved to test_output.wav")

    header = response.content[:64]
    print("First 64 bytes (hex):", header.hex())
    if response.content[:4] == b"RIFF" and response.content[8:12] == b"WAVE":
        print("Looks like a valid RIFF/WAVE file.")
    else:
        print("WARNING: response does not start with a RIFF/WAVE header — "
              "this may not be a valid WAV file at all.")