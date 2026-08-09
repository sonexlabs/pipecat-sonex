# Changelog

All notable changes to `pipecat-sonex` are documented here.

## [0.2.6] - 2026-08-09
### Added
- `CHANGELOG.md`.
- Foundational single-file example (`examples/01_say_one_thing.py`).
- "Tested with Pipecat v1.7.0" compatibility note in the README.

## [0.2.5] - 2026-08-09
### Changed
- Removed all language-code examples (`"en"`, `"hi"`, etc.) from docs — `language` is optional and defaults to auto-detect.

## [0.2.4] - 2026-08-09
### Fixed
- Removed `sample_rate` from the basic quickstart example (default already fits WebRTC); kept it only in the telephony example where it's a meaningful override.
- Fixed a voice/language mismatch in the telephony docstring example.

## [0.2.3] - 2026-08-09
### Fixed
- Removed two scratch test scripts that were accidentally committed.
- Fixed a voice/language mismatch in the quickstart example.

## [0.2.2] - 2026-08-09
### Fixed
- Replaced a second fabricated `voice_id` (`hi-IN-female-1`) with a real ID from `GET /v1/voices`.

## [0.2.1] - 2026-08-09
### Fixed
- Replaced fabricated `voice_id` examples (e.g. `en-US-male-1`) with real IDs from `GET /v1/voices`.
- Dropped BCP-47 jargon from language parameter docs.

## [0.2.0] - 2026-08-09
### Changed
- Rewrote `SonexTTSService` to extend pipecat's `TTSService` base class directly (previously a standalone processor).
- Switched to the `/v1/speech/stream` endpoint for lower time-to-first-audio.
- Connections are pooled and reused via a shared `aiohttp.ClientSession` with `TCPConnector` keep-alive.

## [0.1.0] - 2026-08-01
### Added
- Initial release as `PaniniStreamingTTSProcessor`.
