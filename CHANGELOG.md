# Changelog

All notable changes to `pipecat-sonex` are documented here.

## [0.2.7] - 2026-08-10
### Added
- Maintainer attribution in the README (SonexLabs).
### Changed
- Condensed changelog history into higher-level entries.

## [0.2.6] - 2026-08-09
### Added
- `CHANGELOG.md`.
- Foundational single-file example (`examples/01_say_one_thing.py`).
- "Tested with Pipecat v1.7.0" compatibility note in the README.

## [0.2.1 – 0.2.5] - 2026-08-09
### Changed
- Simplified voice and language configuration examples throughout the docs.
- General documentation polish and clarity improvements.

## [0.2.0] - 2026-08-09
### Changed
- Rewrote `SonexTTSService` to extend pipecat's `TTSService` base class directly (previously a standalone processor).
- Switched to the `/v1/speech/stream` endpoint for lower time-to-first-audio.
- Connections are pooled and reused via a shared `aiohttp.ClientSession` with `TCPConnector` keep-alive.

## [0.1.0] - 2026-08-01
### Added
- Initial release as `PaniniStreamingTTSProcessor`.

### Added
- Initial release as `PaniniStreamingTTSProcessor`.
