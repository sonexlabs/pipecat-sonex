"""
Minimal test — verifies PaniniStreamingTTSProcessor can be imported and constructed.
Does NOT make live HTTP requests.
"""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock


def test_import():
    from pipecat_sonex import PaniniStreamingTTSProcessor
    assert PaniniStreamingTTSProcessor is not None


def test_requires_endpoints():
    from pipecat_sonex import PaniniStreamingTTSProcessor
    with pytest.raises(ValueError, match="endpoints"):
        PaniniStreamingTTSProcessor(api_token="vsk_test", endpoints=[])


def test_constructor_defaults():
    from pipecat_sonex import PaniniStreamingTTSProcessor
    p = PaniniStreamingTTSProcessor(
        api_token="vsk_test",
        endpoints=["http://localhost:8000"],
    )
    assert p._model == "panini"
    assert p._voice == "auto"
    assert p._num_step == 16
    assert p._speed == 1.0
    assert p._sample_rate == 24000


def test_sanitize_markdown():
    from pipecat_sonex import PaniniStreamingTTSProcessor
    p = PaniniStreamingTTSProcessor(
        api_token="vsk_test",
        endpoints=["http://localhost:8000"],
    )
    assert p._sanitize_tts_text("**Hello** _world_") == "Hello world"
    assert p._sanitize_tts_text("[click here](https://example.com)") == "click here"


def test_is_speakable():
    from pipecat_sonex import PaniniStreamingTTSProcessor
    p = PaniniStreamingTTSProcessor(
        api_token="vsk_test",
        endpoints=["http://localhost:8000"],
    )
    assert p._is_speakable_text("Hello world") is True
    assert p._is_speakable_text("   ") is False
    assert p._is_speakable_text("...") is False


def test_endpoint_rotation():
    from pipecat_sonex import PaniniStreamingTTSProcessor
    p = PaniniStreamingTTSProcessor(
        api_token="vsk_test",
        endpoints=["http://a:8000", "http://b:8000"],
    )
    assert p._get_next_endpoint() == "http://a:8000"
    assert p._get_next_endpoint() == "http://b:8000"
    assert p._get_next_endpoint() == "http://a:8000"  # wraps
