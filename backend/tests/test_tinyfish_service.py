import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from market_monitor_api.config import get_settings
from market_monitor_api.services.tinyfish import run_ad_hoc_tinyfish_research


def build_settings(root: Path):
    env_file = root / ".env"
    env_file.write_text(
        "\n".join(
            [
                "MARKET_MONITOR_APP_NAME=AI Market Sentry Platform",
                f"MARKET_MONITOR_SOURCE_CONFIG_FILE={root / 'sources.json'}",
                f"MARKET_MONITOR_SNAPSHOT_STORE_DIR={root / 'snapshots'}",
                "PRODUCT_VIABILITY_MAX_IMAGES=4",
                "PRODUCT_VIABILITY_MAX_IMAGE_BYTES=1024",
                "TINYFISH_BASE_URL=https://agent.tinyfish.ai",
                "TINYFISH_API_KEY=test-tinyfish-key",
                "TINYFISH_TIMEOUT_SECONDS=30",
            ]
        )
    )
    with patch.dict(os.environ, {}, clear=True):
        get_settings.cache_clear()
        settings = get_settings(root)
        get_settings.cache_clear()
    return settings


class FakeHTTPResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def read(self):
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class TinyFishServiceTests(unittest.TestCase):
    def tearDown(self):
        get_settings.cache_clear()

    def test_run_ad_hoc_tinyfish_research_polls_until_completed(self):
        settings = build_settings(Path(tempfile.mkdtemp()))
        research_spec = {
            "lane": "standard",
            "target_url": "https://duckduckgo.com/?q=portable+espresso",
            "goal": "Research portable espresso makers.",
            "output_schema": {
                "type": "object",
                "required": ["summary", "competitors", "pricing_landscape", "demand_signals", "risks", "source_citations"],
                "properties": {
                    "summary": {"type": "string"},
                    "competitors": {"type": "array", "items": {"type": "object"}},
                    "pricing_landscape": {"type": "array", "items": {"type": "string"}},
                    "demand_signals": {"type": "array", "items": {"type": "string"}},
                    "risks": {"type": "array", "items": {"type": "string"}},
                    "source_citations": {"type": "array", "items": {"type": "object"}},
                },
            },
            "stop_conditions": [],
            "error_handling": {},
            "browser_profile": "lite",
            "use_vault": False,
            "credential_item_ids": [],
        }
        completed_run = {
            "run_id": "run-123",
            "status": "COMPLETED",
            "started_at": "2026-03-28T00:00:00Z",
            "finished_at": "2026-03-28T00:00:05Z",
            "result": {
                "summary": "Found live competitor and pricing evidence.",
                "competitors": [{"name": "Rival Brewer", "url": "https://example.com/rival", "summary": "Portable brewer"}],
                "pricing_landscape": ["Most products fall between $69 and $99."],
                "demand_signals": ["Portable espresso gets recurring interest in travel forums."],
                "risks": ["Crowded category."],
                "source_citations": [{"title": "Rival Brewer", "url": "https://example.com/rival"}],
            },
            "error": None,
        }

        with patch(
            "market_monitor_api.services.tinyfish.urlopen",
            side_effect=[
                FakeHTTPResponse({"run_id": "run-123", "status": "RUNNING"}),
                FakeHTTPResponse({"data": [completed_run], "not_found": None}),
            ],
        ) as mock_urlopen, patch("market_monitor_api.services.tinyfish.time.sleep", return_value=None):
            result = run_ad_hoc_tinyfish_research(settings, research_spec)

        self.assertEqual(mock_urlopen.call_count, 2)
        self.assertEqual(result["status"], "COMPLETED")
        self.assertEqual(result["result"]["summary"], completed_run["result"]["summary"])

    def test_run_ad_hoc_tinyfish_research_returns_pending_after_wait_window(self):
        settings = build_settings(Path(tempfile.mkdtemp()))
        settings = settings.__class__(**{**settings.__dict__, "tinyfish_timeout_seconds": 1})
        research_spec = {
            "lane": "standard",
            "target_url": "https://duckduckgo.com/?q=portable+espresso",
            "goal": "Research portable espresso makers.",
            "output_schema": {"type": "object", "properties": {}},
            "stop_conditions": [],
            "error_handling": {},
            "browser_profile": "lite",
            "use_vault": False,
            "credential_item_ids": [],
        }

        with patch(
            "market_monitor_api.services.tinyfish.urlopen",
            side_effect=[
                FakeHTTPResponse({"run_id": "run-123", "error": None}),
                FakeHTTPResponse(
                    {
                        "data": [
                            {
                                "run_id": "run-123",
                                "status": "PENDING",
                                "started_at": None,
                                "finished_at": None,
                                "num_of_steps": None,
                                "result": None,
                                "error": None,
                            }
                        ],
                        "not_found": None,
                    }
                ),
            ],
        ), patch(
            "market_monitor_api.services.tinyfish.time.monotonic",
            side_effect=[0, 0, 2],
        ), patch(
            "market_monitor_api.services.tinyfish.time.sleep",
            return_value=None,
        ):
            result = run_ad_hoc_tinyfish_research(settings, research_spec)

        self.assertEqual(result["status"], "PENDING")
        self.assertEqual(result["error"]["code"], "still_running")


if __name__ == "__main__":
    unittest.main()
