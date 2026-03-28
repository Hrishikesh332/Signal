import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from werkzeug.datastructures import FileMultiDict, MultiDict


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from market_monitor_api.config import get_settings
from market_monitor_api.services.openai_service import (
    build_product_viability_request_payload,
    normalize_product_viability_decision,
    run_product_viability_analysis,
)
from market_monitor_api.services.product_viability import (
    ProductViabilityError,
    ProductViabilityImage,
    ProductViabilityInput,
    build_product_viability_enrichment,
    build_product_viability_live_research,
    build_product_viability_response,
    build_research_query,
    build_standard_research_spec,
    parse_product_viability_input,
)


def build_settings(
    root: Path,
    source_config: Path | None = None,
    snapshot_dir: Path | None = None,
    include_openai: bool = True,
):
    env_file = root / ".env"
    env_lines = [
        "MARKET_MONITOR_APP_NAME=AI Market Sentry Platform",
        f"MARKET_MONITOR_SOURCE_CONFIG_FILE={source_config or (root / 'sources.json')}",
        f"MARKET_MONITOR_SNAPSHOT_STORE_DIR={snapshot_dir or (root / 'snapshots')}",
        "PRODUCT_VIABILITY_MAX_IMAGES=4",
        "PRODUCT_VIABILITY_MAX_IMAGE_BYTES=1024",
        "TINYFISH_BASE_URL=https://agent.tinyfish.ai",
        "TINYFISH_API_KEY=test-tinyfish-key",
        "TINYFISH_TIMEOUT_SECONDS=30",
    ]
    if include_openai:
        env_lines.extend(
            [
                "OPENAI_BASE_URL=https://api.openai.com/v1",
                "OPENAI_API_KEY=test-key",
                "OPENAI_MODEL=gpt-4.1-mini",
                "OPENAI_TIMEOUT_SECONDS=30",
            ]
        )
    env_file.write_text("\n".join(env_lines))
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


class ProductViabilityServiceTests(unittest.TestCase):
    def tearDown(self):
        get_settings.cache_clear()

    def test_multimodal_request_payload_includes_text_and_images(self):
        settings = build_settings(Path(tempfile.mkdtemp()))
        payload = {
            "natural_language_input": "Is there a viable market for a portable espresso maker for travelers?",
            "product_name": "Pocket Brewer",
            "description": "Portable espresso device",
            "category": "consumer hardware",
            "price_point": "$79",
            "target_customer": "travelers",
            "market_context": "crowded coffee accessories market",
            "research_depth": "deep",
            "images": [
                {
                    "filename": "concept.png",
                    "content_type": "image/png",
                    "size_bytes": 5,
                    "data_url": "data:image/png;base64,abc",
                }
            ],
        }
        request_payload = build_product_viability_request_payload(
            payload,
            {
                "live_market_research": {"summary": "TinyFish found active competitors."},
                "local_signal_context": {"used_local_context": False},
            },
            settings,
        )

        user_content = request_payload["input"][1]["content"]
        self.assertEqual(request_payload["model"], "gpt-4.1-mini")
        self.assertEqual(user_content[0]["type"], "input_text")
        self.assertEqual(user_content[1]["type"], "input_image")
        self.assertEqual(user_content[1]["image_url"], "data:image/png;base64,abc")
        prompt_document = json.loads(user_content[0]["text"])
        self.assertEqual(prompt_document["submitted_product"]["research_depth"], "deep")
        self.assertEqual(
            prompt_document["submitted_product"]["natural_language_input"],
            "Is there a viable market for a portable espresso maker for travelers?",
        )
        self.assertEqual(prompt_document["live_market_research"]["summary"], "TinyFish found active competitors.")

    def test_normalize_product_viability_decision_normalizes_scores_and_sources(self):
        normalized = normalize_product_viability_decision(
            {
                "summary": "Promising wedge for specialty coffee fans.",
                "viability_score": 0.82,
                "recommendation": "strong_yes",
                "target_customer": "Frequent travelers",
                "strengths": ["Portable", "Distinctive ritual"],
                "risks": ["Crowded market"],
                "differentiation": "Focuses on premium portable espresso.",
                "pricing_fit": "Premium but plausible.",
                "demand_signals": ["Search interest in travel coffee gear"],
                "next_validation_steps": ["Run landing page test"],
                "confidence_score": 84,
                "analysis_sources": ["user_description", "user_images", "tinyfish_live_research", "invalid"],
            }
        )

        self.assertEqual(normalized["viability_score"], 82)
        self.assertEqual(normalized["confidence_score"], 0.84)
        self.assertEqual(
            normalized["analysis_sources"],
            ["user_description", "user_images", "tinyfish_live_research"],
        )

    def test_run_product_viability_analysis_rejects_invalid_json(self):
        settings = build_settings(Path(tempfile.mkdtemp()))
        api_payload = {
            "output": [
                {
                    "content": [
                        {
                            "type": "output_text",
                            "text": "not-json",
                        }
                    ]
                }
            ]
        }

        with patch(
            "market_monitor_api.services.openai_service.urlopen",
            return_value=FakeHTTPResponse(api_payload),
        ):
            with self.assertRaises(ValueError):
                run_product_viability_analysis(
                    {"description": "AI note-taking app", "images": [], "research_depth": "standard"},
                    {"live_market_research": {"summary": "TinyFish evidence"}, "local_signal_context": {}},
                    settings,
                )

    def test_parse_product_viability_input_defaults_research_depth_to_standard(self):
        settings = build_settings(Path(tempfile.mkdtemp()))
        payload = parse_product_viability_input(
            MultiDict({"description": "AI note-taking app"}),
            FileMultiDict(),
            settings,
        )

        self.assertEqual(payload.research_depth, "standard")

    def test_parse_product_viability_input_does_not_require_openai(self):
        settings = build_settings(Path(tempfile.mkdtemp()), include_openai=False)

        payload = parse_product_viability_input(
            MultiDict({"description": "AI note-taking app"}),
            FileMultiDict(),
            settings,
        )

        self.assertEqual(payload.description, "AI note-taking app")

    def test_parse_product_viability_input_accepts_natural_language_only(self):
        settings = build_settings(Path(tempfile.mkdtemp()))
        payload = parse_product_viability_input(
            MultiDict({"query": "Would an AI note-taking app for students be commercially viable?"}),
            FileMultiDict(),
            settings,
        )

        self.assertEqual(
            payload.natural_language_input,
            "Would an AI note-taking app for students be commercially viable?",
        )
        self.assertEqual(
            payload.description,
            "Would an AI note-taking app for students be commercially viable?",
        )

    def test_parse_product_viability_input_rejects_invalid_research_depth(self):
        settings = build_settings(Path(tempfile.mkdtemp()))

        with self.assertRaises(ProductViabilityError):
            parse_product_viability_input(
                MultiDict({"description": "AI note-taking app", "research_depth": "extreme"}),
                FileMultiDict(),
                settings,
            )

    def test_standard_live_research_uses_one_tinyfish_run(self):
        settings = build_settings(Path(tempfile.mkdtemp()))
        payload = ProductViabilityInput(
            natural_language_input="Is there demand for a portable espresso device for travelers?",
            product_name="Pocket Brewer",
            description="Portable espresso device",
            category="consumer hardware",
            price_point="$79",
            target_customer="travelers",
            market_context="crowded coffee accessories market",
            research_depth="standard",
            images=[],
        )

        with patch(
            "market_monitor_api.services.product_viability.run_ad_hoc_tinyfish_research",
            return_value={
                "lane": "standard",
                "status": "COMPLETED",
                "result": {
                    "summary": "Found a few direct competitors and price anchors.",
                    "competitors": [{"name": "Rival Brewer", "url": "https://example.com/rival", "summary": "Portable brewer"}],
                    "pricing_landscape": ["Most products appear between $69 and $99."],
                    "demand_signals": ["Travel coffee gear shows recurring interest in forums."],
                    "risks": ["Crowded portable coffee category."],
                    "source_citations": [{"title": "Rival Brewer", "url": "https://example.com/rival"}],
                },
                "error": None,
            },
        ) as mock_run:
            research = build_product_viability_live_research(settings, payload)

        self.assertEqual(mock_run.call_count, 1)
        self.assertEqual(research["status"], "completed")
        self.assertEqual(research["lane_statuses"], [{"lane": "standard", "status": "completed"}])

    def test_research_query_is_simplified_from_natural_language_input(self):
        payload = ProductViabilityInput(
            natural_language_input=(
                "Would a portable espresso maker for travelers and campers be commercially viable "
                "at around $79 for coffee enthusiasts?"
            ),
            product_name="",
            description="Portable espresso device for travelers and campers",
            category="consumer hardware",
            price_point="$79",
            target_customer="travelers and campers",
            market_context="",
            research_depth="standard",
            images=[],
        )

        query = build_research_query(payload, "competitors pricing demand customer pain points")

        self.assertIn("consumer", query)
        self.assertIn("hardware", query)
        self.assertIn("travelers", query)
        self.assertIn("79", query)
        self.assertNotIn("would", query)
        self.assertNotIn("commercially", query)
        self.assertLessEqual(len(query.split()), 8)

    def test_standard_research_uses_duckduckgo_target_url(self):
        payload = ProductViabilityInput(
            natural_language_input="Would a portable espresso maker be viable?",
            product_name="",
            description="Portable espresso maker",
            category="consumer hardware",
            price_point="$79",
            target_customer="travelers",
            market_context="",
            research_depth="standard",
            images=[],
        )

        spec = build_standard_research_spec(payload)

        self.assertTrue(spec["target_url"].startswith("https://duckduckgo.com/?q="))

    def test_deep_live_research_handles_partial_lane_failure(self):
        settings = build_settings(Path(tempfile.mkdtemp()))
        payload = ProductViabilityInput(
            natural_language_input="Is there demand for a portable espresso device for travelers?",
            product_name="Pocket Brewer",
            description="Portable espresso device",
            category="consumer hardware",
            price_point="$79",
            target_customer="travelers",
            market_context="crowded coffee accessories market",
            research_depth="deep",
            images=[],
        )

        side_effect = [
            {
                "lane": "competitors",
                "status": "COMPLETED",
                "result": {
                    "summary": "Direct competitor found.",
                    "competitors": [{"name": "Rival Brewer", "url": "https://example.com/rival", "summary": "Portable brewer"}],
                    "pricing_landscape": [],
                    "demand_signals": [],
                    "risks": [],
                    "source_citations": [{"title": "Rival Brewer", "url": "https://example.com/rival"}],
                },
                "error": None,
            },
            {
                "lane": "pricing",
                "status": "FAILED",
                "result": None,
                "error": {"code": "timeout", "message": "Timed out"},
            },
            {
                "lane": "demand",
                "status": "COMPLETED",
                "result": {
                    "summary": "Community demand signals appear in travel coffee forums.",
                    "competitors": [],
                    "pricing_landscape": [],
                    "demand_signals": ["Portable espresso discussed repeatedly in travel coffee threads."],
                    "risks": ["Demand may be niche."],
                    "source_citations": [{"title": "Travel Coffee Forum", "url": "https://example.com/forum"}],
                },
                "error": None,
            },
        ]

        with patch(
            "market_monitor_api.services.product_viability.run_ad_hoc_tinyfish_research",
            side_effect=side_effect,
        ) as mock_run:
            research = build_product_viability_live_research(settings, payload)

        self.assertEqual(mock_run.call_count, 3)
        self.assertEqual(research["status"], "partial")
        self.assertEqual(len(research["lane_reports"]), 3)

    def test_deep_live_research_raises_when_all_lanes_fail(self):
        settings = build_settings(Path(tempfile.mkdtemp()))
        payload = ProductViabilityInput(
            natural_language_input="Is there demand for a portable espresso device for travelers?",
            product_name="Pocket Brewer",
            description="Portable espresso device",
            category="consumer hardware",
            price_point="$79",
            target_customer="travelers",
            market_context="crowded coffee accessories market",
            research_depth="deep",
            images=[],
        )

        with patch(
            "market_monitor_api.services.product_viability.run_ad_hoc_tinyfish_research",
            side_effect=[
                {"lane": "competitors", "status": "FAILED", "result": None, "error": {"code": "timeout", "message": "Timed out"}},
                {"lane": "pricing", "status": "FAILED", "result": None, "error": {"code": "timeout", "message": "Timed out"}},
                {"lane": "demand", "status": "FAILED", "result": None, "error": {"code": "timeout", "message": "Timed out"}},
            ],
        ):
            with self.assertRaises(ProductViabilityError):
                build_product_viability_live_research(settings, payload)

    def test_standard_live_research_returns_pending_when_tinyfish_is_still_running(self):
        settings = build_settings(Path(tempfile.mkdtemp()))
        payload = ProductViabilityInput(
            natural_language_input="Would a portable espresso maker for campers be viable?",
            product_name="Pocket Brewer",
            description="Portable espresso maker for travelers and campers",
            category="consumer hardware",
            price_point="$79",
            target_customer="travelers",
            market_context="",
            research_depth="standard",
            images=[],
        )

        with patch(
            "market_monitor_api.services.product_viability.run_ad_hoc_tinyfish_research",
            return_value={
                "lane": "standard",
                "status": "PENDING",
                "result": None,
                "error": {"code": "still_running", "message": "TinyFish run is still pending."},
            },
        ):
            research = build_product_viability_live_research(settings, payload)

        self.assertEqual(research["status"], "pending")
        self.assertEqual(research["lane_statuses"], [{"lane": "standard", "status": "pending"}])

    def test_build_product_viability_response_uses_tinyfish_decision_when_openai_not_configured(self):
        settings = build_settings(Path(tempfile.mkdtemp()), include_openai=False)
        payload = ProductViabilityInput(
            natural_language_input="Would a portable espresso maker for campers be viable?",
            product_name="Pocket Brewer",
            description="Portable espresso maker for travelers and campers",
            category="consumer hardware",
            price_point="$79",
            target_customer="travelers",
            market_context="",
            research_depth="standard",
            images=[],
        )
        live_market_research = {
            "status": "completed",
            "lane_statuses": [{"lane": "standard", "status": "completed"}],
            "summary": "Portable espresso makers show active demand and established competitors.",
            "competitors": [
                {
                    "name": "Wacaco Nanopresso",
                    "url": "https://example.com/nanopresso",
                    "summary": "Popular portable espresso competitor.",
                    "price_point": "$69.90",
                }
            ],
            "pricing_landscape": ["Mid-range manual espresso makers appear between $60 and $85."],
            "demand_signals": ["Outdoor coffee communities discuss portable espresso often."],
            "risks": ["Ease of use matters in a crowded manual brewer segment."],
            "source_citations": [{"title": "Portable Espresso Guide", "url": "https://example.com/guide"}],
            "lane_reports": [],
        }
        local_signal_context = {
            "matched_entities": {"companies": [], "products": []},
            "related_signal_context": {"growth_events_count": 0, "commerce_signals_count": 0, "growth_events": [], "commerce_signals": []},
            "used_local_context": False,
        }

        with patch(
            "market_monitor_api.services.product_viability.build_product_viability_live_research",
            return_value=live_market_research,
        ), patch(
            "market_monitor_api.services.product_viability.build_product_viability_enrichment",
            return_value=local_signal_context,
        ), patch(
            "market_monitor_api.services.product_viability.run_product_viability_analysis",
        ) as mock_openai, patch(
            "market_monitor_api.services.product_viability.LOGGER.info",
        ) as mock_logger:
            response = build_product_viability_response(settings, payload)

        mock_openai.assert_not_called()
        mock_logger.assert_called_once()
        self.assertEqual(response["meta"]["decision_provider"], "TinyFish")
        self.assertEqual(response["meta"]["openai_status"], "not_configured")
        self.assertEqual(response["summary"], live_market_research["summary"])
        self.assertEqual(response["recommendation"], "unclear")
        self.assertEqual(response["competitors"][0]["name"], "Wacaco Nanopresso")
        self.assertEqual(response["sources"][0]["title"], "Portable Espresso Guide")
        self.assertIn("tinyfish_live_research", mock_logger.call_args[0][1])

    def test_build_product_viability_response_falls_back_when_openai_fails(self):
        settings = build_settings(Path(tempfile.mkdtemp()))
        payload = ProductViabilityInput(
            natural_language_input="Would a portable espresso maker for campers be viable?",
            product_name="Pocket Brewer",
            description="Portable espresso maker for travelers and campers",
            category="consumer hardware",
            price_point="$79",
            target_customer="travelers",
            market_context="",
            research_depth="standard",
            images=[
                ProductViabilityImage(
                    filename="concept.png",
                    content_type="image/png",
                    size_bytes=4,
                    data_url="data:image/png;base64,abcd",
                )
            ],
        )
        live_market_research = {
            "status": "completed",
            "lane_statuses": [{"lane": "standard", "status": "completed"}],
            "summary": "Portable espresso makers show active demand and established competitors.",
            "competitors": [],
            "pricing_landscape": ["Mid-range manual espresso makers appear between $60 and $85."],
            "demand_signals": ["Outdoor coffee communities discuss portable espresso often."],
            "risks": ["Ease of use matters in a crowded manual brewer segment."],
            "source_citations": [{"title": "Portable Espresso Guide", "url": "https://example.com/guide"}],
            "lane_reports": [],
        }
        local_signal_context = {
            "matched_entities": {"companies": [], "products": []},
            "related_signal_context": {"growth_events_count": 0, "commerce_signals_count": 0, "growth_events": [], "commerce_signals": []},
            "used_local_context": False,
        }

        with patch(
            "market_monitor_api.services.product_viability.build_product_viability_live_research",
            return_value=live_market_research,
        ), patch(
            "market_monitor_api.services.product_viability.build_product_viability_enrichment",
            return_value=local_signal_context,
        ), patch(
            "market_monitor_api.services.product_viability.run_product_viability_analysis",
            side_effect=ValueError("OpenAI product viability analysis is unavailable."),
        ), patch(
            "market_monitor_api.services.product_viability.LOGGER.info",
        ):
            response = build_product_viability_response(settings, payload)

        self.assertEqual(response["meta"]["decision_provider"], "TinyFish")
        self.assertEqual(response["meta"]["openai_status"], "failed")
        self.assertEqual(response["summary"], live_market_research["summary"])
        self.assertEqual(
            response["highlights"]["pricing_fit"],
            "$79 should be tested against this market evidence: Mid-range manual espresso makers appear between $60 and $85.",
        )

    def test_build_product_viability_response_returns_pending_without_decision_memo(self):
        settings = build_settings(Path(tempfile.mkdtemp()), include_openai=False)
        payload = ProductViabilityInput(
            natural_language_input="Would a portable espresso maker for campers be viable?",
            product_name="Pocket Brewer",
            description="Portable espresso maker for travelers and campers",
            category="consumer hardware",
            price_point="$79",
            target_customer="travelers",
            market_context="",
            research_depth="standard",
            images=[],
        )
        live_market_research = {
            "status": "pending",
            "lane_statuses": [{"lane": "standard", "status": "pending"}],
            "summary": "",
            "competitors": [],
            "pricing_landscape": [],
            "demand_signals": [],
            "risks": [],
            "source_citations": [],
            "lane_reports": [
                {
                    "lane": "standard",
                    "status": "pending",
                    "summary": None,
                    "competitors_count": 0,
                    "pricing_points_count": 0,
                    "demand_signals_count": 0,
                    "risks_count": 0,
                    "citations_count": 0,
                    "error": {"code": "still_running", "message": "TinyFish run is still pending."},
                }
            ],
        }
        local_signal_context = {
            "matched_entities": {"companies": [], "products": []},
            "related_signal_context": {"growth_events_count": 0, "commerce_signals_count": 0, "growth_events": [], "commerce_signals": []},
            "used_local_context": False,
        }

        with patch(
            "market_monitor_api.services.product_viability.build_product_viability_live_research",
            return_value=live_market_research,
        ), patch(
            "market_monitor_api.services.product_viability.build_product_viability_enrichment",
            return_value=local_signal_context,
        ), patch(
            "market_monitor_api.services.product_viability.LOGGER.info",
        ):
            response = build_product_viability_response(settings, payload)

        self.assertEqual(response["meta"]["research_status"], "pending")
        self.assertEqual(response["meta"]["decision_status"], "pending")
        self.assertEqual(response["status"], "pending")
        self.assertEqual(response["summary"], "TinyFish research is still running.")
        self.assertIsNone(response["recommendation"])
        self.assertEqual(response["competitors"], [])

    def test_enrichment_matches_existing_company_and_returns_related_signals(self):
        temp_root = Path(tempfile.mkdtemp())
        source_config = temp_root / "sources.json"
        snapshot_root = temp_root / "snapshots" / "source-commerce-portable-brewer"
        snapshot_root.mkdir(parents=True)
        source_config.write_text(
            json.dumps(
                {
                    "sources": [
                        {
                            "id": "source-commerce-portable-brewer",
                            "name": "OpenAI Portable Brewer Store",
                            "category": "commerce_intelligence",
                            "company_id": "company-openai",
                            "company_name": "OpenAI",
                            "product_id": "product-brewer",
                            "product_name": "Pocket Brewer",
                            "target_url": "https://example.com/brewer",
                            "goal": "Monitor price changes.",
                            "output_schema": {"type": "object"},
                            "stop_conditions": [],
                            "error_handling": {},
                            "browser_profile": "lite",
                            "sku": "BREWER-1",
                            "marketplace": "web",
                            "tracking_group_id": "tg-brewer",
                            "schedule": {"interval_minutes": 60},
                        }
                    ]
                }
            )
        )
        first_snapshot = {
            "snapshot_id": "snapshot-1",
            "captured_at": "2026-03-01T00:00:00Z",
            "capture_status": "COMPLETED",
            "source_id": "source-commerce-portable-brewer",
            "source_name": "OpenAI Portable Brewer Store",
            "category": "commerce_intelligence",
            "company_id": "company-openai",
            "company_name": "OpenAI",
            "product_id": "product-brewer",
            "product_name": "Pocket Brewer",
            "target_url": "https://example.com/brewer",
            "goal": "Monitor price changes.",
            "output_schema": {"type": "object"},
            "stop_conditions": [],
            "error_handling": {},
            "run": {},
            "result": {
                "captured_at": "2026-03-01T00:00:00Z",
                "price": 99.0,
                "discount_percent": 0.0,
                "stock_status": "in_stock",
                "seller": "OpenAI Store",
                "product_name": "Pocket Brewer",
                "currency": "USD",
            },
            "validation_errors": [],
        }
        second_snapshot = {
            **first_snapshot,
            "snapshot_id": "snapshot-2",
            "captured_at": "2026-03-02T00:00:00Z",
            "result": {
                **first_snapshot["result"],
                "captured_at": "2026-03-02T00:00:00Z",
                "price": 79.0,
                "discount_percent": 20.0,
            },
        }
        (snapshot_root / "snapshot-1.json").write_text(json.dumps(first_snapshot))
        (snapshot_root / "snapshot-2.json").write_text(json.dumps(second_snapshot))

        settings = build_settings(temp_root, source_config=source_config, snapshot_dir=temp_root / "snapshots")
        payload = ProductViabilityInput(
            natural_language_input="Analyze whether Pocket Brewer is commercially viable.",
            product_name="Pocket Brewer",
            description="OpenAI portable brewer for travelers",
            category="consumer hardware",
            price_point="$79",
            target_customer="travelers",
            market_context="premium coffee gear",
            research_depth="standard",
            images=[ProductViabilityImage("concept.png", "image/png", 5, "data:image/png;base64,abc")],
        )

        enrichment = build_product_viability_enrichment(settings, payload)

        self.assertTrue(enrichment["used_local_context"])
        self.assertEqual(enrichment["matched_entities"]["companies"][0]["company_id"], "company-openai")
        self.assertEqual(enrichment["matched_entities"]["products"][0]["product_id"], "product-brewer")
        self.assertGreaterEqual(enrichment["related_signal_context"]["commerce_signals_count"], 1)

    def test_enrichment_without_matches_returns_empty_context(self):
        temp_root = Path(tempfile.mkdtemp())
        settings = build_settings(temp_root)
        payload = ProductViabilityInput(
            natural_language_input="Would a brand-new gadget for students be viable?",
            product_name="Unknown Gadget",
            description="A brand-new gadget with no tracked references",
            category="hardware",
            price_point="$49",
            target_customer="students",
            market_context="new category",
            research_depth="standard",
            images=[],
        )

        enrichment = build_product_viability_enrichment(settings, payload)

        self.assertFalse(enrichment["used_local_context"])
        self.assertEqual(enrichment["matched_entities"]["companies"], [])
        self.assertEqual(enrichment["related_signal_context"]["growth_events"], [])
        self.assertEqual(enrichment["related_signal_context"]["commerce_signals"], [])

    def test_settings_are_loaded_from_env_file(self):
        settings = build_settings(Path(tempfile.mkdtemp()))

        self.assertEqual(settings.openai_model, "gpt-4.1-mini")
        self.assertEqual(settings.tinyfish_api_key, "test-tinyfish-key")
        self.assertEqual(settings.product_viability_max_images, 4)


if __name__ == "__main__":
    unittest.main()
