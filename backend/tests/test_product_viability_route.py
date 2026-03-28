import os
import sys
import tempfile
import unittest
from io import BytesIO
from pathlib import Path
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from market_monitor_api import create_app
from market_monitor_api.config import get_settings


def build_settings_from_env():
    temp_root = Path(tempfile.mkdtemp())
    env_file = temp_root / ".env"
    env_file.write_text(
        "\n".join(
            [
                "MARKET_MONITOR_APP_NAME=AI Market Sentry Platform",
                f"MARKET_MONITOR_SOURCE_CONFIG_FILE={temp_root / 'sources.json'}",
                f"MARKET_MONITOR_SNAPSHOT_STORE_DIR={temp_root / 'snapshots'}",
                "PRODUCT_VIABILITY_MAX_IMAGES=4",
                "PRODUCT_VIABILITY_MAX_IMAGE_BYTES=16",
                "TINYFISH_BASE_URL=https://agent.tinyfish.ai",
                "TINYFISH_API_KEY=test-tinyfish-key",
                "TINYFISH_TIMEOUT_SECONDS=30",
                "OPENAI_BASE_URL=https://api.openai.com/v1",
                "OPENAI_API_KEY=test-key",
                "OPENAI_MODEL=gpt-4.1-mini",
                "OPENAI_TIMEOUT_SECONDS=30",
            ]
        )
    )
    with patch.dict(os.environ, {}, clear=True):
        get_settings.cache_clear()
        settings = get_settings(temp_root)
        get_settings.cache_clear()
    return settings


class ProductViabilityRouteTests(unittest.TestCase):
    def setUp(self):
        self.app = create_app()
        self.app.config["TESTING"] = True
        self.app.config["SETTINGS"] = build_settings_from_env()
        self.client = self.app.test_client()

    def tearDown(self):
        get_settings.cache_clear()

    def test_text_only_request_returns_response(self):
        mock_response = {"decision_memo": {"summary": "Strong market pull."}}
        with patch(
            "market_monitor_api.routes.product_viability.build_product_viability_response",
            return_value=mock_response,
        ):
            response = self.client.post(
                "/api/v1/product-viability",
                data={"description": "AI assistant for boutique retailers"},
                content_type="multipart/form-data",
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), mock_response)

    def test_natural_language_only_request_returns_response(self):
        mock_response = {"decision_memo": {"summary": "Natural language processed."}}
        with patch(
            "market_monitor_api.routes.product_viability.build_product_viability_response",
            return_value=mock_response,
        ):
            response = self.client.post(
                "/api/v1/product-viability",
                data={"query": "Is there a viable market for a portable espresso maker for campers?"},
                content_type="multipart/form-data",
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), mock_response)

    def test_images_only_request_returns_response(self):
        mock_response = {"decision_memo": {"summary": "Visual concept understood."}}
        with patch(
            "market_monitor_api.routes.product_viability.build_product_viability_response",
            return_value=mock_response,
        ):
            response = self.client.post(
                "/api/v1/product-viability",
                data={"images": (BytesIO(b"image-bytes"), "concept.png")},
                content_type="multipart/form-data",
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), mock_response)

    def test_text_and_images_request_returns_response(self):
        mock_response = {"decision_memo": {"summary": "Image and text analyzed."}}
        with patch(
            "market_monitor_api.routes.product_viability.build_product_viability_response",
            return_value=mock_response,
        ):
            response = self.client.post(
                "/api/v1/product-viability",
                data={
                    "description": "Portable espresso device",
                    "images": (BytesIO(b"png-data"), "device.png"),
                },
                content_type="multipart/form-data",
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), mock_response)

    def test_empty_request_is_rejected(self):
        response = self.client.post(
            "/api/v1/product-viability",
            data={},
            content_type="multipart/form-data",
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"]["code"], "product_viability_input_missing")

    def test_invalid_research_depth_is_rejected(self):
        response = self.client.post(
            "/api/v1/product-viability",
            data={
                "description": "Portable espresso device",
                "research_depth": "extreme",
            },
            content_type="multipart/form-data",
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"]["code"], "product_viability_invalid_research_depth")

    def test_unsupported_image_type_is_rejected(self):
        response = self.client.post(
            "/api/v1/product-viability",
            data={"images": (BytesIO(b"plain-text"), "concept.txt", "text/plain")},
            content_type="multipart/form-data",
        )

        self.assertEqual(response.status_code, 415)
        self.assertEqual(response.get_json()["error"]["code"], "product_viability_unsupported_media_type")

    def test_too_many_images_are_rejected(self):
        response = self.client.post(
            "/api/v1/product-viability",
            data={
                "images": [
                    (BytesIO(b"a"), "1.png"),
                    (BytesIO(b"b"), "2.png"),
                    (BytesIO(b"c"), "3.png"),
                    (BytesIO(b"d"), "4.png"),
                    (BytesIO(b"e"), "5.png"),
                ]
            },
            content_type="multipart/form-data",
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"]["code"], "product_viability_too_many_images")

    def test_too_large_image_is_rejected(self):
        response = self.client.post(
            "/api/v1/product-viability",
            data={"images": (BytesIO(b"x" * 17), "concept.png")},
            content_type="multipart/form-data",
        )

        self.assertEqual(response.status_code, 413)
        self.assertEqual(response.get_json()["error"]["code"], "product_viability_image_too_large")


if __name__ == "__main__":
    unittest.main()
