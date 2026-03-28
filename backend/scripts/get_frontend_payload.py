from pathlib import Path
import json
import sys
from urllib.parse import urlencode


PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "backend"))

from app import app


VALID_ENDPOINTS = {
    "dashboard": "/api/v1/dashboard",
    "market-signals": "/api/v1/market-signals",
}


def main() -> None:
    endpoint_key = sys.argv[1] if len(sys.argv) > 1 else ""
    if endpoint_key not in VALID_ENDPOINTS:
        print(json.dumps({"error": {"code": "invalid_endpoint", "message": "Unsupported frontend payload endpoint."}}))
        raise SystemExit(1)
    query_pairs = json.loads(sys.argv[2]) if len(sys.argv) > 2 else {}
    if not isinstance(query_pairs, dict):
        query_pairs = {}
    query = urlencode({key: value for key, value in query_pairs.items() if isinstance(value, (str, int, float, bool))})
    path = VALID_ENDPOINTS[endpoint_key]
    target = f"{path}?{query}" if query else path
    with app.test_client() as client:
        response = client.get(target)
        payload = response.get_json(silent=True)
    print(
        json.dumps(
            {
                "status_code": response.status_code,
                "payload": payload,
            },
            ensure_ascii=True,
        )
    )


if __name__ == "__main__":
    main()
