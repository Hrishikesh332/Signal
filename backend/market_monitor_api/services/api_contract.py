from base64 import urlsafe_b64decode, urlsafe_b64encode
import json


CONTRACT_VERSION = "2026-03-28"

API_ENUMS = {
    "categories": [
        "commerce_intelligence",
        "growth_intelligence",
        "reputation_intelligence",
    ],
    "severity": [
        "critical",
        "high",
        "medium",
        "low",
    ],
    "wire_level": [
        "high",
        "elevated",
        "watch",
    ],
    "signal_lifecycle_state": [
        "new",
        "updated",
        "resolved",
        "suppressed",
        "confirmed",
    ],
    "source_health_status": [
        "healthy",
        "failed",
        "validation_error",
        "not_run",
    ],
}


def build_contract_payload(module: str, view: str | None = None) -> dict:
    payload = {
        "module": module,
        "contract_version": CONTRACT_VERSION,
        "enums": API_ENUMS,
    }
    if view:
        payload["view"] = view
    return payload


def parse_limit(value: str | None, default: int, maximum: int = 500) -> int:
    if not value:
        return default
    normalized = value.strip()
    if not normalized:
        return default
    parsed = int(normalized)
    if parsed < 1 or parsed > maximum:
        raise ValueError(f"limit must be between 1 and {maximum}")
    return parsed


def encode_cursor(item_id: str, timestamp: str) -> str:
    raw_value = json.dumps(
        {
            "id": item_id,
            "timestamp": timestamp,
        },
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return urlsafe_b64encode(raw_value).decode("utf-8")


def decode_cursor(value: str | None) -> dict | None:
    if not value:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    payload = json.loads(urlsafe_b64decode(normalized.encode("utf-8")).decode("utf-8"))
    if not isinstance(payload, dict):
        return None
    item_id = payload.get("id")
    timestamp = payload.get("timestamp")
    if not isinstance(item_id, str) or not isinstance(timestamp, str):
        return None
    return {
        "id": item_id,
        "timestamp": timestamp,
    }


def paginate_records(records: list[dict], cursor: dict | None, limit: int) -> tuple[list[dict], dict]:
    start_index = 0
    if cursor:
        for index, record in enumerate(records):
            if record.get("id") == cursor["id"] and record.get("timestamp") == cursor["timestamp"]:
                start_index = index + 1
                break
    page_items = records[start_index:start_index + limit]
    has_more = start_index + limit < len(records)
    next_cursor = None
    if has_more and page_items:
        next_cursor = encode_cursor(page_items[-1]["id"], page_items[-1]["timestamp"])
    return page_items, {
        "limit": limit,
        "count": len(page_items),
        "has_more": has_more,
        "next_cursor": next_cursor,
    }
