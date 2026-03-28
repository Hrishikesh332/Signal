from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
import socket
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import json

from market_monitor_api.config import Settings


VALID_SOURCE_CATEGORIES = {
    "commerce_intelligence",
    "reputation_intelligence",
    "growth_intelligence",
}

VALID_TINYFISH_BROWSER_PROFILES = {"lite", "stealth"}


def load_source_catalog(settings: Settings) -> list[dict]:
    source_file = settings.resolve_path(settings.source_config_file)
    if not source_file.exists():
        raise FileNotFoundError(f"Source config file not found: {source_file}")
    payload = json.loads(source_file.read_text())
    if isinstance(payload, dict):
        sources = payload.get("sources", [])
    elif isinstance(payload, list):
        sources = payload
    else:
        raise ValueError("Source config must be a JSON array or an object with a sources field.")
    validated_sources = [validate_source_definition(source) for source in sources]
    return [
        {
            **source,
            "watcher": build_watcher_runtime(settings, source),
        }
        for source in validated_sources
    ]


def validate_source_definition(source: dict) -> dict:
    required_fields = [
        "id",
        "name",
        "category",
        "company_id",
        "company_name",
        "target_url",
        "goal",
        "output_schema",
        "stop_conditions",
        "error_handling",
        "browser_profile",
    ]
    missing_fields = [field for field in required_fields if field not in source]
    if missing_fields:
        raise ValueError(f"Source {source.get('id', 'unknown')} is missing fields: {', '.join(missing_fields)}")
    if source["category"] not in VALID_SOURCE_CATEGORIES:
        raise ValueError(f"Source {source['id']} has invalid category: {source['category']}")
    if source["browser_profile"] not in VALID_TINYFISH_BROWSER_PROFILES:
        raise ValueError(f"Source {source['id']} has invalid browser_profile: {source['browser_profile']}")
    if not str(source["target_url"]).startswith("http"):
        raise ValueError(f"Source {source['id']} must use a real target_url.")
    if not isinstance(source["output_schema"], dict):
        raise ValueError(f"Source {source['id']} must define output_schema as an object.")
    if not isinstance(source["stop_conditions"], list):
        raise ValueError(f"Source {source['id']} must define stop_conditions as an array.")
    if not isinstance(source["error_handling"], dict):
        raise ValueError(f"Source {source['id']} must define error_handling as an object.")
    return {
        **source,
        "product_id": source.get("product_id"),
        "product_name": source.get("product_name"),
        "proxy_config": source.get("proxy_config"),
        "use_vault": bool(source.get("use_vault", False)),
        "credential_item_ids": source.get("credential_item_ids", []),
    }


def build_watcher_runtime(settings: Settings, source: dict) -> dict:
    return {
        "provider": "TinyFish",
        "endpoint": f"{settings.tinyfish_base_url.rstrip('/')}/v1/automation/run",
        "browser_profile": source["browser_profile"],
        "configured": settings.tinyfish_configured,
    }


def build_tinyfish_goal_prompt(source: dict) -> str:
    schema_block = json.dumps(source["output_schema"], separators=(",", ":"), ensure_ascii=True)
    stop_conditions_block = json.dumps(source["stop_conditions"], separators=(",", ":"), ensure_ascii=True)
    error_handling_block = json.dumps(source["error_handling"], separators=(",", ":"), ensure_ascii=True)
    return (
        f"{source['goal'].strip()}\n\n"
        "Return only structured JSON that matches this schema exactly:\n"
        f"{schema_block}\n\n"
        "Stop conditions:\n"
        f"{stop_conditions_block}\n\n"
        "Structured error handling rules:\n"
        f"{error_handling_block}"
    )


def run_source_refreshes(settings: Settings, sources: list[dict]) -> list[dict]:
    snapshots = []
    for source in sources:
        run_response = run_tinyfish_source(settings, source)
        snapshot = build_snapshot_record(source, run_response)
        persist_snapshot(settings, snapshot)
        snapshots.append(snapshot)
    return snapshots


def run_tinyfish_source(settings: Settings, source: dict) -> dict:
    request_body = build_tinyfish_request_body(source)
    request_url = f"{settings.tinyfish_base_url.rstrip('/')}/v1/automation/run"
    request = Request(
        request_url,
        data=json.dumps(request_body).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "X-API-Key": settings.tinyfish_api_key,
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=settings.tinyfish_timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        return build_failed_run_response(
            error_code=f"http_{exc.code}",
            error_message=read_error_message(exc),
        )
    except (TimeoutError, socket.timeout) as exc:
        return build_failed_run_response(
            error_code="timeout",
            error_message=str(exc) or "TinyFish request timed out.",
        )
    except URLError as exc:
        return build_failed_run_response(
            error_code="network_error",
            error_message=str(exc.reason),
        )


def build_tinyfish_request_body(source: dict) -> dict:
    request_body = {
        "url": source["target_url"],
        "goal": build_tinyfish_goal_prompt(source),
        "browser_profile": source["browser_profile"],
        "api_integration": "market-monitor",
        "use_vault": source["use_vault"],
    }
    if source.get("proxy_config"):
        request_body["proxy_config"] = source["proxy_config"]
    if source.get("credential_item_ids"):
        request_body["credential_item_ids"] = source["credential_item_ids"]
    return request_body


def build_failed_run_response(error_code: str, error_message: str) -> dict:
    timestamp = to_iso_timestamp(datetime.now(timezone.utc))
    return {
        "run_id": None,
        "status": "FAILED",
        "started_at": timestamp,
        "finished_at": timestamp,
        "num_of_steps": None,
        "result": None,
        "error": {
            "code": error_code,
            "message": error_message,
        },
    }


def read_error_message(error: HTTPError) -> str:
    try:
        payload = json.loads(error.read().decode("utf-8"))
    except Exception:
        return error.reason or "TinyFish request failed."
    if isinstance(payload, dict):
        error_payload = payload.get("error")
        if isinstance(error_payload, dict):
            return str(error_payload.get("message") or payload)
        return str(payload)
    return error.reason or "TinyFish request failed."


def build_snapshot_record(source: dict, run_response: dict) -> dict:
    captured_at = run_response.get("finished_at") or run_response.get("started_at") or to_iso_timestamp(
        datetime.now(timezone.utc)
    )
    result = run_response.get("result")
    validation_errors = []
    capture_status = run_response.get("status", "FAILED")
    if capture_status == "COMPLETED" and result is not None:
        validation_errors = validate_result_against_schema(source["output_schema"], result)
        if validation_errors:
            capture_status = "VALIDATION_ERROR"
    return {
        "snapshot_id": build_snapshot_id(source["id"], captured_at, run_response.get("run_id")),
        "captured_at": captured_at,
        "capture_status": capture_status,
        "source_id": source["id"],
        "source_name": source["name"],
        "category": source["category"],
        "company_id": source["company_id"],
        "company_name": source["company_name"],
        "product_id": source.get("product_id"),
        "product_name": source.get("product_name"),
        "target_url": source["target_url"],
        "goal": source["goal"],
        "goal_prompt": build_tinyfish_goal_prompt(source),
        "output_schema": source["output_schema"],
        "stop_conditions": source["stop_conditions"],
        "error_handling": source["error_handling"],
        "run": {
            "run_id": run_response.get("run_id"),
            "status": run_response.get("status"),
            "started_at": run_response.get("started_at"),
            "finished_at": run_response.get("finished_at"),
            "num_of_steps": run_response.get("num_of_steps"),
            "error": run_response.get("error"),
        },
        "result": result,
        "validation_errors": validation_errors,
    }


def build_snapshot_id(source_id: str, captured_at: str, run_id: str | None) -> str:
    normalized_timestamp = captured_at.replace(":", "").replace("-", "").replace(".", "")
    normalized_timestamp = normalized_timestamp.replace("+00:00", "Z")
    if run_id:
        return f"{source_id}-{run_id}"
    return f"{source_id}-{normalized_timestamp}"


def validate_result_against_schema(schema: dict, value, path: str = "$") -> list[dict]:
    validation_errors = []
    schema_type = schema.get("type")
    if schema_type and not validate_type_value(schema_type, value):
        validation_errors.append(
            {
                "path": path,
                "message": f"Expected {schema_type}, received {type(value).__name__}",
            }
        )
        return validation_errors
    if schema_type == "object":
        required_fields = schema.get("required", [])
        properties = schema.get("properties", {})
        for field in required_fields:
            if field not in value:
                validation_errors.append(
                    {
                        "path": path,
                        "message": f"Missing required field: {field}",
                    }
                )
        for key, child_schema in properties.items():
            if key in value:
                validation_errors.extend(
                    validate_result_against_schema(child_schema, value[key], f"{path}.{key}")
                )
    if schema_type == "array":
        item_schema = schema.get("items")
        if item_schema:
            for index, item in enumerate(value):
                validation_errors.extend(
                    validate_result_against_schema(item_schema, item, f"{path}[{index}]")
                )
    return validation_errors


def validate_type_value(expected_type: str, value) -> bool:
    type_map = {
        "object": lambda item: isinstance(item, dict),
        "array": lambda item: isinstance(item, list),
        "string": lambda item: isinstance(item, str),
        "number": lambda item: isinstance(item, (int, float)) and not isinstance(item, bool),
        "integer": lambda item: isinstance(item, int) and not isinstance(item, bool),
        "boolean": lambda item: isinstance(item, bool),
        "null": lambda item: item is None,
    }
    validator = type_map.get(expected_type)
    if not validator:
        return True
    return validator(value)


def persist_snapshot(settings: Settings, snapshot: dict) -> Path:
    snapshot_root = settings.resolve_path(settings.snapshot_store_dir)
    source_dir = snapshot_root / snapshot["source_id"]
    source_dir.mkdir(parents=True, exist_ok=True)
    file_path = source_dir / f"{snapshot['snapshot_id']}.json"
    file_path.write_text(json.dumps(snapshot, indent=2, ensure_ascii=True))
    return file_path


def load_snapshots(settings: Settings) -> list[dict]:
    snapshot_root = settings.resolve_path(settings.snapshot_store_dir)
    if not snapshot_root.exists():
        return []
    snapshots = []
    for file_path in sorted(snapshot_root.rglob("*.json")):
        snapshots.append(json.loads(file_path.read_text()))
    return sort_snapshots(snapshots)


def sort_snapshots(snapshots: list[dict]) -> list[dict]:
    return sorted(snapshots, key=lambda snapshot: parse_iso_datetime(snapshot["captured_at"]))


def build_source_health(settings: Settings, sources: list[dict], snapshots: list[dict]) -> list[dict]:
    source_snapshots: dict[str, list[dict]] = defaultdict(list)
    for snapshot in snapshots:
        source_snapshots[snapshot["source_id"]].append(snapshot)
    health_records = []
    for source in sources:
        records = sort_snapshots(source_snapshots.get(source["id"], []))
        latest = records[-1] if records else None
        successful_runs = len([record for record in records if record["capture_status"] == "COMPLETED"])
        total_runs = len(records)
        health_records.append(
            {
                "source_id": source["id"],
                "status": build_health_status(latest),
                "last_run_at": latest["captured_at"] if latest else None,
                "success_rate": round(successful_runs / total_runs, 4) if total_runs else None,
                "avg_runtime_ms": build_average_runtime_ms(records),
                "snapshots_total": total_runs,
                "provider": "TinyFish",
                "configured": settings.tinyfish_configured,
                "last_error": build_last_error(latest),
            }
        )
    return health_records


def build_health_status(latest_snapshot: dict | None) -> str:
    if latest_snapshot is None:
        return "not_run"
    if latest_snapshot["capture_status"] == "COMPLETED":
        return "healthy"
    if latest_snapshot["capture_status"] == "VALIDATION_ERROR":
        return "validation_error"
    return "failed"


def build_average_runtime_ms(records: list[dict]) -> int | None:
    durations = []
    for record in records:
        started_at = record["run"].get("started_at")
        finished_at = record["run"].get("finished_at")
        if not started_at or not finished_at:
            continue
        duration_ms = int((parse_iso_datetime(finished_at) - parse_iso_datetime(started_at)).total_seconds() * 1000)
        durations.append(duration_ms)
    if not durations:
        return None
    return int(sum(durations) / len(durations))


def build_last_error(latest_snapshot: dict | None):
    if latest_snapshot is None:
        return None
    if latest_snapshot["capture_status"] == "VALIDATION_ERROR":
        return {
            "code": "validation_error",
            "message": "TinyFish returned data that did not match the configured output schema.",
            "details": latest_snapshot["validation_errors"],
        }
    return latest_snapshot["run"].get("error")


def build_company_catalog(sources: list[dict], snapshots: list[dict]) -> list[dict]:
    categories_by_company: dict[str, set[str]] = defaultdict(set)
    source_ids_by_company: dict[str, list[str]] = defaultdict(list)
    names_by_company: dict[str, str] = {}
    for source in sources:
        categories_by_company[source["company_id"]].add(source["category"])
        source_ids_by_company[source["company_id"]].append(source["id"])
        names_by_company[source["company_id"]] = source["company_name"]
    return [
        {
            "id": company_id,
            "name": names_by_company[company_id],
            "tracked_categories": sorted(categories),
            "source_ids": sorted(source_ids_by_company[company_id]),
        }
        for company_id, categories in sorted(categories_by_company.items())
    ]


def build_product_catalog(sources: list[dict], snapshots: list[dict]) -> list[dict]:
    products = {}
    for source in sources:
        if not source.get("product_id"):
            continue
        products[source["product_id"]] = {
            "id": source["product_id"],
            "company_id": source["company_id"],
            "name": source.get("product_name"),
            "source_id": source["id"],
        }
    return [products[product_id] for product_id in sorted(products)]


def build_snapshot_comparisons(sources: list[dict], snapshots: list[dict]) -> list[dict]:
    source_snapshots: dict[str, list[dict]] = defaultdict(list)
    for snapshot in snapshots:
        if snapshot["capture_status"] == "COMPLETED" and snapshot.get("result") is not None:
            source_snapshots[snapshot["source_id"]].append(snapshot)
    comparisons = []
    for source in sources:
        records = sort_snapshots(source_snapshots.get(source["id"], []))
        if len(records) < 2:
            continue
        previous_snapshot = records[-2]
        current_snapshot = records[-1]
        changes = build_result_changes(previous_snapshot["result"], current_snapshot["result"])
        if not changes:
            continue
        comparisons.append(
            {
                "comparison_id": current_snapshot["snapshot_id"],
                "snapshot_id": current_snapshot["snapshot_id"],
                "previous_snapshot_id": previous_snapshot["snapshot_id"],
                "captured_at": current_snapshot["captured_at"],
                "category": current_snapshot["category"],
                "source_id": current_snapshot["source_id"],
                "source_name": current_snapshot["source_name"],
                "company_id": current_snapshot["company_id"],
                "company_name": current_snapshot["company_name"],
                "product_id": current_snapshot.get("product_id"),
                "product_name": current_snapshot.get("product_name"),
                "target_url": current_snapshot["target_url"],
                "current": current_snapshot["result"],
                "previous": previous_snapshot["result"],
                "changes": changes,
            }
        )
    return sorted(comparisons, key=lambda item: parse_iso_datetime(item["captured_at"]), reverse=True)


def build_result_changes(previous_result: dict, current_result: dict) -> list[dict]:
    previous_fields = flatten_result(previous_result)
    current_fields = flatten_result(current_result)
    changes = []
    for field in sorted(set(previous_fields) | set(current_fields)):
        if field in {"captured_at", "metrics", "map_points"}:
            continue
        previous_value = previous_fields.get(field)
        current_value = current_fields.get(field)
        if previous_value == current_value:
            continue
        changes.append(
            {
                "field": field,
                "previous": previous_value,
                "current": current_value,
            }
        )
    return changes


def flatten_result(value, prefix: str = "") -> dict:
    if isinstance(value, dict):
        flattened = {}
        for key, child_value in value.items():
            next_prefix = f"{prefix}.{key}" if prefix else key
            flattened.update(flatten_result(child_value, next_prefix))
        return flattened
    if isinstance(value, list):
        if all(not isinstance(item, (dict, list)) for item in value):
            return {prefix: value}
        flattened = {}
        for index, child_value in enumerate(value):
            next_prefix = f"{prefix}[{index}]"
            flattened.update(flatten_result(child_value, next_prefix))
        return flattened
    return {prefix: value}


def build_trend_series(snapshots: list[dict]) -> dict:
    series = {
        "price": [],
        "sentiment": [],
        "growth": [],
    }
    for snapshot in sort_snapshots(snapshots):
        if snapshot["capture_status"] != "COMPLETED":
            continue
        result = snapshot.get("result")
        if not isinstance(result, dict):
            continue
        metrics = result.get("metrics")
        if not isinstance(metrics, dict):
            continue
        for metric_name in series:
            metric_value = metrics.get(metric_name)
            if isinstance(metric_value, (int, float)) and not isinstance(metric_value, bool):
                series[metric_name].append(
                    {
                        "timestamp": snapshot["captured_at"],
                        "value": metric_value,
                        "source_id": snapshot["source_id"],
                        "company_id": snapshot["company_id"],
                    }
                )
    return series


def build_map_points(snapshots: list[dict]) -> list[dict]:
    latest_by_source = {}
    for snapshot in sort_snapshots(snapshots):
        if snapshot["capture_status"] != "COMPLETED":
            continue
        latest_by_source[snapshot["source_id"]] = snapshot
    points = []
    for snapshot in latest_by_source.values():
        result = snapshot.get("result")
        if not isinstance(result, dict):
            continue
        map_points = result.get("map_points")
        if not isinstance(map_points, list):
            continue
        for point in map_points:
            normalized_point = normalize_map_point(point)
            if normalized_point:
                points.append(normalized_point)
    return points


def normalize_map_point(point: dict) -> dict | None:
    required_fields = [
        "latitude",
        "longitude",
        "entity_name",
        "signal_type",
        "severity",
        "timestamp",
        "explanation",
    ]
    if not isinstance(point, dict):
        return None
    if any(field not in point for field in required_fields):
        return None
    return {
        "latitude": point["latitude"],
        "longitude": point["longitude"],
        "entity_name": point["entity_name"],
        "signal_type": point["signal_type"],
        "severity": point["severity"],
        "timestamp": point["timestamp"],
        "explanation": point["explanation"],
        "cluster_key": point.get("cluster_key"),
    }


def parse_iso_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def to_iso_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
