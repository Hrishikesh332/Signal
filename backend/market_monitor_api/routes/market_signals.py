from flask import Blueprint, current_app, jsonify, request

from market_monitor_api.services.market_signals import (
    MarketSignalsConfigError,
    SIGNAL_LIFECYCLE_STATES,
    VALID_MARKET_SIGNAL_CATEGORIES,
    VALID_MARKET_SIGNAL_SEVERITIES,
    VALID_MARKET_TOPICS,
    VALID_WIRE_LEVELS,
    build_market_signal_detail_response,
    build_market_signals_response,
    update_market_signal_lifecycle,
)

market_signals_bp = Blueprint("market_signals", __name__)


@market_signals_bp.get("/market-signals")
def get_market_signals():
    try:
        settings = current_app.config["SETTINGS"]
        refresh = parse_refresh_flag(request.args.get("refresh"))
        filters = build_market_signal_filters(request.args)
        return jsonify(build_market_signals_response(settings, refresh=refresh, filters=filters)), 200
    except MarketSignalsConfigError as exc:
        return jsonify({"error": {"code": exc.code, "message": exc.message}}), exc.status_code
    except Exception:
        return (
            jsonify(
                {
                    "error": {
                        "code": "market_signals_unavailable",
                        "message": "Unable to build market signals payload.",
                    }
                }
            ),
            500,
        )


@market_signals_bp.get("/market-signals/<path:signal_id>")
def get_market_signal_detail(signal_id: str):
    try:
        settings = current_app.config["SETTINGS"]
        return jsonify(build_market_signal_detail_response(settings, signal_id)), 200
    except MarketSignalsConfigError as exc:
        return jsonify({"error": {"code": exc.code, "message": exc.message}}), exc.status_code
    except Exception:
        return (
            jsonify(
                {
                    "error": {
                        "code": "market_signal_detail_unavailable",
                        "message": "Unable to build market signal detail payload.",
                    }
                }
            ),
            500,
        )


@market_signals_bp.post("/market-signals/<path:signal_id>/lifecycle")
def post_market_signal_lifecycle(signal_id: str):
    try:
        settings = current_app.config["SETTINGS"]
        payload = request.get_json(silent=True) or {}
        lifecycle_state = payload.get("state")
        if not isinstance(lifecycle_state, str):
            raise MarketSignalsConfigError(
                "invalid_market_signal_lifecycle",
                "Lifecycle state is required.",
                status_code=400,
            )
        response_payload = update_market_signal_lifecycle(
            settings,
            signal_id,
            lifecycle_state.strip(),
            reason=payload.get("reason"),
            actor=payload.get("actor"),
        )
        return jsonify(response_payload), 200
    except MarketSignalsConfigError as exc:
        return jsonify({"error": {"code": exc.code, "message": exc.message}}), exc.status_code
    except Exception:
        return (
            jsonify(
                {
                    "error": {
                        "code": "market_signal_lifecycle_unavailable",
                        "message": "Unable to update market signal lifecycle.",
                    }
                }
            ),
            500,
        )


def parse_refresh_flag(value: str | None) -> bool:
    if not value:
        return False
    return value.strip().lower() in {"1", "true", "yes"}


def build_market_signal_filters(args) -> dict:
    filters = {}
    category = read_filter_value(args, "category")
    if category and category != "all":
        validate_market_signal_filter("category", category, VALID_MARKET_SIGNAL_CATEGORIES, "Invalid category filter.")
        filters["category"] = category
    severity = read_filter_value(args, "severity")
    if severity:
        validate_market_signal_filter("severity", severity, VALID_MARKET_SIGNAL_SEVERITIES, "Invalid severity filter.")
        filters["severity"] = severity
    wire_level = read_filter_value(args, "wire_level")
    if wire_level:
        validate_market_signal_filter("wire_level", wire_level, VALID_WIRE_LEVELS, "Invalid wire_level filter.")
        filters["wire_level"] = wire_level
    lifecycle_state = read_filter_value(args, "lifecycle_state")
    if lifecycle_state:
        validate_market_signal_filter(
            "lifecycle_state",
            lifecycle_state,
            SIGNAL_LIFECYCLE_STATES,
            "Invalid lifecycle_state filter.",
        )
        filters["lifecycle_state"] = lifecycle_state
    market_category = read_filter_value(args, "market_category")
    if market_category:
        normalized_market_category = normalize_market_category_filter(market_category)
        validate_market_signal_filter(
            "market_category",
            normalized_market_category,
            VALID_MARKET_TOPICS,
            "Invalid market_category filter.",
        )
        filters["market_category"] = normalized_market_category
    for key in ("company_id", "source_id", "signal_type", "location", "marketplace", "cursor"):
        value = read_filter_value(args, key)
        if value:
            filters[key] = value
    limit = read_limit_value(args.get("limit"))
    if limit is not None:
        filters["limit"] = limit
    return filters


def normalize_market_category_filter(value: str) -> str:
    normalized = value.strip().lower()
    if normalized == "tech":
        return "tech"
    if normalized == "technology":
        return "tech"
    return normalized


def read_filter_value(args, key: str) -> str | None:
    value = args.get(key)
    if not value:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    return normalized


def validate_market_signal_filter(filter_name: str, value: str, valid_values: set[str], message: str) -> None:
    if value not in valid_values:
        valid_label = ", ".join(sorted(valid_values))
        raise MarketSignalsConfigError(
            "invalid_market_signal_filter",
            f"{message} {filter_name} must be one of: {valid_label}.",
            status_code=400,
        )


def read_limit_value(value: str | None) -> int | None:
    if not value:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    try:
        parsed = int(normalized)
    except ValueError as exc:
        raise MarketSignalsConfigError(
            "invalid_market_signal_filter",
            "Invalid limit filter. limit must be an integer between 1 and 500.",
            status_code=400,
        ) from exc
    if parsed < 1 or parsed > 500:
        raise MarketSignalsConfigError(
            "invalid_market_signal_filter",
            "Invalid limit filter. limit must be an integer between 1 and 500.",
            status_code=400,
        )
    return parsed
