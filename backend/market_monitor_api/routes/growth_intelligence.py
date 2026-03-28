from flask import Blueprint, current_app, jsonify, request

from market_monitor_api.services.growth_intelligence import (
    GrowthConfigError,
    build_growth_events_response,
    build_growth_history_response,
    build_growth_response,
    build_growth_trends_response,
)

growth_bp = Blueprint("growth_intelligence", __name__)


@growth_bp.get("/growth-intelligence")
def get_growth_intelligence():
    try:
        settings = current_app.config["SETTINGS"]
        refresh = parse_refresh_flag(request.args.get("refresh"))
        filters = build_growth_filters(request.args)
        return jsonify(build_growth_response(settings, refresh=refresh, filters=filters)), 200
    except GrowthConfigError as exc:
        return jsonify({"error": {"code": exc.code, "message": exc.message}}), exc.status_code
    except Exception:
        return (
            jsonify(
                {
                    "error": {
                        "code": "growth_intelligence_unavailable",
                        "message": "Unable to build growth intelligence payload.",
                    }
                }
            ),
            500,
        )


@growth_bp.get("/growth-intelligence/events")
def get_growth_events():
    try:
        settings = current_app.config["SETTINGS"]
        refresh = parse_refresh_flag(request.args.get("refresh"))
        filters = build_growth_filters(request.args)
        return jsonify(build_growth_events_response(settings, refresh=refresh, filters=filters)), 200
    except GrowthConfigError as exc:
        return jsonify({"error": {"code": exc.code, "message": exc.message}}), exc.status_code
    except Exception:
        return (
            jsonify(
                {
                    "error": {
                        "code": "growth_events_unavailable",
                        "message": "Unable to build growth events payload.",
                    }
                }
            ),
            500,
        )


@growth_bp.get("/growth-intelligence/history")
def get_growth_history():
    try:
        settings = current_app.config["SETTINGS"]
        filters = build_growth_filters(request.args)
        return jsonify(build_growth_history_response(settings, filters=filters)), 200
    except GrowthConfigError as exc:
        return jsonify({"error": {"code": exc.code, "message": exc.message}}), exc.status_code
    except Exception:
        return (
            jsonify(
                {
                    "error": {
                        "code": "growth_history_unavailable",
                        "message": "Unable to build growth history payload.",
                    }
                }
            ),
            500,
        )


@growth_bp.get("/growth-intelligence/trends")
def get_growth_trends():
    try:
        settings = current_app.config["SETTINGS"]
        filters = build_growth_filters(request.args)
        return jsonify(build_growth_trends_response(settings, filters=filters)), 200
    except GrowthConfigError as exc:
        return jsonify({"error": {"code": exc.code, "message": exc.message}}), exc.status_code
    except Exception:
        return (
            jsonify(
                {
                    "error": {
                        "code": "growth_trends_unavailable",
                        "message": "Unable to build growth trends payload.",
                    }
                }
            ),
            500,
        )


def parse_refresh_flag(value: str | None) -> bool:
    if not value:
        return False
    return value.strip().lower() in {"1", "true", "yes"}


def build_growth_filters(args) -> dict:
    filters = {}
    for key in ("source_id", "company_id", "source_type", "signal_type", "cluster_name", "location"):
        value = args.get(key)
        if value:
            filters[key] = value.strip()
    cursor = args.get("cursor")
    if cursor and cursor.strip():
        filters["cursor"] = cursor.strip()
    limit = args.get("limit")
    if limit and limit.strip():
        try:
            parsed = int(limit.strip())
        except ValueError as exc:
            raise GrowthConfigError(
                "invalid_growth_filter",
                "Invalid limit filter. limit must be an integer between 1 and 500.",
                status_code=400,
            ) from exc
        if parsed < 1 or parsed > 500:
            raise GrowthConfigError(
                "invalid_growth_filter",
                "Invalid limit filter. limit must be an integer between 1 and 500.",
                status_code=400,
            )
        filters["limit"] = parsed
    return filters
