from flask import Blueprint, current_app, jsonify, request

from market_monitor_api.services.commerce_intelligence import (
    CommerceConfigError,
    build_commerce_history_response,
    build_commerce_response,
    build_commerce_signals_response,
)

commerce_bp = Blueprint("commerce_intelligence", __name__)


@commerce_bp.get("/commerce-intelligence")
def get_commerce_intelligence():
    try:
        settings = current_app.config["SETTINGS"]
        refresh = parse_refresh_flag(request.args.get("refresh"))
        filters = build_commerce_filters(request.args)
        return jsonify(build_commerce_response(settings, refresh=refresh, filters=filters)), 200
    except CommerceConfigError as exc:
        return jsonify({"error": {"code": exc.code, "message": exc.message}}), exc.status_code
    except Exception:
        return (
            jsonify(
                {
                    "error": {
                        "code": "commerce_intelligence_unavailable",
                        "message": "Unable to build commerce intelligence payload.",
                    }
                }
            ),
            500,
        )


@commerce_bp.get("/commerce-intelligence/signals")
def get_commerce_signals():
    try:
        settings = current_app.config["SETTINGS"]
        refresh = parse_refresh_flag(request.args.get("refresh"))
        filters = build_commerce_filters(request.args)
        return jsonify(build_commerce_signals_response(settings, refresh=refresh, filters=filters)), 200
    except CommerceConfigError as exc:
        return jsonify({"error": {"code": exc.code, "message": exc.message}}), exc.status_code
    except Exception:
        return (
            jsonify(
                {
                    "error": {
                        "code": "commerce_signals_unavailable",
                        "message": "Unable to build commerce signals payload.",
                    }
                }
            ),
            500,
        )


@commerce_bp.get("/commerce-intelligence/history")
def get_commerce_history():
    try:
        settings = current_app.config["SETTINGS"]
        filters = build_commerce_filters(request.args)
        return jsonify(build_commerce_history_response(settings, filters=filters)), 200
    except CommerceConfigError as exc:
        return jsonify({"error": {"code": exc.code, "message": exc.message}}), exc.status_code
    except Exception:
        return (
            jsonify(
                {
                    "error": {
                        "code": "commerce_history_unavailable",
                        "message": "Unable to build commerce history payload.",
                    }
                }
            ),
            500,
        )


def parse_refresh_flag(value: str | None) -> bool:
    if not value:
        return False
    return value.strip().lower() in {"1", "true", "yes"}


def build_commerce_filters(args) -> dict:
    filters = {}
    for key in ("source_id", "sku", "marketplace", "tracking_group_id", "company_id", "competitor_id"):
        value = args.get(key)
        if value:
            filters[key] = value.strip()
    return filters
