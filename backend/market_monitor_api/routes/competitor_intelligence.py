from flask import Blueprint, current_app, jsonify, request

from market_monitor_api.services.competitor_intelligence import (
    CompetitorIntelligenceConfigError,
    build_competitor_intelligence_response,
)

competitor_bp = Blueprint("competitor_intelligence", __name__)


@competitor_bp.get("/competitor-intelligence")
def get_competitor_intelligence():
    try:
        settings = current_app.config["SETTINGS"]
        company_url = read_query_value(request.args, "company_url")
        if not company_url:
            raise CompetitorIntelligenceConfigError(
                "missing_company_url",
                "company_url is required.",
                status_code=400,
            )
        refresh = parse_refresh_flag(request.args.get("refresh"))
        top_n = read_top_n_value(request.args.get("top_n"))
        return jsonify(
            build_competitor_intelligence_response(
                settings,
                company_url=company_url,
                refresh=refresh,
                top_n=top_n,
            )
        ), 200
    except CompetitorIntelligenceConfigError as exc:
        return jsonify({"error": {"code": exc.code, "message": exc.message}}), exc.status_code
    except Exception:
        return (
            jsonify(
                {
                    "error": {
                        "code": "competitor_intelligence_unavailable",
                        "message": "Unable to build competitor intelligence payload.",
                    }
                }
            ),
            500,
        )


def parse_refresh_flag(value: str | None) -> bool:
    if not value:
        return False
    return value.strip().lower() in {"1", "true", "yes"}


def read_query_value(args, key: str) -> str | None:
    value = args.get(key)
    if not value:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    return normalized


def read_top_n_value(value: str | None) -> int:
    if not value:
        return 4
    normalized = value.strip()
    if not normalized:
        return 4
    try:
        parsed = int(normalized)
    except ValueError as exc:
        raise CompetitorIntelligenceConfigError(
            "invalid_top_n",
            "top_n must be an integer between 1 and 8.",
            status_code=400,
        ) from exc
    if parsed < 1 or parsed > 8:
        raise CompetitorIntelligenceConfigError(
            "invalid_top_n",
            "top_n must be an integer between 1 and 8.",
            status_code=400,
        )
    return parsed
