from flask import Blueprint, current_app, jsonify, request

from market_monitor_api.services.dashboard import DashboardConfigError, build_dashboard_response

dashboard_bp = Blueprint("dashboard", __name__)


@dashboard_bp.get("/dashboard")
def get_dashboard():
    try:
        settings = current_app.config["SETTINGS"]
        refresh = request.args.get("refresh", "").strip().lower() in {"1", "true", "yes"}
        return jsonify(build_dashboard_response(settings, refresh=refresh)), 200
    except DashboardConfigError as exc:
        return jsonify({"error": {"code": exc.code, "message": exc.message}}), exc.status_code
    except Exception:
        return (
            jsonify(
                {
                    "error": {
                        "code": "dashboard_unavailable",
                        "message": "Unable to build dashboard payload.",
                    }
                }
            ),
            500,
        )
