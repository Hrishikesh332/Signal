from flask import Blueprint, current_app, jsonify, request

from market_monitor_api.services.market_signals import (
    MarketSignalsConfigError,
    build_watcher_qa_response,
    replay_watcher_snapshots,
)

watcher_qa_bp = Blueprint("watcher_qa", __name__)


@watcher_qa_bp.get("/watcher-qa")
def get_watcher_qa():
    try:
        settings = current_app.config["SETTINGS"]
        return jsonify(build_watcher_qa_response(settings)), 200
    except MarketSignalsConfigError as exc:
        return jsonify({"error": {"code": exc.code, "message": exc.message}}), exc.status_code
    except Exception:
        return (
            jsonify(
                {
                    "error": {
                        "code": "watcher_qa_unavailable",
                        "message": "Unable to build watcher QA payload.",
                    }
                }
            ),
            500,
        )


@watcher_qa_bp.post("/watcher-qa/replay")
def post_watcher_replay():
    try:
        settings = current_app.config["SETTINGS"]
        payload = request.get_json(silent=True) or {}
        return jsonify(replay_watcher_snapshots(settings, payload)), 200
    except MarketSignalsConfigError as exc:
        return jsonify({"error": {"code": exc.code, "message": exc.message}}), exc.status_code
    except Exception:
        return (
            jsonify(
                {
                    "error": {
                        "code": "watcher_replay_unavailable",
                        "message": "Unable to replay watcher snapshots.",
                    }
                }
            ),
            500,
        )
