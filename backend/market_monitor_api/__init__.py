from pathlib import Path

from flask import Flask

from market_monitor_api.config import get_settings
from market_monitor_api.routes.commerce_intelligence import commerce_bp
from market_monitor_api.routes.dashboard import dashboard_bp
from market_monitor_api.routes.growth_intelligence import growth_bp
from market_monitor_api.routes.market_signals import market_signals_bp
from market_monitor_api.routes.watcher_qa import watcher_qa_bp


def create_app() -> Flask:
    app = Flask(__name__)
    project_root = Path(__file__).resolve().parents[2]
    settings = get_settings(project_root)
    app.config["SETTINGS"] = settings
    app.register_blueprint(dashboard_bp, url_prefix="/api/v1")
    app.register_blueprint(commerce_bp, url_prefix="/api/v1")
    app.register_blueprint(growth_bp, url_prefix="/api/v1")
    app.register_blueprint(market_signals_bp, url_prefix="/api/v1")
    app.register_blueprint(watcher_qa_bp, url_prefix="/api/v1")
    return app
