from pathlib import Path
import json
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "backend"))

from market_monitor_api.config import get_settings
from market_monitor_api.services.market_signals import build_market_signals_response


def main() -> None:
    settings = get_settings(PROJECT_ROOT)
    payload = build_market_signals_response(settings, refresh=True, filters={})
    print(
        json.dumps(
            {
                "generated_at": payload["meta"]["generated_at"],
                "schedule_interval_minutes": payload["meta"].get("schedule_interval_minutes"),
                "active_count": payload["meta"]["active_count"],
                "latest_snapshot_at": payload["meta"].get("latest_snapshot_at"),
                "recent_run_count": len(payload.get("recent_runs", [])),
            },
            indent=2,
            ensure_ascii=True,
        )
    )


if __name__ == "__main__":
    main()
