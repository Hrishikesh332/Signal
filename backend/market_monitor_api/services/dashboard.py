from datetime import datetime, timedelta, timezone

from market_monitor_api.config import Settings
from market_monitor_api.services.api_contract import build_contract_payload
from market_monitor_api.services.growth_intelligence import GrowthConfigError, collect_growth_dataset
from market_monitor_api.services.openai_service import (
    build_alert_entities,
    build_event_analysis_map,
    build_event_entities,
)
from market_monitor_api.services.tinyfish import (
    build_company_catalog,
    build_map_points,
    build_product_catalog,
    build_source_health,
    build_snapshot_comparisons,
    build_trend_series,
    load_snapshots,
    load_source_catalog,
    parse_iso_datetime,
    run_source_refreshes,
    to_iso_timestamp,
)


class DashboardConfigError(Exception):
    def __init__(self, code: str, message: str, status_code: int = 503):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


def build_dashboard_response(settings: Settings, refresh: bool = False) -> dict:
    try:
        sources = load_source_catalog(settings)
    except FileNotFoundError as exc:
        raise DashboardConfigError("source_config_missing", str(exc))
    except ValueError as exc:
        raise DashboardConfigError("source_config_invalid", str(exc), status_code=500)
    if refresh and not settings.tinyfish_configured:
        raise DashboardConfigError(
            "tinyfish_not_configured",
            "TinyFish is required for refresh=true. Set TINYFISH_API_KEY in the .env file.",
        )
    if refresh and sources:
        run_source_refreshes(settings, sources)
    snapshots = load_snapshots(settings)
    comparisons = build_snapshot_comparisons(sources, snapshots)
    analysis_map = build_event_analysis_map(comparisons, settings)
    events = build_event_entities(comparisons, analysis_map, settings)
    alerts = build_alert_entities(events)
    source_health = build_source_health(settings, sources, snapshots)
    trend_series = build_trend_series(snapshots)
    growth_intelligence = build_growth_dashboard_section(settings)
    return {
        "contract": build_contract_payload("dashboard", view="overview"),
        "meta": build_meta(settings, sources, snapshots, refresh),
        "kpis": build_kpis(events, alerts, source_health, snapshots),
        "events": events,
        "alerts": alerts,
        "sources": sources,
        "companies": build_company_catalog(sources, snapshots),
        "products": build_product_catalog(sources, snapshots),
        "trends": build_trend_entities(trend_series),
        "trend_series": trend_series,
        "source_health": source_health,
        "map": {
            "clusters_enabled": True,
            "cluster_radius_km": 25,
            "points": build_map_points(snapshots),
        },
        "growth_intelligence": growth_intelligence,
    }


def build_meta(settings: Settings, sources: list[dict], snapshots: list[dict], refresh: bool) -> dict:
    latest_snapshot_at = snapshots[-1]["captured_at"] if snapshots else None
    return {
        "api_version": "v1",
        "contract_version": build_contract_payload("dashboard")["contract_version"],
        "platform": settings.app_name,
        "generated_at": to_iso_timestamp(datetime.now(timezone.utc)),
        "refresh_requested": refresh,
        "source_config_file": settings.source_config_file,
        "snapshot_store_dir": settings.snapshot_store_dir,
        "source_run_store_dir": settings.source_run_store_dir,
        "source_count": len(sources),
        "snapshot_count": len(snapshots),
        "latest_snapshot_at": latest_snapshot_at,
        "snapshot_strategy": {
            "mode": "change_only",
            "timestamp_field": "captured_at",
            "comparison_windows": ["previous_snapshot", "historical_store"],
            "deduplication_key": "source_id + content_fingerprint",
            "unchanged_runs": "stored_as_source_runs_only",
        },
        "integrations": {
            "tinyfish": {
                "provider": "TinyFish",
                "configured": settings.tinyfish_configured,
                "base_url": settings.tinyfish_base_url,
                "timeout_seconds": settings.tinyfish_timeout_seconds,
            },
            "openai": {
                "provider": "OpenAI",
                "configured": settings.openai_configured,
                "base_url": settings.openai_base_url,
                "model": settings.openai_model or None,
                "timeout_seconds": settings.openai_timeout_seconds,
            },
        },
        "entity_types": ["events", "alerts", "sources", "companies", "products", "trends"],
    }


def build_kpis(
    events: list[dict],
    alerts: list[dict],
    source_health: list[dict],
    snapshots: list[dict],
) -> list[dict]:
    now = datetime.now(timezone.utc)
    events_last_24h = len(
        [
            event
            for event in events
            if parse_iso_datetime(event["timestamp"]) >= now - timedelta(hours=24)
        ]
    )
    healthy_sources = len([item for item in source_health if item["status"] == "healthy"])
    return [
        {"id": "active_alerts", "label": "Active Alerts", "value": len(alerts)},
        {"id": "events_last_24h", "label": "Events (24h)", "value": events_last_24h},
        {"id": "healthy_sources", "label": "Healthy Sources", "value": healthy_sources},
        {"id": "snapshots_stored", "label": "Snapshots Stored", "value": len(snapshots)},
    ]


def build_trend_entities(trend_series: dict) -> list[dict]:
    trend_labels = {
        "price": "Price",
        "sentiment": "Sentiment",
        "growth": "Growth",
    }
    trends = []
    for metric_name, points in trend_series.items():
        if len(points) < 2:
            trends.append(
                {
                    "id": f"trend-{metric_name}",
                    "label": trend_labels.get(metric_name, metric_name.title()),
                    "direction": None,
                    "delta": None,
                    "points": len(points),
                }
            )
            continue
        delta = round(points[-1]["value"] - points[0]["value"], 4)
        if delta > 0:
            direction = "up"
        elif delta < 0:
            direction = "down"
        else:
            direction = "flat"
        trends.append(
            {
                "id": f"trend-{metric_name}",
                "label": trend_labels.get(metric_name, metric_name.title()),
                "direction": direction,
                "delta": delta,
                "points": len(points),
            }
        )
    return trends


def build_growth_dashboard_section(settings: Settings) -> dict:
    try:
        dataset = collect_growth_dataset(settings, refresh=False, filters={})
    except GrowthConfigError as exc:
        return {
            "error": {
                "code": exc.code,
                "message": exc.message,
            }
        }
    return {
        "kpis": build_growth_dashboard_kpis(dataset),
        "events": dataset["events"][:10],
        "strategic_insights": dataset["strategic_insights"][:5],
        "company_rollups": dataset["company_rollups"],
        "trend_series": dataset["trend_series"],
    }


def build_growth_dashboard_kpis(dataset: dict) -> list[dict]:
    total_jobs = sum(rollup["jobs_count"] for rollup in dataset["company_rollups"])
    return [
        {"id": "growth_events", "label": "Growth Events", "value": len(dataset["events"])},
        {"id": "growth_insights", "label": "Growth Insights", "value": len(dataset["strategic_insights"])},
        {"id": "tracked_jobs", "label": "Tracked Jobs", "value": total_jobs},
        {"id": "tracked_growth_companies", "label": "Growth Companies", "value": len(dataset["companies"])},
    ]
