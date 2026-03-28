from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
import hashlib
import json

from market_monitor_api.services.api_contract import build_contract_payload, decode_cursor, paginate_records
from market_monitor_api.config import Settings
from market_monitor_api.services.commerce_intelligence import (
    CommerceConfigError,
    build_commerce_signals,
    collect_commerce_dataset,
    normalize_commerce_snapshot,
)
from market_monitor_api.services.growth_intelligence import (
    GrowthConfigError,
    build_growth_comparison_history,
    build_growth_history_event_occurrences,
    collect_growth_dataset,
    normalize_growth_snapshot,
)
from market_monitor_api.services.openai_service import build_cross_category_correlations
from market_monitor_api.services.tinyfish import (
    build_source_health,
    load_snapshots,
    load_source_catalog,
    load_source_runs,
    parse_iso_datetime,
    run_source_refreshes,
    to_iso_timestamp,
    validate_result_against_schema,
)


VALID_MARKET_SIGNAL_CATEGORIES = {
    "commerce_intelligence",
    "growth_intelligence",
    "reputation_intelligence",
}

VALID_MARKET_SIGNAL_SEVERITIES = {
    "critical",
    "high",
    "medium",
    "low",
}

VALID_WIRE_LEVELS = {
    "high",
    "elevated",
    "watch",
}

SIGNAL_LIFECYCLE_STATES = {
    "new",
    "updated",
    "resolved",
    "suppressed",
    "confirmed",
}

SIGNAL_STATE_MANUAL_STATES = {
    "suppressed",
    "confirmed",
}

SEVERITY_RANKS = {
    "critical": 4,
    "high": 3,
    "medium": 2,
    "low": 1,
}

ROLE_SENIORITY_KEYWORDS = {
    "executive": ["chief", "vp", "vice president", "head of", "general manager"],
    "director": ["director"],
    "principal": ["principal", "staff", "distinguished", "lead"],
    "senior": ["senior", "sr"],
}

REGION_SIZE_SCORES = {
    "none": 0.0,
    "city": 0.35,
    "country": 0.7,
    "multi_region": 1.0,
    "marketplace": 0.4,
}

IMPACT_COMPONENT_WEIGHTS = {
    "revenue_exposure": 0.3,
    "region_size": 0.15,
    "role_seniority": 0.15,
    "complaint_volume": 0.15,
    "novelty": 0.15,
    "benchmark_deviation": 0.1,
}

VALID_MARKET_TOPICS = {
    "tech",
    "finance",
}


class MarketSignalsConfigError(Exception):
    def __init__(self, code: str, message: str, status_code: int = 503):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


def build_market_signals_response(settings: Settings, refresh: bool = False, filters: dict | None = None) -> dict:
    dataset = collect_market_signals_dataset(settings, refresh=refresh, filters=filters or {}, include_correlations=True)
    return {
        "contract": build_contract_payload("market_signals", view="wire"),
        "meta": build_market_signals_meta(settings, dataset, refresh),
        "summary": build_market_signals_summary(dataset["view_items"]),
        "wire": {
            "stats": build_wire_stats(dataset["view_items"]),
            "items": dataset["page_items"],
            "pagination": dataset["pagination"],
        },
        "category_status": dataset["category_status"],
        "source_health": dataset["source_health"],
        "sources": dataset["sources"],
        "companies": dataset["companies"],
        "entities": dataset["entities"],
        "correlations": dataset["correlations"],
        "qa": dataset["qa"],
        "map": dataset["map"],
        "recent_runs": dataset["recent_runs"],
        "facets": build_market_signal_facets(dataset["registry_items"]),
    }


def build_market_signal_detail_response(settings: Settings, signal_id: str) -> dict:
    dataset = collect_market_signals_dataset(settings, refresh=False, filters={}, include_correlations=False)
    signal = dataset["items_by_id"].get(signal_id)
    if not signal:
        raise MarketSignalsConfigError("market_signal_not_found", f"Market signal not found: {signal_id}", status_code=404)
    signal_history = dataset["history_by_key"].get(signal["signal_key"], [])
    snapshots_by_id = {snapshot["snapshot_id"]: snapshot for snapshot in dataset["snapshots"]}
    related_signals = build_related_market_signals(signal, dataset["registry_items"])
    return {
        "contract": build_contract_payload("market_signals", view="detail"),
        "meta": {
            "api_version": "v1",
            "module": "market_signals",
            "view": "detail",
            "generated_at": to_iso_timestamp(datetime.now(timezone.utc)),
            "signal_id": signal_id,
        },
        "signal": signal,
        "occurrences": signal_history,
        "related_signals": related_signals,
        "trend_history": build_signal_trend_history(signal_history),
        "source_run_history": build_source_run_history(settings, signal["source_ids"], dataset["snapshots"]),
        "raw_evidence": build_raw_evidence_payload(signal_history, snapshots_by_id),
        "benchmark": signal.get("benchmark"),
        "impact_rubric": signal.get("impact_rubric"),
        "available_lifecycle_states": sorted(SIGNAL_LIFECYCLE_STATES),
    }


def update_market_signal_lifecycle(
    settings: Settings,
    signal_id: str,
    lifecycle_state: str,
    reason: str | None = None,
    actor: str | None = None,
) -> dict:
    if lifecycle_state not in SIGNAL_STATE_MANUAL_STATES:
        raise MarketSignalsConfigError(
            "invalid_market_signal_lifecycle",
            "Lifecycle updates support only confirmed or suppressed.",
            status_code=400,
        )
    dataset = collect_market_signals_dataset(settings, refresh=False, filters={}, include_correlations=False)
    signal = dataset["items_by_id"].get(signal_id)
    if not signal:
        raise MarketSignalsConfigError("market_signal_not_found", f"Market signal not found: {signal_id}", status_code=404)
    overrides = load_signal_lifecycle_overrides(settings)
    overrides[signal["signal_key"]] = {
        "signal_key": signal["signal_key"],
        "state": lifecycle_state,
        "reason": reason.strip() if isinstance(reason, str) and reason.strip() else None,
        "actor": actor.strip() if isinstance(actor, str) and actor.strip() else "api",
        "updated_at": to_iso_timestamp(datetime.now(timezone.utc)),
    }
    persist_signal_lifecycle_overrides(settings, overrides)
    updated_dataset = collect_market_signals_dataset(settings, refresh=False, filters={}, include_correlations=False)
    updated_signal = updated_dataset["items_by_id"].get(signal_id)
    return {
        "contract": build_contract_payload("market_signals", view="lifecycle"),
        "meta": {
            "api_version": "v1",
            "module": "market_signals",
            "view": "lifecycle",
            "generated_at": to_iso_timestamp(datetime.now(timezone.utc)),
        },
        "signal": updated_signal,
        "override": overrides[signal["signal_key"]],
    }


def build_watcher_qa_response(settings: Settings) -> dict:
    sources = load_source_catalog(settings)
    snapshots = load_snapshots(settings)
    source_runs = load_source_runs(settings)
    source_health = []
    category_status = []
    for category in sorted(VALID_MARKET_SIGNAL_CATEGORIES):
        category_result = collect_market_signal_category(settings, category, refresh=False, filters={})
        source_health.extend(category_result["source_health"])
        category_status.append(category_result["status"])
    qa = build_watcher_qa_payload(settings, sources, snapshots, source_health)
    return {
        "contract": build_contract_payload("watcher_qa", view="overview"),
        "meta": {
            "api_version": "v1",
            "module": "watcher_qa",
            "generated_at": to_iso_timestamp(datetime.now(timezone.utc)),
            "source_count": len(sources),
            "snapshot_count": len(snapshots),
            "run_count": len(source_runs),
        },
        "category_status": category_status,
        "source_health": sort_market_signal_source_health(source_health),
        "qa": qa,
    }


def replay_watcher_snapshots(settings: Settings, payload: dict | None = None) -> dict:
    payload = payload or {}
    source_id = payload.get("source_id")
    requested_snapshot_ids = payload.get("snapshot_ids") if isinstance(payload.get("snapshot_ids"), list) else None
    snapshot_id_set = {
        snapshot_id
        for snapshot_id in (requested_snapshot_ids or [])
        if isinstance(snapshot_id, str) and snapshot_id.strip()
    }
    sources = load_source_catalog(settings)
    source_map = {source["id"]: source for source in sources}
    snapshots = load_snapshots(settings)
    selected = []
    for snapshot in snapshots:
        if source_id and snapshot["source_id"] != source_id:
            continue
        if snapshot_id_set and snapshot["snapshot_id"] not in snapshot_id_set:
            continue
        selected.append(snapshot)
    if not selected:
        raise MarketSignalsConfigError("replay_selection_empty", "No snapshots matched the replay request.", status_code=404)
    replayed = []
    for snapshot in selected:
        source = source_map.get(snapshot["source_id"])
        if not source:
            continue
        replayed.append(build_snapshot_replay_result(source, snapshot))
    return {
        "contract": build_contract_payload("watcher_qa", view="replay"),
        "meta": {
            "api_version": "v1",
            "module": "watcher_qa",
            "view": "replay",
            "generated_at": to_iso_timestamp(datetime.now(timezone.utc)),
            "requested_source_id": source_id,
            "requested_snapshot_count": len(snapshot_id_set),
            "replayed_count": len(replayed),
        },
        "summary": {
            "normalized_count": len([item for item in replayed if item["replay_status"] == "normalized"]),
            "rejected_count": len([item for item in replayed if item["replay_status"] != "normalized"]),
        },
        "replayed": replayed,
    }


def collect_market_signals_dataset(
    settings: Settings,
    refresh: bool,
    filters: dict,
    include_correlations: bool,
) -> dict:
    requested_categories = select_requested_categories(settings, filters)
    current_occurrences = []
    history_occurrences = []
    source_health = []
    source_records = []
    company_records = []
    snapshots = []
    latest_snapshot_candidates = []
    category_status = []
    for category in requested_categories:
        result = collect_market_signal_category(settings, category, refresh, filters)
        current_occurrences.extend(result["current_occurrences"])
        history_occurrences.extend(result["history_occurrences"])
        source_health.extend(result["source_health"])
        source_records.extend(result["sources"])
        company_records.extend(result["companies"])
        snapshots.extend(result["snapshots"])
        category_status.append(result["status"])
        latest_snapshot_at = result["status"].get("latest_snapshot_at")
        if latest_snapshot_at:
            latest_snapshot_candidates.append(latest_snapshot_at)
    entities = build_canonical_entities(source_records, history_occurrences, snapshots)
    signal_registry = build_signal_registry(settings, current_occurrences, history_occurrences, entities)
    view_items = build_filtered_market_signal_view(signal_registry["registry_items"], filters)
    cursor = decode_cursor(filters.get("cursor"))
    limit = filters.get("limit", 50)
    page_items, pagination = paginate_records(view_items, cursor, limit)
    correlations = (
        build_cross_category_correlations(signal_registry["active_items"], entities["companies"], settings)
        if include_correlations
        else []
    )
    qa = build_watcher_qa_payload(settings, source_records, snapshots, source_health)
    signal_map = build_signal_map_payload(signal_registry["active_items"])
    relevant_source_ids = [source["source_id"] for source in deduplicate_market_signal_sources(source_records)]
    return {
        "registry_items": signal_registry["registry_items"],
        "active_items": signal_registry["active_items"],
        "resolved_items": signal_registry["resolved_items"],
        "items_by_id": signal_registry["items_by_id"],
        "history_by_key": signal_registry["history_by_key"],
        "view_items": view_items,
        "page_items": page_items,
        "pagination": pagination,
        "category_status": category_status,
        "source_health": sort_market_signal_source_health(source_health),
        "sources": deduplicate_market_signal_sources(source_records),
        "companies": deduplicate_market_signal_companies(company_records),
        "entities": entities,
        "correlations": correlations,
        "qa": qa,
        "map": signal_map,
        "filters": filters,
        "latest_snapshot_at": max(latest_snapshot_candidates, key=parse_iso_datetime) if latest_snapshot_candidates else None,
        "snapshots": deduplicate_snapshots(snapshots),
        "recent_runs": build_recent_source_runs(settings, relevant_source_ids, limit=40),
    }


def select_requested_categories(settings: Settings, filters: dict) -> list[str]:
    category = filters.get("category")
    if category in VALID_MARKET_SIGNAL_CATEGORIES:
        return [category]
    try:
        sources = load_source_catalog(settings)
    except Exception:
        sources = []
    if any(source["category"] == "reputation_intelligence" for source in sources):
        return ["reputation_intelligence"]
    return sorted(VALID_MARKET_SIGNAL_CATEGORIES)


def collect_market_signal_category(settings: Settings, category: str, refresh: bool, filters: dict) -> dict:
    if category == "commerce_intelligence":
        return collect_market_signal_commerce_category(settings, refresh, filters)
    if category == "growth_intelligence":
        return collect_market_signal_growth_category(settings, refresh, filters)
    if category == "reputation_intelligence":
        return collect_market_signal_reputation_category(settings, refresh, filters)
    raise MarketSignalsConfigError("market_signal_category_invalid", f"Unsupported category: {category}", status_code=400)


def collect_market_signal_commerce_category(settings: Settings, refresh: bool, filters: dict) -> dict:
    try:
        dataset = collect_commerce_dataset(
            settings,
            refresh=refresh,
            filters=build_commerce_market_signal_filters(filters),
        )
    except CommerceConfigError as exc:
        return build_market_signal_error_category("commerce_intelligence", exc)
    source_map = {source["id"]: source for source in dataset["sources"]}
    comparison_map = {comparison["comparison_id"]: comparison for comparison in dataset["comparisons"]}
    latest_snapshot_by_source = build_latest_snapshot_by_source(dataset["snapshots"])
    history_occurrences = [
        build_commerce_market_signal_occurrence(signal, source_map, comparison_map, latest_snapshot_by_source)
        for signal in dataset["signals"]
    ]
    latest_comparisons = build_current_commerce_comparisons(dataset["comparisons"])
    current_signals = build_commerce_signals(
        dataset["sources"],
        dataset["snapshots"],
        latest_comparisons,
        dataset["current_listings"],
    )
    signal_map = {signal["id"]: signal for signal in dataset["signals"]}
    current_occurrences = [
        build_commerce_market_signal_occurrence(
            merge_current_signal_ai_fields(signal, signal_map),
            source_map,
            comparison_map,
            latest_snapshot_by_source,
        )
        for signal in current_signals
    ]
    return {
        "current_occurrences": [item for item in current_occurrences if item],
        "history_occurrences": [item for item in history_occurrences if item],
        "source_health": build_market_signal_source_health("commerce_intelligence", dataset["sources"], dataset["source_health"]),
        "sources": build_market_signal_sources("commerce_intelligence", dataset["sources"]),
        "companies": build_market_signal_companies("commerce_intelligence", dataset["sources"]),
        "status": build_market_signal_category_status(
            "commerce_intelligence",
            len(dataset["sources"]),
            len(dataset["snapshots"]),
            len([item for item in current_occurrences if item]),
            dataset["snapshots"][-1]["captured_at"] if dataset["snapshots"] else None,
            dataset["source_health"],
        ),
        "snapshots": dataset["snapshots"],
    }


def collect_market_signal_growth_category(settings: Settings, refresh: bool, filters: dict) -> dict:
    try:
        dataset = collect_growth_dataset(
            settings,
            refresh=refresh,
            filters=build_growth_market_signal_filters(filters),
        )
    except GrowthConfigError as exc:
        return build_market_signal_error_category("growth_intelligence", exc)
    source_map = {source["id"]: source for source in dataset["sources"]}
    latest_snapshot_pairs = build_latest_snapshot_pairs_by_source(dataset["snapshots"])
    snapshot_map = {snapshot["snapshot_id"]: snapshot for snapshot in dataset["snapshots"]}
    insight_map = build_growth_signal_insight_map(dataset["strategic_insights"])
    current_occurrences = [
        build_growth_market_signal_occurrence(event, source_map, latest_snapshot_pairs, insight_map)
        for event in dataset["events"]
    ]
    historical_comparisons = build_growth_comparison_history(dataset["sources"], dataset["snapshots"])
    history_occurrences = [
        build_growth_market_signal_history_occurrence(event, source_map, snapshot_map)
        for event in build_growth_history_event_occurrences(historical_comparisons)
    ]
    return {
        "current_occurrences": [item for item in current_occurrences if item],
        "history_occurrences": [item for item in history_occurrences if item],
        "source_health": build_market_signal_source_health("growth_intelligence", dataset["sources"], dataset["source_health"]),
        "sources": build_market_signal_sources("growth_intelligence", dataset["sources"]),
        "companies": build_market_signal_companies("growth_intelligence", dataset["sources"]),
        "status": build_market_signal_category_status(
            "growth_intelligence",
            len(dataset["sources"]),
            len(dataset["snapshots"]),
            len([item for item in current_occurrences if item]),
            dataset["snapshots"][-1]["captured_at"] if dataset["snapshots"] else None,
            dataset["source_health"],
        ),
        "snapshots": dataset["snapshots"],
    }


def collect_market_signal_reputation_category(settings: Settings, refresh: bool, filters: dict) -> dict:
    try:
        all_sources = load_source_catalog(settings)
    except FileNotFoundError as exc:
        return build_market_signal_error_category("reputation_intelligence", exc)
    except ValueError as exc:
        return build_market_signal_error_category("reputation_intelligence", exc)
    sources = [source for source in all_sources if source["category"] == "reputation_intelligence"]
    filtered_sources = [source for source in sources if matches_reputation_source_filters(source, filters)]
    if refresh and filtered_sources:
        if not settings.tinyfish_configured:
            return build_market_signal_error_category(
                "reputation_intelligence",
                MarketSignalsConfigError(
                    "tinyfish_not_configured",
                    "TinyFish is required for refresh=true. Set TINYFISH_API_KEY in the .env file.",
                ),
            )
        run_source_refreshes(settings, filtered_sources)
    all_snapshots = load_snapshots(settings)
    snapshots = build_reputation_market_snapshots(filtered_sources, all_snapshots)
    current_occurrences = build_current_reputation_occurrences(snapshots, filtered_sources)
    history_occurrences = build_history_reputation_occurrences(snapshots, filtered_sources)
    source_health = build_source_health(settings, filtered_sources, all_snapshots)
    return {
        "current_occurrences": [item for item in current_occurrences if item],
        "history_occurrences": [item for item in history_occurrences if item],
        "source_health": build_market_signal_source_health("reputation_intelligence", filtered_sources, source_health),
        "sources": build_market_signal_sources("reputation_intelligence", filtered_sources),
        "companies": build_market_signal_companies("reputation_intelligence", filtered_sources),
        "status": build_market_signal_category_status(
            "reputation_intelligence",
            len(filtered_sources),
            len(snapshots),
            len([item for item in current_occurrences if item]),
            snapshots[-1]["captured_at"] if snapshots else None,
            source_health,
        ),
        "snapshots": snapshots,
    }


def build_market_signal_error_category(category: str, exc: Exception) -> dict:
    return {
        "current_occurrences": [],
        "history_occurrences": [],
        "source_health": [],
        "sources": [],
        "companies": [],
        "snapshots": [],
        "status": {
            "category": category,
            "status": "error",
            "source_count": 0,
            "snapshot_count": 0,
            "active_count": 0,
            "latest_snapshot_at": None,
            "error": {
                "code": getattr(exc, "code", "category_unavailable"),
                "message": getattr(exc, "message", str(exc)),
            },
        },
    }


def build_commerce_market_signal_filters(filters: dict) -> dict:
    mapped = {}
    for key in ("source_id", "company_id", "marketplace"):
        if filters.get(key):
            mapped[key] = filters[key]
    return mapped


def build_growth_market_signal_filters(filters: dict) -> dict:
    mapped = {}
    for key in ("source_id", "company_id", "signal_type", "location"):
        if filters.get(key):
            mapped[key] = filters[key]
    return mapped


def matches_reputation_source_filters(source: dict, filters: dict) -> bool:
    for key in ("source_id", "company_id"):
        filter_value = filters.get(key)
        if filter_value and source.get("id" if key == "source_id" else key) != filter_value:
            return False
    market_category = filters.get("market_category")
    if market_category and source.get("company_id") != f"market-{market_category}":
        return False
    return True


def build_reputation_market_snapshots(sources: list[dict], all_snapshots: list[dict]) -> list[dict]:
    source_map = {source["id"]: source for source in sources}
    snapshots = []
    for snapshot in all_snapshots:
        source = source_map.get(snapshot["source_id"])
        if not source:
            continue
        normalized = normalize_reputation_market_snapshot(snapshot, source)
        if normalized:
            snapshots.append(normalized)
    return sorted(snapshots, key=lambda item: parse_iso_datetime(item["captured_at"]))


def normalize_reputation_market_snapshot(snapshot: dict, source: dict) -> dict | None:
    if snapshot.get("capture_status") not in {"COMPLETED", "VALIDATION_ERROR"}:
        return None
    result = snapshot.get("result")
    if not isinstance(result, dict):
        return None
    captured_at = select_market_snapshot_captured_at(result.get("captured_at"), snapshot.get("captured_at"))
    if not isinstance(captured_at, str) or not captured_at.strip():
        return None
    articles = collect_market_articles(result)
    metrics = build_market_metrics(result, articles)
    if metrics is None:
        return None
    return {
        "snapshot_id": snapshot["snapshot_id"],
        "captured_at": captured_at.strip(),
        "source_id": source["id"],
        "source_name": source["name"],
        "company_id": source["company_id"],
        "company_name": source["company_name"],
        "target_url": snapshot["target_url"],
        "schedule": source.get("schedule"),
        "articles": articles,
        "metrics": metrics,
        "map_points": result.get("map_points") if isinstance(result.get("map_points"), list) else [],
        "raw_result": result,
        "file_path": snapshot.get("file_path"),
        "run": snapshot.get("run"),
    }


def select_market_snapshot_captured_at(result_captured_at, snapshot_captured_at) -> str | None:
    if isinstance(result_captured_at, str) and result_captured_at.strip():
        normalized = result_captured_at.strip()
        if "T" in normalized and any(token in normalized for token in ["Z", "+"]):
            return normalized
    if isinstance(snapshot_captured_at, str) and snapshot_captured_at.strip():
        return snapshot_captured_at.strip()
    if isinstance(result_captured_at, str) and result_captured_at.strip():
        return result_captured_at.strip()
    return None


def collect_market_articles(result: dict) -> list[dict]:
    raw_articles = result.get("articles") or result.get("stories") or result.get("items")
    if not isinstance(raw_articles, list):
        return []
    articles = []
    for item in raw_articles:
        normalized = normalize_market_article(item)
        if normalized:
            articles.append(normalized)
    return articles


def normalize_market_article(item: dict) -> dict | None:
    if not isinstance(item, dict):
        return None
    title = first_string(item, ["title", "headline", "name"])
    summary = first_string(item, ["summary", "description", "excerpt"])
    published_at = first_string(item, ["published_at", "published", "timestamp", "date"])
    article_url = first_string(item, ["article_url", "url", "link"])
    signal_type = first_string(item, ["signal_type", "story_type", "topic", "category"])
    market_category = first_string(item, ["market_category", "market", "sector", "topic"])
    severity = first_string(item, ["severity", "priority"])
    if not all(isinstance(value, str) and value.strip() for value in (title, published_at, article_url)):
        return None
    normalized_market_category = normalize_market_category_value(market_category or title or "")
    if normalized_market_category not in VALID_MARKET_TOPICS:
        return None
    normalized_signal_type = normalize_article_signal_type(signal_type, normalized_market_category, title, summary or "")
    normalized_severity = normalize_article_severity(severity, normalized_market_category, title, summary or "")
    if normalized_severity not in VALID_MARKET_SIGNAL_SEVERITIES:
        return None
    companies = item.get("companies") or item.get("mentioned_companies") or item.get("company_names")
    regions = item.get("regions") or item.get("locations") or item.get("markets")
    companies = [value.strip() for value in companies if isinstance(value, str) and value.strip()] if isinstance(companies, list) else []
    regions = [value.strip() for value in regions if isinstance(value, str) and value.strip()] if isinstance(regions, list) else []
    return {
        "id": build_canonical_id("article", article_url),
        "title": title.strip(),
        "summary": (summary or title).strip(),
        "published_at": published_at.strip(),
        "article_url": article_url.strip(),
        "signal_type": normalized_signal_type,
        "market_category": normalized_market_category,
        "severity": normalized_severity,
        "companies": companies,
        "regions": regions,
    }


def build_market_metrics(result: dict, articles: list[dict]) -> dict | None:
    metrics = result.get("metrics")
    if isinstance(metrics, dict):
        story_count = metrics.get("story_count") or metrics.get("count")
        if isinstance(story_count, (int, float)) and not isinstance(story_count, bool):
            return {"story_count": float(story_count)}
    if articles:
        return {"story_count": float(len(articles))}
    return None


def first_string(payload: dict, keys: list[str]) -> str | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def normalize_market_category_value(value: str) -> str | None:
    normalized = value.strip().lower()
    if normalized in {"tech", "technology", "ai", "artificial intelligence", "software", "developer"}:
        return "tech"
    if normalized in {"finance", "financial", "fintech", "banking", "payments", "markets"}:
        return "finance"
    if any(
        keyword in normalized
        for keyword in [
            "tech",
            "ai",
            "software",
            "chip",
            "developer",
            "platform",
            "infrastructure",
            "semiconductor",
            "hardware",
            "display",
            "telecom",
            "operating system",
        ]
    ):
        return "tech"
    if any(keyword in normalized for keyword in ["finance", "fintech", "bank", "payment", "market", "insurance", "trading"]):
        return "finance"
    return None


def normalize_article_signal_type(raw_value: str | None, market_category: str, title: str, summary: str) -> str:
    haystack = " ".join(part for part in [raw_value or "", title, summary] if isinstance(part, str)).lower()
    if any(keyword in haystack for keyword in ["fund", "raise", "investment", "funding"]):
        return "funding_signal"
    if any(keyword in haystack for keyword in ["launch", "release", "announce", "ship"]):
        return "product_signal"
    if any(keyword in haystack for keyword in ["hire", "job", "talent", "layoff"]):
        return "workforce_signal"
    if market_category == "finance":
        return "finance_news"
    return "tech_news"


def normalize_article_severity(raw_value: str | None, market_category: str, title: str, summary: str) -> str:
    if isinstance(raw_value, str) and raw_value.strip().lower() in VALID_MARKET_SIGNAL_SEVERITIES:
        return raw_value.strip().lower()
    haystack = " ".join(part for part in [title, summary] if isinstance(part, str)).lower()
    if any(keyword in haystack for keyword in ["surge", "crash", "plunge", "record", "breakthrough", "major", "largest"]):
        return "high"
    if any(keyword in haystack for keyword in ["launch", "raise", "acquire", "earnings", "market", "chip", "ai"]):
        return "medium"
    if market_category == "finance":
        return "medium"
    return "low"


def build_current_reputation_occurrences(snapshots: list[dict], sources: list[dict]) -> list[dict]:
    source_map = {source["id"]: source for source in sources}
    latest_by_source = build_latest_snapshot_by_source(snapshots)
    occurrences = []
    for source_id, snapshot in latest_by_source.items():
        source = source_map.get(source_id)
        if not source:
            continue
        for article in snapshot["articles"]:
            occurrences.append(build_reputation_market_signal_occurrence(article, snapshot, source))
    return sort_market_signal_items(occurrences)


def build_history_reputation_occurrences(snapshots: list[dict], sources: list[dict]) -> list[dict]:
    source_map = {source["id"]: source for source in sources}
    occurrences = []
    for snapshot in snapshots:
        source = source_map.get(snapshot["source_id"])
        if not source:
            continue
        for article in snapshot["articles"]:
            occurrences.append(build_reputation_market_signal_occurrence(article, snapshot, source))
    return sort_market_signal_items(occurrences)


def build_reputation_market_signal_occurrence(article: dict, snapshot: dict, source: dict) -> dict:
    market_category = article["market_category"]
    content_hash = build_market_signal_content_hash(
        {
            "title": article["title"],
            "summary": article["summary"],
            "article_url": article["article_url"],
            "signal_type": article["signal_type"],
            "market_category": market_category,
            "severity": article["severity"],
            "companies": article["companies"],
            "regions": article["regions"],
        }
    )
    return {
        "id": f"{snapshot['snapshot_id']}::{article['id']}",
        "occurrence_id": f"{snapshot['snapshot_id']}::{article['id']}",
        "content_hash": content_hash,
        "signal_key": "::".join(["reputation", source["id"], article["article_url"]]),
        "title": article["title"],
        "summary": article["summary"],
        "category": "reputation_intelligence",
        "signal_type": article["signal_type"],
        "severity": article["severity"],
        "wire_level": build_wire_level(article["severity"]),
        "timestamp": article["published_at"],
        "relative_time_label": build_relative_time_label(article["published_at"]),
        "company_id": source["company_id"],
        "company_name": source["company_name"],
        "competitor_id": None,
        "competitor_name": None,
        "product_id": None,
        "product_name": None,
        "sku": None,
        "marketplace": None,
        "market_category": market_category,
        "market_categories": [market_category],
        "location_label": article["regions"][0] if article["regions"] else None,
        "locations": article["regions"],
        "tracking_group_id": None,
        "source_id": source["id"],
        "source_ids": [source["id"]],
        "source_name": source["name"],
        "source_names": [source["name"]],
        "source_types": ["market_news"],
        "sector": market_category,
        "geography": None,
        "product_category": None,
        "revenue_exposure_weight": source.get("revenue_exposure_weight"),
        "confidence_score": None,
        "ai_impact_score": None,
        "impact_score": None,
        "impact_rubric": None,
        "lifecycle_state": None,
        "benchmark": None,
        "detail_url": f"/api/v1/market-signals/{snapshot['snapshot_id']}::{article['id']}",
        "tags": [article["signal_type"], market_category, source["name"]],
        "evidence": [
            {
                "label": article["title"],
                "url": article["article_url"],
                "location": article["regions"][0] if article["regions"] else None,
                "timestamp": article["published_at"],
            }
        ],
        "provenance": {
            "source_ids": [source["id"]],
            "snapshot_ids": [snapshot["snapshot_id"]],
            "extraction_timestamps": [snapshot["captured_at"], article["published_at"]],
            "evidence_urls": [article["article_url"]],
            "target_urls": [source["target_url"]],
            "file_paths": [snapshot["file_path"]] if isinstance(snapshot.get("file_path"), str) else [],
            "run_ids": [snapshot.get("run", {}).get("run_id")] if snapshot.get("run", {}).get("run_id") else [],
        },
        "map_points": [],
        "detail": {
            "current_value": 1,
            "previous_value": 0,
            "delta": 1,
            "delta_ratio": 1.0,
            "article_url": article["article_url"],
            "mentioned_companies": article["companies"],
            "regions": article["regions"],
            "target_url": source["target_url"],
        },
    }


def build_market_signal_sources(category: str, sources: list[dict]) -> list[dict]:
    return [
        {
            "category": category,
            "source_id": source["id"],
            "source_name": source["name"],
            "company_id": source["company_id"],
            "company_name": source["company_name"],
            "target_url": source["target_url"],
            "sector": source.get("sector"),
            "geography": source.get("geography"),
            "product_category": source.get("product_category"),
            "revenue_exposure_weight": source.get("revenue_exposure_weight"),
            "schedule": source.get("schedule"),
            "watcher": source.get("watcher"),
        }
        for source in sources
    ]


def build_market_signal_companies(category: str, sources: list[dict]) -> list[dict]:
    companies = {}
    for source in sources:
        record = companies.setdefault(
            source["company_id"],
            {
                "company_id": source["company_id"],
                "company_name": source["company_name"],
                "categories": set(),
                "sector": source.get("sector"),
                "geography": source.get("geography"),
                "product_category": source.get("product_category"),
            },
        )
        record["categories"].add(category)
    return [
        {
            "company_id": company["company_id"],
            "company_name": company["company_name"],
            "categories": sorted(company["categories"]),
            "sector": company.get("sector"),
            "geography": company.get("geography"),
            "product_category": company.get("product_category"),
        }
        for company in sorted(companies.values(), key=lambda item: item["company_name"])
    ]


def build_market_signal_source_health(category: str, sources: list[dict], records: list[dict]) -> list[dict]:
    source_map = {source["id"]: source for source in sources}
    enriched = []
    for record in records:
        source = source_map.get(record["source_id"])
        enriched.append(
            {
                **record,
                "category": category,
                "source_name": source["name"] if source else None,
                "company_id": source["company_id"] if source else None,
                "company_name": source["company_name"] if source else None,
                "schedule": source.get("schedule") if source else None,
            }
        )
    return enriched


def build_recent_source_runs(settings: Settings, source_ids: list[str], limit: int = 40) -> list[dict]:
    source_id_set = set(source_ids)
    runs = [
        record
        for record in load_source_runs(settings)
        if record["source_id"] in source_id_set
    ]
    runs = sorted(runs, key=lambda item: parse_iso_datetime(item["captured_at"]), reverse=True)
    return [
        {
            "run_record_id": record["run_record_id"],
            "source_id": record["source_id"],
            "source_name": record.get("source_name"),
            "category": record.get("category"),
            "company_id": record.get("company_id"),
            "company_name": record.get("company_name"),
            "captured_at": record["captured_at"],
            "capture_status": record["capture_status"],
            "change_state": record.get("change_state"),
            "snapshot_persisted": record.get("snapshot_persisted"),
            "observed_snapshot_id": record.get("observed_snapshot_id"),
            "canonical_snapshot_id": record.get("canonical_snapshot_id"),
            "duplicate_of_snapshot_id": record.get("duplicate_of_snapshot_id"),
            "target_url": record.get("target_url"),
            "run": record.get("run"),
        }
        for record in runs[:limit]
    ]


def build_market_signal_category_status(
    category: str,
    source_count: int,
    snapshot_count: int,
    active_count: int,
    latest_snapshot_at: str | None,
    source_health: list[dict],
) -> dict:
    return {
        "category": category,
        "status": classify_market_signal_category_status(source_count, snapshot_count, active_count, source_health),
        "source_count": source_count,
        "snapshot_count": snapshot_count,
        "active_count": active_count,
        "latest_snapshot_at": latest_snapshot_at,
        "error": build_market_signal_category_error(source_health),
    }


def classify_market_signal_category_status(
    source_count: int,
    snapshot_count: int,
    active_count: int,
    source_health: list[dict],
) -> str:
    if source_count == 0:
        return "not_configured"
    if any(record["status"] in {"failed", "validation_error"} for record in source_health):
        return "degraded"
    if active_count > 0:
        return "active"
    if snapshot_count > 0:
        return "quiet"
    return "tracking"


def build_market_signal_category_error(source_health: list[dict]) -> dict | None:
    for record in source_health:
        if record.get("last_error"):
            return record["last_error"]
    return None


def build_commerce_market_signal_occurrence(
    signal: dict,
    source_map: dict[str, dict],
    comparison_map: dict[str, dict],
    latest_snapshot_by_source: dict[str, dict],
) -> dict | None:
    source = source_map.get(signal["source_id"])
    comparison = comparison_map.get(extract_commerce_comparison_id(signal["id"]))
    latest_snapshot = latest_snapshot_by_source.get(signal["source_id"])
    marketplace = signal.get("marketplace")
    provenance = build_commerce_signal_provenance(signal, source, comparison, latest_snapshot)
    return {
        "id": signal["id"],
        "occurrence_id": signal["id"],
        "signal_key": build_commerce_signal_key(signal),
        "title": build_commerce_market_signal_title(signal),
        "summary": build_commerce_market_signal_summary(signal),
        "category": signal["category"],
        "signal_type": signal["signal_type"],
        "severity": signal["severity"],
        "wire_level": build_wire_level(signal["severity"]),
        "timestamp": signal["timestamp"],
        "relative_time_label": build_relative_time_label(signal["timestamp"]),
        "company_id": signal["company_id"],
        "company_name": signal["company_name"],
        "competitor_id": signal.get("competitor_id"),
        "competitor_name": signal.get("competitor_name"),
        "product_id": signal.get("product_id"),
        "product_name": signal.get("product_name"),
        "sku": signal.get("sku"),
        "marketplace": marketplace,
        "market_category": None,
        "market_categories": [],
        "location_label": marketplace,
        "locations": [marketplace] if marketplace else [],
        "tracking_group_id": signal.get("tracking_group_id"),
        "source_id": signal["source_id"],
        "source_ids": [signal["source_id"]],
        "source_name": signal["source_name"],
        "source_names": [signal["source_name"]],
        "source_types": [],
        "sector": source.get("sector") if source else None,
        "geography": source.get("geography") if source else None,
        "product_category": source.get("product_category") if source else None,
        "revenue_exposure_weight": source.get("revenue_exposure_weight") if source else None,
        "confidence_score": signal.get("confidence_score"),
        "ai_impact_score": signal.get("impact_score"),
        "impact_score": None,
        "impact_rubric": None,
        "lifecycle_state": None,
        "benchmark": None,
        "detail_url": f"/api/v1/market-signals/{signal['id']}",
        "tags": build_commerce_market_signal_tags(signal),
        "evidence": build_commerce_market_signal_evidence(signal),
        "provenance": provenance,
        "map_points": [],
        "detail": {
            "current_value": signal.get("current_value"),
            "previous_value": signal.get("previous_value"),
            "delta": signal.get("delta"),
            "seller": signal.get("seller"),
            "product_url": signal.get("product_url"),
            "image_url": signal.get("image_url"),
            "target_url": source["target_url"] if source else None,
            "comparison_id": comparison["comparison_id"] if comparison else None,
        },
    }


def build_commerce_market_signal_title(signal: dict) -> str:
    marketplace_label = format_simple_label(signal.get("marketplace"))
    competitor_name = signal["competitor_name"]
    signal_type = signal["signal_type"]
    if signal_type == "price_drop":
        return f"{competitor_name} price dropped on {marketplace_label}"
    if signal_type == "price_increase":
        return f"{competitor_name} price increased on {marketplace_label}"
    if signal_type == "flash_sale":
        return f"{competitor_name} flash sale detected on {marketplace_label}"
    if signal_type == "inventory_shift":
        stock_status = signal["current_value"]["stock_status"].replace("_", " ")
        return f"{competitor_name} inventory shifted to {stock_status}"
    if signal_type == "undercutting":
        return f"{competitor_name} is undercutting on {marketplace_label}"
    return f"{competitor_name} commerce signal on {marketplace_label}"


def build_commerce_market_signal_summary(signal: dict) -> str:
    insight = signal.get("insight")
    if isinstance(insight, dict) and isinstance(insight.get("summary"), str) and insight["summary"].strip():
        return insight["summary"].strip()
    signal_type = signal["signal_type"]
    product_name = signal["product_name"]
    marketplace_label = format_simple_label(signal.get("marketplace"))
    if signal_type in {"price_drop", "price_increase"}:
        current_price = format_money(signal["current_value"].get("price"), signal["current_value"].get("currency"))
        previous_price = format_money(signal["previous_value"].get("price"), signal["previous_value"].get("currency"))
        return f"{product_name} moved from {previous_price} to {current_price} on {marketplace_label}."
    if signal_type == "flash_sale":
        return f"{product_name} now shows {signal['current_value'].get('discount_percent')}% discount on {marketplace_label}."
    if signal_type == "inventory_shift":
        current_status = signal["current_value"].get("stock_status", "").replace("_", " ")
        previous_status = signal["previous_value"].get("stock_status", "").replace("_", " ")
        return f"{product_name} changed stock state from {previous_status} to {current_status}."
    if signal_type == "undercutting":
        current_price = format_money(signal["current_value"].get("price"), signal["current_value"].get("currency"))
        reference_price = format_money(
            signal["current_value"].get("competitor_reference_price"),
            signal["current_value"].get("currency"),
        )
        return f"{product_name} is listed at {current_price} versus {reference_price} for the next best tracked offer."
    return product_name


def build_commerce_market_signal_tags(signal: dict) -> list[str]:
    tags = [signal["signal_type"], signal["severity"]]
    if signal.get("marketplace"):
        tags.append(signal["marketplace"])
    if signal.get("sku"):
        tags.append(signal["sku"])
    return tags


def build_commerce_market_signal_evidence(signal: dict) -> list[dict]:
    if not signal.get("product_url"):
        return []
    return [
        {
            "label": signal["product_name"],
            "url": signal["product_url"],
            "location": signal.get("marketplace"),
            "timestamp": signal["timestamp"],
        }
    ]


def build_growth_market_signal_occurrence(
    event: dict,
    source_map: dict[str, dict],
    latest_snapshot_pairs: dict[str, dict],
    insight_map: dict[str, dict],
) -> dict | None:
    primary_source_id = event["source_ids"][0] if event["source_ids"] else None
    primary_source = source_map.get(primary_source_id) if primary_source_id else None
    source_names = [source_map[source_id]["name"] for source_id in event["source_ids"] if source_id in source_map]
    insight = insight_map.get(event["id"])
    provenance = build_growth_signal_provenance(event, source_map, latest_snapshot_pairs)
    map_points = build_growth_map_points(event, latest_snapshot_pairs)
    return {
        "id": event["id"],
        "occurrence_id": event["id"],
        "signal_key": build_growth_signal_key(event),
        "title": event["title"],
        "summary": event["summary"],
        "category": event["category"],
        "signal_type": event["signal_type"],
        "severity": event["severity"],
        "wire_level": build_wire_level(event["severity"]),
        "timestamp": event["timestamp"],
        "relative_time_label": build_relative_time_label(event["timestamp"]),
        "company_id": event["company_id"],
        "company_name": event["company_name"],
        "competitor_id": None,
        "competitor_name": None,
        "product_id": None,
        "product_name": None,
        "sku": None,
        "marketplace": None,
        "market_category": infer_growth_market_category(event),
        "market_categories": infer_growth_market_categories(event),
        "location_label": event["locations"][0] if event["locations"] else None,
        "locations": event["locations"],
        "tracking_group_id": None,
        "source_id": primary_source_id,
        "source_ids": event["source_ids"],
        "source_name": primary_source["name"] if primary_source else None,
        "source_names": source_names,
        "source_types": event.get("source_types", []),
        "sector": primary_source.get("sector") if primary_source else None,
        "geography": primary_source.get("geography") if primary_source else None,
        "product_category": primary_source.get("product_category") if primary_source else None,
        "revenue_exposure_weight": primary_source.get("revenue_exposure_weight") if primary_source else None,
        "confidence_score": insight["confidence_score"] if insight else None,
        "ai_impact_score": insight["impact_score"] if insight else None,
        "impact_score": None,
        "impact_rubric": None,
        "lifecycle_state": None,
        "benchmark": None,
        "detail_url": f"/api/v1/market-signals/{event['id']}",
        "tags": build_growth_market_signal_tags(event),
        "evidence": event["evidence"],
        "provenance": provenance,
        "map_points": map_points,
        "detail": {
            "current_value": event["current_value"],
            "previous_value": event["previous_value"],
            "delta": event["delta"],
            "delta_ratio": event["delta_ratio"],
            "cluster_name": event["cluster_name"],
            "target_url": primary_source["target_url"] if primary_source else None,
            "strategic_direction": insight["strategic_direction"] if insight else None,
        },
    }


def build_growth_market_signal_history_occurrence(
    event: dict,
    source_map: dict[str, dict],
    snapshot_map: dict[str, dict],
) -> dict | None:
    primary_source_id = event["source_ids"][0] if event["source_ids"] else None
    primary_source = source_map.get(primary_source_id) if primary_source_id else None
    source_names = [source_map[source_id]["name"] for source_id in event["source_ids"] if source_id in source_map]
    provenance = build_history_growth_signal_provenance(event, source_map, snapshot_map)
    return {
        "id": event["id"],
        "occurrence_id": event["id"],
        "signal_key": build_growth_signal_key(event),
        "title": event["title"],
        "summary": event["summary"],
        "category": event["category"],
        "signal_type": event["signal_type"],
        "severity": event["severity"],
        "wire_level": build_wire_level(event["severity"]),
        "timestamp": event["timestamp"],
        "relative_time_label": build_relative_time_label(event["timestamp"]),
        "company_id": event["company_id"],
        "company_name": event["company_name"],
        "competitor_id": None,
        "competitor_name": None,
        "product_id": None,
        "product_name": None,
        "sku": None,
        "marketplace": None,
        "market_category": infer_growth_market_category(event),
        "market_categories": infer_growth_market_categories(event),
        "location_label": event["locations"][0] if event["locations"] else None,
        "locations": event["locations"],
        "tracking_group_id": None,
        "source_id": primary_source_id,
        "source_ids": event["source_ids"],
        "source_name": primary_source["name"] if primary_source else None,
        "source_names": source_names,
        "source_types": event.get("source_types", []),
        "sector": primary_source.get("sector") if primary_source else None,
        "geography": primary_source.get("geography") if primary_source else None,
        "product_category": primary_source.get("product_category") if primary_source else None,
        "revenue_exposure_weight": primary_source.get("revenue_exposure_weight") if primary_source else None,
        "confidence_score": None,
        "ai_impact_score": None,
        "impact_score": None,
        "impact_rubric": None,
        "lifecycle_state": None,
        "benchmark": None,
        "detail_url": f"/api/v1/market-signals/{event['id']}",
        "tags": build_growth_market_signal_tags(event),
        "evidence": event["evidence"],
        "provenance": provenance,
        "map_points": [],
        "detail": {
            "current_value": event["current_value"],
            "previous_value": event["previous_value"],
            "delta": event["delta"],
            "delta_ratio": event["delta_ratio"],
            "cluster_name": event["cluster_name"],
            "target_url": primary_source["target_url"] if primary_source else None,
            "strategic_direction": None,
        },
    }


def build_growth_market_signal_tags(event: dict) -> list[str]:
    tags = [event["signal_type"], event["severity"]]
    if event.get("cluster_name"):
        tags.append(event["cluster_name"])
    for location in event.get("locations", [])[:2]:
        tags.append(location)
    return tags


def infer_growth_market_categories(event: dict) -> list[str]:
    haystack_parts = [event.get("title"), event.get("summary"), event.get("detail", {}).get("cluster_name"), event.get("cluster_name")]
    for evidence in event.get("evidence", []):
        haystack_parts.extend([evidence.get("label"), evidence.get("location")])
    haystack = " ".join(part for part in haystack_parts if isinstance(part, str)).lower()
    categories = []
    if any(keyword in haystack for keyword in ["finance", "financial", "fintech", "bank", "banking", "payment", "payments", "insurance", "revenue accounting"]):
        categories.append("finance")
    if any(keyword in haystack for keyword in ["technology", "technical", "ai", "engineer", "engineering", "research", "infrastructure", "platform", "compute", "model", "digital native", "codex"]):
        categories.append("tech")
    return categories


def infer_growth_market_category(event: dict) -> str | None:
    categories = infer_growth_market_categories(event)
    return categories[0] if categories else None


def build_wire_level(severity: str) -> str:
    if severity in {"critical", "high"}:
        return "high"
    if severity == "medium":
        return "elevated"
    return "watch"


def build_relative_time_label(timestamp: str) -> str:
    now = datetime.now(timezone.utc)
    delta_seconds = max(int((now - parse_iso_datetime(timestamp)).total_seconds()), 0)
    if delta_seconds < 60:
        return "just now"
    if delta_seconds < 3600:
        return f"{delta_seconds // 60}m ago"
    if delta_seconds < 86400:
        return f"{delta_seconds // 3600}h ago"
    return f"{delta_seconds // 86400}d ago"


def format_money(value, currency: str | None) -> str:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return "n/a"
    amount = f"{float(value):.2f}"
    if isinstance(currency, str) and currency.strip():
        return f"{currency.strip()} {amount}"
    return amount


def format_simple_label(value: str | None) -> str:
    if not isinstance(value, str) or not value.strip():
        return "tracked market"
    return value.strip()


def build_latest_snapshot_by_source(snapshots: list[dict]) -> dict[str, dict]:
    latest = {}
    for snapshot in sorted(snapshots, key=lambda item: parse_iso_datetime(item["captured_at"])):
        latest[snapshot["source_id"]] = snapshot
    return latest


def build_latest_snapshot_pairs_by_source(snapshots: list[dict]) -> dict[str, dict]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for snapshot in snapshots:
        grouped[snapshot["source_id"]].append(snapshot)
    pairs = {}
    for source_id, items in grouped.items():
        ordered = sorted(items, key=lambda item: parse_iso_datetime(item["captured_at"]))
        pairs[source_id] = {
            "current": ordered[-1] if ordered else None,
            "previous": ordered[-2] if len(ordered) >= 2 else None,
        }
    return pairs


def build_current_commerce_comparisons(comparisons: list[dict]) -> list[dict]:
    latest = {}
    for comparison in sorted(comparisons, key=lambda item: parse_iso_datetime(item["timestamp"])):
        latest[comparison["source_id"]] = comparison
    return sorted(latest.values(), key=lambda item: parse_iso_datetime(item["timestamp"]), reverse=True)


def merge_current_signal_ai_fields(signal: dict, insight_map: dict[str, dict]) -> dict:
    historical = insight_map.get(signal["id"])
    if not historical:
        return signal
    merged = dict(signal)
    if "insight" in historical:
        merged["insight"] = historical["insight"]
    if historical.get("confidence_score") is not None:
        merged["confidence_score"] = historical["confidence_score"]
    if historical.get("impact_score") is not None:
        merged["impact_score"] = historical["impact_score"]
    return merged


def extract_commerce_comparison_id(signal_id: str) -> str | None:
    if "::" not in signal_id:
        return None
    return signal_id.rsplit("::", 1)[0]


def build_commerce_signal_key(signal: dict) -> str:
    parts = [
        "commerce",
        signal.get("company_id") or "unknown",
        signal.get("competitor_id") or "unknown",
        signal.get("tracking_group_id") or signal.get("sku") or "unknown",
        signal.get("marketplace") or "global",
        signal["signal_type"],
    ]
    return "::".join(parts)


def build_growth_signal_key(event: dict) -> str:
    evidence_url = read_primary_evidence_url(event)
    if event["signal_type"] == "role_cluster_surge":
        return "::".join(["growth", event["company_id"], event["signal_type"], event.get("cluster_name") or "general"])
    if event["signal_type"] in {"market_entry", "geographic_expansion"}:
        location = event["locations"][0] if event["locations"] else "unknown"
        return "::".join(["growth", event["company_id"], event["signal_type"], location])
    if evidence_url:
        return "::".join(["growth", event["company_id"], event["signal_type"], evidence_url])
    return "::".join(["growth", event["company_id"], event["signal_type"]])


def build_commerce_signal_provenance(
    signal: dict,
    source: dict | None,
    comparison: dict | None,
    latest_snapshot: dict | None,
) -> dict:
    snapshot_ids = []
    extraction_timestamps = []
    target_urls = []
    file_paths = []
    run_ids = []
    evidence_urls = [item["url"] for item in build_commerce_market_signal_evidence(signal) if item.get("url")]
    if comparison:
        snapshot_ids.extend([comparison["current_snapshot_id"], comparison["previous_snapshot_id"]])
        extraction_timestamps.append(comparison["timestamp"])
    if latest_snapshot:
        file_path = latest_snapshot.get("file_path")
        if isinstance(file_path, str):
            file_paths.append(file_path)
        run_id = latest_snapshot.get("run", {}).get("run_id")
        if run_id:
            run_ids.append(run_id)
    if source and source.get("target_url"):
        target_urls.append(source["target_url"])
    return {
        "source_ids": [signal["source_id"]],
        "snapshot_ids": deduplicate_strings(snapshot_ids),
        "extraction_timestamps": deduplicate_strings(extraction_timestamps),
        "evidence_urls": deduplicate_strings(evidence_urls),
        "target_urls": deduplicate_strings(target_urls),
        "file_paths": deduplicate_strings(file_paths),
        "run_ids": deduplicate_strings(run_ids),
    }


def build_growth_signal_provenance(
    event: dict,
    source_map: dict[str, dict],
    latest_snapshot_pairs: dict[str, dict],
) -> dict:
    snapshot_ids = []
    extraction_timestamps = []
    evidence_urls = []
    target_urls = []
    file_paths = []
    run_ids = []
    for source_id in event["source_ids"]:
        source = source_map.get(source_id)
        pair = latest_snapshot_pairs.get(source_id, {})
        current_snapshot = pair.get("current")
        previous_snapshot = pair.get("previous")
        if source and source.get("target_url"):
            target_urls.append(source["target_url"])
        for snapshot in (current_snapshot, previous_snapshot):
            if not snapshot:
                continue
            snapshot_ids.append(snapshot["snapshot_id"])
            extraction_timestamps.append(snapshot["captured_at"])
            if isinstance(snapshot.get("file_path"), str):
                file_paths.append(snapshot["file_path"])
            run_id = snapshot.get("run", {}).get("run_id")
            if run_id:
                run_ids.append(run_id)
        for evidence in event.get("evidence", []):
            if evidence.get("url"):
                evidence_urls.append(evidence["url"])
    return {
        "source_ids": deduplicate_strings(event["source_ids"]),
        "snapshot_ids": deduplicate_strings(snapshot_ids),
        "extraction_timestamps": deduplicate_strings(extraction_timestamps),
        "evidence_urls": deduplicate_strings(evidence_urls),
        "target_urls": deduplicate_strings(target_urls),
        "file_paths": deduplicate_strings(file_paths),
        "run_ids": deduplicate_strings(run_ids),
    }


def build_history_growth_signal_provenance(
    event: dict,
    source_map: dict[str, dict],
    snapshot_map: dict[str, dict],
) -> dict:
    provenance = event.get("provenance") or {}
    target_urls = []
    file_paths = []
    run_ids = []
    for source_id in provenance.get("source_ids", []):
        source = source_map.get(source_id)
        if source and source.get("target_url"):
            target_urls.append(source["target_url"])
    for snapshot_id in provenance.get("snapshot_ids", []):
        snapshot = snapshot_map.get(snapshot_id)
        if not snapshot:
            continue
        if isinstance(snapshot.get("file_path"), str):
            file_paths.append(snapshot["file_path"])
        run_id = snapshot.get("run", {}).get("run_id")
        if run_id:
            run_ids.append(run_id)
    evidence_urls = [item["url"] for item in event.get("evidence", []) if item.get("url")]
    return {
        "source_ids": deduplicate_strings(provenance.get("source_ids", event.get("source_ids", []))),
        "snapshot_ids": deduplicate_strings(provenance.get("snapshot_ids", [])),
        "extraction_timestamps": deduplicate_strings(provenance.get("extraction_timestamps", [event["timestamp"]])),
        "evidence_urls": deduplicate_strings(evidence_urls),
        "target_urls": deduplicate_strings(target_urls),
        "file_paths": deduplicate_strings(file_paths),
        "run_ids": deduplicate_strings(run_ids),
    }


def build_growth_map_points(event: dict, latest_snapshot_pairs: dict[str, dict]) -> list[dict]:
    points = []
    evidence_labels = {item["label"] for item in event.get("evidence", []) if item.get("label")}
    evidence_timestamps = {item["timestamp"] for item in event.get("evidence", []) if item.get("timestamp")}
    locations = set(event.get("locations", []))
    for source_id in event["source_ids"]:
        pair = latest_snapshot_pairs.get(source_id, {})
        current_snapshot = pair.get("current")
        if not current_snapshot:
            continue
        for point in current_snapshot.get("map_points", []):
            if not is_valid_map_point(point):
                continue
            if not point_matches_event(point, event, evidence_labels, evidence_timestamps, locations):
                continue
            points.append(
                {
                    "signal_id": event["id"],
                    "latitude": point["latitude"],
                    "longitude": point["longitude"],
                    "entity_name": point["entity_name"],
                    "signal_type": point["signal_type"],
                    "severity": point["severity"],
                    "timestamp": point["timestamp"],
                    "explanation": point["explanation"],
                    "cluster_key": point.get("cluster_key"),
                    "source_id": source_id,
                }
            )
    return deduplicate_map_points(points)


def is_valid_map_point(point: dict) -> bool:
    required_fields = ["latitude", "longitude", "entity_name", "signal_type", "severity", "timestamp", "explanation"]
    if not isinstance(point, dict):
        return False
    if any(field not in point for field in required_fields):
        return False
    return isinstance(point["latitude"], (int, float)) and isinstance(point["longitude"], (int, float))


def point_matches_event(
    point: dict,
    event: dict,
    evidence_labels: set[str],
    evidence_timestamps: set[str],
    locations: set[str],
) -> bool:
    if point.get("signal_type") == event["signal_type"]:
        return True
    if point.get("timestamp") in evidence_timestamps or point.get("timestamp") == event["timestamp"]:
        return True
    if point.get("entity_name") in evidence_labels or point.get("entity_name") in locations:
        return True
    explanation = point.get("explanation", "")
    for location in locations:
        if isinstance(explanation, str) and location and location in explanation:
            return True
    return False


def deduplicate_map_points(points: list[dict]) -> list[dict]:
    unique = {}
    for point in points:
        key = (
            point["latitude"],
            point["longitude"],
            point["entity_name"],
            point["signal_type"],
            point["timestamp"],
        )
        unique[key] = point
    return sorted(unique.values(), key=lambda item: item["timestamp"], reverse=True)


def build_canonical_entities(sources: list[dict], history_occurrences: list[dict], snapshots: list[dict]) -> dict:
    companies = {}
    competitors = {}
    products = {}
    skus = {}
    markets = {}
    regions = {}
    for source in sources:
        company = companies.setdefault(
            source["company_id"],
            {
                "company_id": source["company_id"],
                "company_name": source["company_name"],
                "categories": set(),
                "source_ids": set(),
                "sector": source.get("sector"),
                "geography": source.get("geography"),
                "product_category": source.get("product_category"),
                "peer_cohort_key": build_peer_cohort_key(source.get("sector"), source.get("geography"), source.get("product_category")),
            },
        )
        company["categories"].add(source["category"])
        company["source_ids"].add(source["source_id"])
    for occurrence in history_occurrences:
        if occurrence.get("competitor_id"):
            competitors[occurrence["competitor_id"]] = {
                "competitor_id": occurrence["competitor_id"],
                "competitor_name": occurrence["competitor_name"],
                "company_id": occurrence["company_id"],
            }
        product_id = occurrence.get("product_id") or build_canonical_id("product", occurrence.get("product_name"))
        if product_id and occurrence.get("product_name"):
            products[product_id] = {
                "product_id": product_id,
                "product_name": occurrence["product_name"],
                "company_id": occurrence["company_id"],
                "product_category": occurrence.get("product_category"),
            }
        if occurrence.get("sku"):
            skus[occurrence["sku"]] = {
                "sku": occurrence["sku"],
                "company_id": occurrence["company_id"],
                "product_name": occurrence.get("product_name"),
            }
        if occurrence.get("marketplace"):
            markets[occurrence["marketplace"]] = {
                "market": occurrence["marketplace"],
                "category": "marketplace",
            }
        for location in occurrence.get("locations", []):
            region_id = build_canonical_id("region", location)
            regions[region_id] = {
                "region_id": region_id,
                "label": location,
                "region_size": classify_region_size(location, occurrence.get("locations"), occurrence.get("marketplace")),
            }
    for snapshot in snapshots:
        for market in snapshot.get("markets", []):
            markets[market] = {
                "market": market,
                "category": "growth_market",
            }
        for point in snapshot.get("map_points", []):
            if not is_valid_map_point(point):
                continue
            label = point.get("entity_name")
            if not isinstance(label, str) or not label.strip():
                continue
            region_id = build_canonical_id("region", label)
            regions[region_id] = {
                "region_id": region_id,
                "label": label.strip(),
                "region_size": "city",
            }
    return {
        "companies": [
            {
                **company,
                "categories": sorted(company["categories"]),
                "source_ids": sorted(company["source_ids"]),
            }
            for company in sorted(companies.values(), key=lambda item: item["company_name"])
        ],
        "competitors": [competitors[key] for key in sorted(competitors)],
        "products": [products[key] for key in sorted(products)],
        "skus": [skus[key] for key in sorted(skus)],
        "markets": [markets[key] for key in sorted(markets)],
        "regions": [regions[key] for key in sorted(regions)],
    }


def build_signal_registry(
    settings: Settings,
    current_occurrences: list[dict],
    history_occurrences: list[dict],
    entities: dict,
) -> dict:
    current_by_key: dict[str, list[dict]] = defaultdict(list)
    history_by_key: dict[str, list[dict]] = defaultdict(list)
    dedup_history = {}
    for occurrence in history_occurrences:
        if occurrence:
            dedup_history[(occurrence["signal_key"], occurrence.get("content_hash") or occurrence["occurrence_id"])] = occurrence
    for occurrence in current_occurrences:
        if occurrence:
            current_by_key[occurrence["signal_key"]].append(occurrence)
            dedup_history[(occurrence["signal_key"], occurrence.get("content_hash") or occurrence["occurrence_id"])] = occurrence
    for occurrence in dedup_history.values():
        history_by_key[occurrence["signal_key"]].append(occurrence)
    overrides = load_signal_lifecycle_overrides(settings)
    company_profile_map = {company["company_id"]: company for company in entities["companies"]}
    registry_items = []
    active_items = []
    resolved_items = []
    items_by_id = {}
    all_occurrences = list(dedup_history.values())
    for signal_key, occurrences in history_by_key.items():
        ordered = sort_market_signal_items(occurrences)
        current_occurrence = select_current_occurrence(current_by_key.get(signal_key, []))
        latest_occurrence = current_occurrence or ordered[0]
        previous_occurrence = find_previous_occurrence(ordered, latest_occurrence["occurrence_id"])
        manual_override = overrides.get(signal_key)
        lifecycle_state = build_signal_lifecycle_state(current_occurrence, previous_occurrence, manual_override)
        benchmark = build_signal_benchmark(latest_occurrence, ordered, all_occurrences, company_profile_map)
        impact_rubric = build_signal_impact_rubric(latest_occurrence, ordered, benchmark)
        registry_item = {
            **latest_occurrence,
            "lifecycle_state": lifecycle_state,
            "history_count": len(ordered),
            "first_seen_at": ordered[-1]["timestamp"] if ordered else latest_occurrence["timestamp"],
            "last_seen_at": ordered[0]["timestamp"] if ordered else latest_occurrence["timestamp"],
            "previous_seen_at": previous_occurrence["timestamp"] if previous_occurrence else None,
            "lifecycle": {
                "state": lifecycle_state,
                "manual_override": manual_override,
                "available_manual_states": sorted(SIGNAL_STATE_MANUAL_STATES),
            },
            "benchmark": benchmark,
            "impact_score": impact_rubric["score"],
            "impact_rubric": impact_rubric,
        }
        registry_items.append(registry_item)
        items_by_id[registry_item["id"]] = registry_item
        if current_occurrence and lifecycle_state != "suppressed":
            active_items.append(registry_item)
        if lifecycle_state == "resolved":
            resolved_items.append(registry_item)
    registry_items = sort_market_signal_items(registry_items)
    active_items = sort_market_signal_items(active_items)
    resolved_items = sort_market_signal_items(resolved_items)
    normalized_history_by_key = {
        signal_key: sort_market_signal_items(occurrences)
        for signal_key, occurrences in history_by_key.items()
    }
    return {
        "registry_items": registry_items,
        "active_items": active_items,
        "resolved_items": resolved_items,
        "items_by_id": items_by_id,
        "history_by_key": normalized_history_by_key,
    }


def build_filtered_market_signal_view(items: list[dict], filters: dict) -> list[dict]:
    filtered = items
    lifecycle_state = filters.get("lifecycle_state")
    if lifecycle_state:
        filtered = [item for item in filtered if item["lifecycle_state"] == lifecycle_state]
    else:
        filtered = [item for item in filtered if item["lifecycle_state"] not in {"resolved", "suppressed"}]
    if filters.get("severity"):
        filtered = [item for item in filtered if item["severity"] == filters["severity"]]
    if filters.get("wire_level"):
        filtered = [item for item in filtered if item["wire_level"] == filters["wire_level"]]
    if filters.get("company_id"):
        filtered = [item for item in filtered if item["company_id"] == filters["company_id"]]
    if filters.get("source_id"):
        filtered = [item for item in filtered if filters["source_id"] in item["source_ids"]]
    if filters.get("signal_type"):
        filtered = [item for item in filtered if item["signal_type"] == filters["signal_type"]]
    if filters.get("location"):
        filtered = [item for item in filtered if filters["location"] in item["locations"]]
    if filters.get("marketplace"):
        filtered = [item for item in filtered if item.get("marketplace") == filters["marketplace"]]
    if filters.get("market_category"):
        filtered = [
            item
            for item in filtered
            if filters["market_category"] == item.get("market_category")
            or filters["market_category"] in item.get("market_categories", [])
        ]
    return sort_market_signal_items(filtered)


def build_signal_lifecycle_state(
    current_occurrence: dict | None,
    previous_occurrence: dict | None,
    manual_override: dict | None,
) -> str:
    if manual_override and manual_override.get("state") in SIGNAL_STATE_MANUAL_STATES:
        return manual_override["state"]
    if current_occurrence is None:
        return "resolved"
    if previous_occurrence is None:
        return "new"
    if build_occurrence_fingerprint(current_occurrence) != build_occurrence_fingerprint(previous_occurrence):
        return "updated"
    return "confirmed"


def build_occurrence_fingerprint(occurrence: dict) -> str:
    payload = {
        "title": occurrence["title"],
        "summary": occurrence["summary"],
        "severity": occurrence["severity"],
        "locations": occurrence.get("locations", []),
        "detail": occurrence.get("detail"),
    }
    return json.dumps(payload, sort_keys=True, ensure_ascii=True)


def select_current_occurrence(occurrences: list[dict]) -> dict | None:
    if not occurrences:
        return None
    return sort_market_signal_items(occurrences)[0]


def find_previous_occurrence(occurrences: list[dict], current_occurrence_id: str) -> dict | None:
    for occurrence in occurrences:
        if occurrence["occurrence_id"] != current_occurrence_id:
            return occurrence
    return None


def build_signal_benchmark(
    signal: dict,
    signal_history: list[dict],
    all_occurrences: list[dict],
    company_profile_map: dict[str, dict],
) -> dict:
    self_candidates = [
        item
        for item in signal_history
        if item["occurrence_id"] != signal["occurrence_id"]
    ]
    peer_candidates = [
        item
        for item in all_occurrences
        if item["occurrence_id"] != signal["occurrence_id"]
        and item["company_id"] != signal["company_id"]
        and item["category"] == signal["category"]
        and item["signal_type"] == signal["signal_type"]
        and match_peer_cohort(signal, item, company_profile_map)
    ]
    global_candidates = [
        item
        for item in all_occurrences
        if item["occurrence_id"] != signal["occurrence_id"]
        and item["category"] == signal["category"]
        and item["signal_type"] == signal["signal_type"]
    ]
    return {
        "magnitude": round(build_signal_magnitude(signal), 4),
        "self": build_benchmark_summary(signal, self_candidates),
        "peer_cohort": build_benchmark_summary(signal, peer_candidates),
        "global": build_benchmark_summary(signal, global_candidates),
        "dimensions": {
            "sector": signal.get("sector"),
            "geography": signal.get("geography"),
            "role_cluster": signal.get("detail", {}).get("cluster_name"),
            "product_category": signal.get("product_category"),
        },
    }


def build_benchmark_summary(signal: dict, candidates: list[dict]) -> dict:
    magnitudes = [build_signal_magnitude(candidate) for candidate in candidates]
    magnitudes = [value for value in magnitudes if value is not None]
    current_magnitude = build_signal_magnitude(signal)
    baseline = round(median(magnitudes), 4) if magnitudes else None
    deviation_ratio = None
    if baseline not in {None, 0} and current_magnitude is not None:
        deviation_ratio = round((current_magnitude - baseline) / baseline, 4)
    elif baseline == 0 and current_magnitude is not None:
        deviation_ratio = 1.0 if current_magnitude > 0 else 0.0
    return {
        "count": len(magnitudes),
        "baseline_magnitude": baseline,
        "deviation_ratio": deviation_ratio,
    }


def build_signal_magnitude(signal: dict) -> float:
    detail = signal.get("detail") or {}
    delta = detail.get("delta")
    delta_ratio = detail.get("delta_ratio")
    if isinstance(delta_ratio, (int, float)) and not isinstance(delta_ratio, bool):
        return abs(float(delta_ratio))
    if isinstance(delta, dict):
        price_percent = delta.get("price_percent")
        if isinstance(price_percent, (int, float)) and not isinstance(price_percent, bool):
            return abs(float(price_percent)) / 100
        discount_percent = delta.get("discount_percent")
        if isinstance(discount_percent, (int, float)) and not isinstance(discount_percent, bool):
            return abs(float(discount_percent)) / 100
        if delta.get("stock_status_changed"):
            return 0.5
        if delta.get("seller_changed"):
            return 0.25
    if isinstance(delta, (int, float)) and not isinstance(delta, bool):
        return abs(float(delta))
    current_value = detail.get("current_value")
    previous_value = detail.get("previous_value")
    if isinstance(current_value, (int, float)) and isinstance(previous_value, (int, float)):
        return abs(float(current_value) - float(previous_value))
    return 0.0


def match_peer_cohort(signal: dict, candidate: dict, company_profile_map: dict[str, dict]) -> bool:
    signal_company = company_profile_map.get(signal["company_id"], {})
    candidate_company = company_profile_map.get(candidate["company_id"], {})
    dimensions = [
        ("sector", signal.get("sector") or signal_company.get("sector"), candidate.get("sector") or candidate_company.get("sector")),
        ("geography", signal.get("geography") or signal_company.get("geography"), candidate.get("geography") or candidate_company.get("geography")),
        ("role_cluster", signal.get("detail", {}).get("cluster_name"), candidate.get("detail", {}).get("cluster_name")),
        ("product_category", signal.get("product_category") or signal_company.get("product_category"), candidate.get("product_category") or candidate_company.get("product_category")),
    ]
    return any(left and right and left == right for _, left, right in dimensions)


def build_signal_impact_rubric(signal: dict, signal_history: list[dict], benchmark: dict) -> dict:
    components = {
        "revenue_exposure": build_revenue_exposure_score(signal),
        "region_size": build_region_size_score(signal),
        "role_seniority": build_role_seniority_score(signal),
        "complaint_volume": build_complaint_volume_score(signal),
        "novelty": build_novelty_score(signal_history),
        "benchmark_deviation": build_benchmark_deviation_score(benchmark),
    }
    weighted_score = 0.0
    for key, weight in IMPACT_COMPONENT_WEIGHTS.items():
        weighted_score += components[key]["score"] * weight
    return {
        "score": round(min(max(weighted_score, 0.0), 1.0), 4),
        "weights": IMPACT_COMPONENT_WEIGHTS,
        "components": components,
        "ai_reference_score": signal.get("ai_impact_score"),
    }


def build_revenue_exposure_score(signal: dict) -> dict:
    explicit_weight = signal.get("revenue_exposure_weight")
    if isinstance(explicit_weight, (int, float)) and not isinstance(explicit_weight, bool):
        score = min(max(float(explicit_weight), 0.0), 1.0)
        return {"score": round(score, 4), "reason": "Configured on source definition."}
    if signal["category"] == "commerce_intelligence":
        return {"score": 0.8, "reason": "Commerce price and inventory shifts are directly tied to sell-through."}
    if signal["signal_type"] in {"product_launch", "funding_mention"}:
        return {"score": 0.7, "reason": "Product and funding signals can materially affect company trajectory."}
    if signal["signal_type"] in {"market_entry", "geographic_expansion"}:
        return {"score": 0.6, "reason": "Geographic expansion affects reachable demand and distribution."}
    return {"score": 0.4, "reason": "Hiring and operating signals are indirect revenue indicators."}


def build_region_size_score(signal: dict) -> dict:
    locations = signal.get("locations", [])
    marketplace = signal.get("marketplace")
    region_size = classify_region_size(signal.get("location_label"), locations, marketplace)
    return {
        "score": REGION_SIZE_SCORES[region_size],
        "reason": f"Explicit location scope classified as {region_size}.",
    }


def classify_region_size(primary_location: str | None, locations: list[str], marketplace: str | None) -> str:
    if marketplace:
        return "marketplace"
    explicit_locations = [location for location in locations if isinstance(location, str) and location.strip()]
    if not explicit_locations and not primary_location:
        return "none"
    if len(explicit_locations) > 1:
        return "multi_region"
    location = explicit_locations[0] if explicit_locations else primary_location or ""
    normalized = location.strip().lower()
    if not normalized:
        return "none"
    if "multiple" in normalized or "locations" in normalized or "/" in normalized:
        return "multi_region"
    if "," in location:
        return "city"
    return "country"


def build_role_seniority_score(signal: dict) -> dict:
    title = signal["title"].lower()
    for level, keywords in ROLE_SENIORITY_KEYWORDS.items():
        if any(keyword in title for keyword in keywords):
            score_map = {
                "executive": 1.0,
                "director": 0.8,
                "principal": 0.65,
                "senior": 0.5,
            }
            return {"score": score_map[level], "reason": f"Detected {level} seniority in signal title."}
    return {"score": 0.2, "reason": "No senior leadership language was detected."}


def build_complaint_volume_score(signal: dict) -> dict:
    detail = signal.get("detail") or {}
    current_value = detail.get("current_value")
    complaint_count = None
    if isinstance(current_value, dict):
        raw_value = current_value.get("complaint_count")
        if isinstance(raw_value, (int, float)) and not isinstance(raw_value, bool):
            complaint_count = float(raw_value)
    if complaint_count is None:
        return {"score": 0.0, "reason": "No explicit complaint volume was present on this signal."}
    if complaint_count >= 100:
        return {"score": 1.0, "reason": "Complaint volume exceeded 100 mentions."}
    if complaint_count >= 25:
        return {"score": 0.6, "reason": "Complaint volume exceeded 25 mentions."}
    return {"score": 0.3, "reason": "Complaint volume was present but limited."}


def build_novelty_score(signal_history: list[dict]) -> dict:
    prior_occurrences = max(len(signal_history) - 1, 0)
    if prior_occurrences == 0:
        return {"score": 1.0, "reason": "This is the first observed occurrence for this signal key."}
    if prior_occurrences == 1:
        return {"score": 0.6, "reason": "Only one prior occurrence exists for this signal key."}
    if prior_occurrences <= 3:
        return {"score": 0.35, "reason": "The signal has occurred a few times before."}
    return {"score": 0.15, "reason": "The signal is historically familiar rather than novel."}


def build_benchmark_deviation_score(benchmark: dict) -> dict:
    deviations = [
        benchmark["self"].get("deviation_ratio"),
        benchmark["peer_cohort"].get("deviation_ratio"),
        benchmark["global"].get("deviation_ratio"),
    ]
    numeric = [abs(value) for value in deviations if isinstance(value, (int, float)) and not isinstance(value, bool)]
    if not numeric:
        return {"score": 0.0, "reason": "No benchmark baseline was available yet."}
    peak = max(numeric)
    return {
        "score": round(min(peak, 1.0), 4),
        "reason": "Score is driven by the largest available deviation across self, peer, and global baselines.",
    }


def build_peer_cohort_key(sector: str | None, geography: str | None, product_category: str | None) -> str:
    return "::".join(part or "unknown" for part in (sector, geography, product_category))


def build_watcher_qa_payload(
    settings: Settings,
    sources: list[dict],
    snapshots: list[dict],
    source_health: list[dict],
) -> dict:
    source_catalog = load_source_catalog(settings)
    source_map = {source["id"]: source for source in source_catalog}
    snapshots_by_source: dict[str, list[dict]] = defaultdict(list)
    for snapshot in snapshots:
        snapshots_by_source[snapshot["source_id"]].append(snapshot)
    issues = []
    source_summaries = []
    for record in sort_market_signal_source_health(source_health):
        source = source_map.get(record["source_id"])
        source_snapshots = sorted(
            snapshots_by_source.get(record["source_id"], []),
            key=lambda item: parse_iso_datetime(item["captured_at"]),
        )
        latest_snapshot = source_snapshots[-1] if source_snapshots else None
        drift = build_source_schema_drift(source, source_snapshots)
        if latest_snapshot and latest_snapshot.get("capture_status") == "VALIDATION_ERROR":
            issues.append(
                build_operational_issue(
                    source,
                    latest_snapshot,
                    "schema_validation_failed",
                    "high",
                    "Latest snapshot did not match the configured output schema.",
                )
            )
        if record["status"] == "failed" and latest_snapshot:
            issues.append(
                build_operational_issue(
                    source,
                    latest_snapshot,
                    "watcher_failed",
                    "high",
                    "Latest TinyFish run failed and requires review.",
                )
            )
        if drift["field_delta_count"] > 0 and latest_snapshot:
            issues.append(
                build_operational_issue(
                    source,
                    latest_snapshot,
                    "schema_drift",
                    "medium",
                    "Observed result fields changed between recent completed snapshots.",
                    details=drift,
                )
            )
        source_summaries.append(
            {
                "source_id": record["source_id"],
                "status": record["status"],
                "last_run_at": record["last_run_at"],
                "snapshots_total": record["snapshots_total"],
                "raw_payload_available": bool(latest_snapshot and latest_snapshot.get("file_path")),
                "schema_drift": drift,
                "last_error": record.get("last_error"),
            }
        )
    issues = sort_operational_issues(issues)
    return {
        "replay_supported": True,
        "issue_count": len(issues),
        "issues": issues,
        "sources": source_summaries,
    }


def build_source_schema_drift(source: dict | None, snapshots: list[dict]) -> dict:
    completed = [snapshot for snapshot in snapshots if snapshot.get("capture_status") == "COMPLETED" and isinstance(snapshot.get("result"), dict)]
    if len(completed) < 2 or not source:
        return {
            "field_delta_count": 0,
            "added_fields": [],
            "removed_fields": [],
            "latest_validation_errors": [],
        }
    previous = completed[-2]
    current = completed[-1]
    previous_fields = set(flatten_result_paths(previous["result"]))
    current_fields = set(flatten_result_paths(current["result"]))
    validation_errors = validate_result_against_schema(source["output_schema"], current["result"])
    return {
        "field_delta_count": len(previous_fields ^ current_fields),
        "added_fields": sorted(current_fields - previous_fields),
        "removed_fields": sorted(previous_fields - current_fields),
        "latest_validation_errors": validation_errors,
    }


def flatten_result_paths(value, prefix: str = "$") -> list[str]:
    if isinstance(value, dict):
        paths = []
        for key, child in value.items():
            paths.extend(flatten_result_paths(child, f"{prefix}.{key}"))
        return paths
    if isinstance(value, list):
        if not value:
            return [prefix]
        paths = []
        for index, child in enumerate(value):
            paths.extend(flatten_result_paths(child, f"{prefix}[{index}]"))
        return paths
    return [prefix]


def build_operational_issue(
    source: dict | None,
    snapshot: dict,
    issue_type: str,
    severity: str,
    message: str,
    details: dict | None = None,
) -> dict:
    source_id = snapshot["source_id"]
    issue_id = f"issue::{source_id}::{snapshot['snapshot_id']}::{issue_type}"
    return {
        "id": issue_id,
        "issue_type": issue_type,
        "severity": severity,
        "source_id": source_id,
        "source_name": source["name"] if source else snapshot.get("source_name"),
        "company_id": snapshot.get("company_id"),
        "company_name": snapshot.get("company_name"),
        "snapshot_id": snapshot["snapshot_id"],
        "opened_at": snapshot["captured_at"],
        "message": message,
        "file_path": snapshot.get("file_path"),
        "target_url": snapshot.get("target_url"),
        "details": details,
    }


def sort_operational_issues(issues: list[dict]) -> list[dict]:
    severity_rank = {"high": 3, "medium": 2, "low": 1}
    return sorted(issues, key=lambda item: (severity_rank.get(item["severity"], 0), item["opened_at"]), reverse=True)


def build_signal_map_payload(active_items: list[dict]) -> dict:
    points = []
    for item in active_items:
        for point in item.get("map_points", []):
            points.append(
                {
                    "signal_id": item["id"],
                    "company_id": item["company_id"],
                    "company_name": item["company_name"],
                    "latitude": point["latitude"],
                    "longitude": point["longitude"],
                    "entity_name": point["entity_name"],
                    "signal_type": point["signal_type"],
                    "severity": point["severity"],
                    "timestamp": point["timestamp"],
                    "explanation": point["explanation"],
                    "cluster_key": point.get("cluster_key"),
                }
            )
    return {
        "clusters_enabled": True,
        "cluster_radius_km": 25,
        "points": deduplicate_map_points(points),
        "clusters": build_map_clusters(points),
    }


def build_map_clusters(points: list[dict]) -> list[dict]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for point in points:
        cluster_key = point.get("cluster_key") or f"{round(point['latitude'], 2)}::{round(point['longitude'], 2)}"
        grouped[cluster_key].append(point)
    clusters = []
    for cluster_key, items in grouped.items():
        if len(items) < 2:
            continue
        clusters.append(
            {
                "cluster_key": cluster_key,
                "count": len(items),
                "latitude": round(sum(item["latitude"] for item in items) / len(items), 6),
                "longitude": round(sum(item["longitude"] for item in items) / len(items), 6),
                "signal_ids": sorted({item["signal_id"] for item in items}),
                "severities": sorted({item["severity"] for item in items}, key=lambda value: SEVERITY_RANKS.get(value, 0), reverse=True),
            }
        )
    return sorted(clusters, key=lambda item: item["count"], reverse=True)


def build_snapshot_replay_result(source: dict, snapshot: dict) -> dict:
    result = snapshot.get("result")
    validation_errors = []
    if isinstance(result, dict):
        validation_errors = validate_result_against_schema(source["output_schema"], result)
    else:
        validation_errors = [{"path": "$", "message": "Snapshot result is missing or not an object."}]
    normalized = None
    if source["category"] == "commerce_intelligence":
        normalized = normalize_commerce_snapshot(snapshot, source)
    if source["category"] == "growth_intelligence":
        normalized = normalize_growth_snapshot(snapshot, source)
    if source["category"] == "reputation_intelligence":
        normalized = normalize_reputation_market_snapshot(snapshot, source)
    replay_status = "normalized" if normalized and not validation_errors else "rejected"
    return {
        "snapshot_id": snapshot["snapshot_id"],
        "source_id": source["id"],
        "source_name": source["name"],
        "category": source["category"],
        "captured_at": snapshot["captured_at"],
        "capture_status": snapshot.get("capture_status"),
        "replay_status": replay_status,
        "file_path": snapshot.get("file_path"),
        "validation_errors": validation_errors,
        "normalized_summary": build_normalized_summary(normalized),
    }


def build_normalized_summary(normalized: dict | None) -> dict | None:
    if not normalized:
        return None
    if "articles" in normalized:
        return {
            "articles": len(normalized.get("articles", [])),
            "story_count": normalized.get("metrics", {}).get("story_count"),
        }
    if "jobs" in normalized:
        return {
            "jobs": len(normalized.get("jobs", [])),
            "product_announcements": len(normalized.get("product_announcements", [])),
            "funding_mentions": len(normalized.get("funding_mentions", [])),
            "expansion_indicators": len(normalized.get("expansion_indicators", [])),
            "markets": len(normalized.get("markets", [])),
        }
    return {
        "product_name": normalized.get("product_name"),
        "price": normalized.get("price"),
        "discount_percent": normalized.get("discount_percent"),
        "stock_status": normalized.get("stock_status"),
    }


def load_signal_lifecycle_overrides(settings: Settings) -> dict:
    path = build_signal_lifecycle_store_path(settings)
    if not path.exists():
        return {}
    payload = json.loads(path.read_text())
    if not isinstance(payload, dict):
        return {}
    overrides = payload.get("overrides", payload)
    if not isinstance(overrides, dict):
        return {}
    return {
        key: value
        for key, value in overrides.items()
        if isinstance(key, str) and isinstance(value, dict)
    }


def persist_signal_lifecycle_overrides(settings: Settings, overrides: dict) -> None:
    path = build_signal_lifecycle_store_path(settings)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"overrides": overrides}, indent=2, ensure_ascii=True))


def build_signal_lifecycle_store_path(settings: Settings) -> Path:
    return settings.resolve_path("backend/data/market_signals/lifecycle_overrides.json")


def build_related_market_signals(signal: dict, registry_items: list[dict]) -> list[dict]:
    related = [
        item
        for item in registry_items
        if item["id"] != signal["id"]
        and item["company_id"] == signal["company_id"]
        and (
            item["category"] == signal["category"]
            or item["signal_type"] == signal["signal_type"]
        )
    ]
    return sort_market_signal_items(related)[:10]


def build_signal_trend_history(signal_history: list[dict]) -> dict:
    ordered = sorted(signal_history, key=lambda item: parse_iso_datetime(item["timestamp"]))
    return {
        "occurrence_count": len(ordered),
        "timeline": [
            {
                "id": item["id"],
                "timestamp": item["timestamp"],
                "severity": item["severity"],
                "magnitude": round(build_signal_magnitude(item), 4),
            }
            for item in ordered
        ],
    }


def build_source_run_history(settings: Settings, source_ids: list[str], snapshots: list[dict]) -> list[dict]:
    source_id_set = set(source_ids)
    run_records = [
        record
        for record in load_source_runs(settings)
        if record["source_id"] in source_id_set
    ]
    if run_records:
        relevant_runs = sorted(run_records, key=lambda item: parse_iso_datetime(item["captured_at"]), reverse=True)
        return [
            {
                "run_record_id": record["run_record_id"],
                "source_id": record["source_id"],
                "captured_at": record["captured_at"],
                "capture_status": record["capture_status"],
                "change_state": record.get("change_state"),
                "snapshot_persisted": record.get("snapshot_persisted"),
                "observed_snapshot_id": record.get("observed_snapshot_id"),
                "canonical_snapshot_id": record.get("canonical_snapshot_id"),
                "duplicate_of_snapshot_id": record.get("duplicate_of_snapshot_id"),
                "file_path": record.get("file_path"),
                "target_url": record.get("target_url"),
                "run": record.get("run"),
                "validation_errors": record.get("validation_errors"),
            }
            for record in relevant_runs[:25]
        ]
    relevant_snapshots = [
        snapshot
        for snapshot in snapshots
        if snapshot["source_id"] in source_id_set
    ]
    relevant_snapshots = sorted(relevant_snapshots, key=lambda item: parse_iso_datetime(item["captured_at"]), reverse=True)
    return [
        {
            "snapshot_id": snapshot["snapshot_id"],
            "source_id": snapshot["source_id"],
            "captured_at": snapshot["captured_at"],
            "capture_status": snapshot["capture_status"],
            "file_path": snapshot.get("file_path"),
            "target_url": snapshot.get("target_url"),
            "run": snapshot.get("run"),
            "validation_errors": snapshot.get("validation_errors"),
        }
        for snapshot in relevant_snapshots[:25]
    ]


def build_raw_evidence_payload(signal_history: list[dict], snapshots_by_id: dict[str, dict]) -> list[dict]:
    evidence = []
    seen = set()
    for occurrence in signal_history:
        for item in occurrence.get("evidence", []):
            key = item.get("url") or item.get("label")
            if key and key not in seen:
                seen.add(key)
                evidence.append(
                    {
                        "type": "evidence_url",
                        "label": item.get("label"),
                        "url": item.get("url"),
                        "timestamp": item.get("timestamp"),
                        "location": item.get("location"),
                    }
                )
        provenance = occurrence.get("provenance", {})
        for snapshot_id in provenance.get("snapshot_ids", []):
            snapshot = snapshots_by_id.get(snapshot_id)
            if not snapshot:
                continue
            key = f"snapshot::{snapshot_id}"
            if key in seen:
                continue
            seen.add(key)
            evidence.append(
                {
                    "type": "snapshot_file",
                    "snapshot_id": snapshot_id,
                    "file_path": snapshot.get("file_path"),
                    "captured_at": snapshot.get("captured_at"),
                    "target_url": snapshot.get("target_url"),
                }
            )
    return evidence


def sort_market_signal_items(items: list[dict]) -> list[dict]:
    return sorted(
        items,
        key=lambda item: (SEVERITY_RANKS.get(item["severity"], 0), item["timestamp"], item["id"]),
        reverse=True,
    )


def sort_market_signal_source_health(records: list[dict]) -> list[dict]:
    return sorted(
        records,
        key=lambda item: (build_source_health_rank(item["status"]), item["last_run_at"] or ""),
        reverse=True,
    )


def build_source_health_rank(status: str) -> int:
    ranks = {
        "failed": 4,
        "validation_error": 3,
        "healthy": 2,
        "not_run": 1,
    }
    return ranks.get(status, 0)


def deduplicate_market_signal_sources(sources: list[dict]) -> list[dict]:
    unique = {}
    for source in sources:
        unique[source["source_id"]] = source
    return [unique[source_id] for source_id in sorted(unique)]


def deduplicate_market_signal_companies(companies: list[dict]) -> list[dict]:
    merged = {}
    for company in companies:
        record = merged.setdefault(
            company["company_id"],
            {
                "company_id": company["company_id"],
                "company_name": company["company_name"],
                "categories": set(),
                "sector": company.get("sector"),
                "geography": company.get("geography"),
                "product_category": company.get("product_category"),
            },
        )
        for category in company["categories"]:
            record["categories"].add(category)
        if company.get("sector"):
            record["sector"] = company["sector"]
        if company.get("geography"):
            record["geography"] = company["geography"]
        if company.get("product_category"):
            record["product_category"] = company["product_category"]
    return [
        {
            "company_id": record["company_id"],
            "company_name": record["company_name"],
            "categories": sorted(record["categories"]),
            "sector": record.get("sector"),
            "geography": record.get("geography"),
            "product_category": record.get("product_category"),
        }
        for record in sorted(merged.values(), key=lambda item: item["company_name"])
    ]


def deduplicate_snapshots(snapshots: list[dict]) -> list[dict]:
    unique = {}
    for snapshot in snapshots:
        unique[snapshot["snapshot_id"]] = snapshot
    return sorted(unique.values(), key=lambda item: parse_iso_datetime(item["captured_at"]))


def build_market_signals_meta(settings: Settings, dataset: dict, refresh: bool) -> dict:
    schedules = sorted(
        {
            source.get("schedule", {}).get("interval_minutes")
            for source in dataset["sources"]
            if isinstance(source.get("schedule"), dict) and isinstance(source["schedule"].get("interval_minutes"), int)
        }
    )
    return {
        "api_version": "v1",
        "module": "market_signals",
        "view": "wire",
        "contract_version": build_contract_payload("market_signals", view="wire")["contract_version"],
        "generated_at": to_iso_timestamp(datetime.now(timezone.utc)),
        "refresh_requested": refresh,
        "filters": dataset["filters"],
        "source_count": len(dataset["sources"]),
        "company_count": len(dataset["companies"]),
        "active_count": len(dataset["view_items"]),
        "latest_snapshot_at": dataset["latest_snapshot_at"],
        "latest_signal_at": dataset["view_items"][0]["timestamp"] if dataset["view_items"] else None,
        "schedule_interval_minutes": schedules[0] if len(schedules) == 1 else None,
        "schedules": schedules,
        "memory": {
            "mode": "persistent",
            "snapshot_strategy": "change_only",
            "source_run_strategy": "append_all_runs",
            "signal_deduplication": "signal_key + content_hash",
            "run_count": sum(item.get("runs_total") or 0 for item in dataset["source_health"]),
            "recent_run_count": len(dataset["recent_runs"]),
            "snapshot_count": len(dataset["snapshots"]),
        },
        "integrations": {
            "tinyfish": {
                "provider": "TinyFish",
                "configured": settings.tinyfish_configured,
                "base_url": settings.tinyfish_base_url,
            },
            "openai": {
                "provider": "OpenAI",
                "configured": settings.openai_configured,
                "base_url": settings.openai_base_url,
                "model": settings.openai_model or None,
            },
        },
    }


def build_market_signals_summary(items: list[dict]) -> dict:
    by_category: dict[str, int] = defaultdict(int)
    by_signal_type: dict[str, int] = defaultdict(int)
    by_lifecycle_state: dict[str, int] = defaultdict(int)
    by_market_category: dict[str, int] = defaultdict(int)
    for item in items:
        by_category[item["category"]] += 1
        by_signal_type[item["signal_type"]] += 1
        by_lifecycle_state[item["lifecycle_state"]] += 1
        for market_category in item.get("market_categories", []):
            by_market_category[market_category] += 1
    return {
        "high_priority_count": len([item for item in items if item["wire_level"] == "high"]),
        "elevated_count": len([item for item in items if item["wire_level"] == "elevated"]),
        "watch_count": len([item for item in items if item["wire_level"] == "watch"]),
        "active_count": len(items),
        "by_category": [{"category": category, "count": count} for category, count in sorted(by_category.items())],
        "by_signal_type": [{"signal_type": signal_type, "count": count} for signal_type, count in sorted(by_signal_type.items())],
        "by_lifecycle_state": [{"lifecycle_state": state, "count": count} for state, count in sorted(by_lifecycle_state.items())],
        "by_market_category": [{"market_category": category, "count": count} for category, count in sorted(by_market_category.items())],
    }


def build_wire_stats(items: list[dict]) -> list[dict]:
    return [
        {"id": "high_priority", "label": "High+", "value": len([item for item in items if item["wire_level"] == "high"])},
        {"id": "elevated", "label": "Elevated", "value": len([item for item in items if item["wire_level"] == "elevated"])},
        {"id": "watch", "label": "Watch", "value": len([item for item in items if item["wire_level"] == "watch"])},
        {"id": "active", "label": "Active", "value": len(items)},
    ]


def build_market_signal_facets(items: list[dict]) -> dict:
    return {
        "categories": sorted({item["category"] for item in items}),
        "severities": sorted({item["severity"] for item in items}, key=lambda item: SEVERITY_RANKS[item], reverse=True),
        "wire_levels": ["high", "elevated", "watch"],
        "signal_types": sorted({item["signal_type"] for item in items}),
        "market_categories": sorted({category for item in items for category in item.get("market_categories", []) if category}),
        "locations": sorted({location for item in items for location in item["locations"]}),
        "marketplaces": sorted({item["marketplace"] for item in items if item.get("marketplace")}),
        "lifecycle_states": sorted({item["lifecycle_state"] for item in items}),
    }


def build_growth_signal_insight_map(strategic_insights: list[dict]) -> dict[str, dict]:
    insight_map = {}
    for insight in strategic_insights:
        for signal_id in insight["signal_ids"]:
            current = insight_map.get(signal_id)
            if not current or insight["impact_score"] > current["impact_score"]:
                insight_map[signal_id] = insight
    return insight_map


def read_primary_evidence_url(event: dict) -> str | None:
    for evidence in event.get("evidence", []):
        url = evidence.get("url")
        if isinstance(url, str) and url.strip():
            return url.strip()
    return None


def deduplicate_strings(values: list[str]) -> list[str]:
    return sorted({value for value in values if isinstance(value, str) and value.strip()})


def build_market_signal_content_hash(payload: dict) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def build_canonical_id(prefix: str, value: str | None) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    normalized = "".join(character.lower() if character.isalnum() else "-" for character in value.strip())
    normalized = "-".join(part for part in normalized.split("-") if part)
    if not normalized:
        return None
    return f"{prefix}-{normalized}"
