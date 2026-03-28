from collections import defaultdict
from datetime import datetime, timedelta, timezone

from market_monitor_api.config import Settings
from market_monitor_api.services.api_contract import build_contract_payload, decode_cursor, paginate_records
from market_monitor_api.services.openai_service import build_growth_insights
from market_monitor_api.services.tinyfish import (
    build_company_catalog,
    build_source_health,
    load_snapshots,
    load_source_catalog,
    parse_iso_datetime,
    run_source_refreshes,
    to_iso_timestamp,
)


GROWTH_REQUIRED_SOURCE_FIELDS = [
    "source_type",
    "schedule",
]

GROWTH_VALID_SOURCE_TYPES = {
    "career_page",
    "job_board",
    "press_release",
    "product_page",
    "directory",
}

ROLE_CLUSTER_KEYWORDS = {
    "fintech": ["fintech", "financial services", "finance", "banking", "payments", "insurance"],
    "government": ["government", "federal", "public sector", "gov"],
    "healthcare": ["healthcare", "life sciences", "medical", "clinical"],
    "security": ["security", "abuse", "fraud", "safety", "trust", "privacy"],
    "infrastructure": ["platform", "infrastructure", "reliability", "distributed", "systems", "compute"],
    "ai_research": ["research", "scientist", "applied", "reasoning", "alignment", "model"],
    "sales_expansion": ["sales", "partnership", "account", "enterprise", "go to market", "business"],
}

SOURCE_FILTER_KEYS = ("source_id", "company_id", "source_type")


class GrowthConfigError(Exception):
    def __init__(self, code: str, message: str, status_code: int = 503):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


def build_growth_response(settings: Settings, refresh: bool = False, filters: dict | None = None) -> dict:
    dataset = collect_growth_dataset(settings, refresh=refresh, filters=filters or {})
    return {
        "contract": build_contract_payload("growth_intelligence", view="overview"),
        "meta": build_growth_meta(settings, dataset, refresh),
        "kpis": build_growth_kpis(dataset),
        "events": dataset["events"],
        "strategic_insights": dataset["strategic_insights"],
        "signal_clusters": dataset["signal_clusters"],
        "current_activity": dataset["current_activity"],
        "company_rollups": dataset["company_rollups"],
        "trend_series": dataset["trend_series"],
        "source_health": dataset["source_health"],
        "sources": dataset["sources"],
        "companies": dataset["companies"],
    }


def build_growth_events_response(
    settings: Settings,
    refresh: bool = False,
    filters: dict | None = None,
) -> dict:
    dataset = collect_growth_dataset(settings, refresh=refresh, filters=filters or {})
    cursor = decode_cursor((filters or {}).get("cursor"))
    limit = (filters or {}).get("limit", 50)
    page_events, pagination = paginate_records(dataset["events"], cursor, limit)
    return {
        "contract": build_contract_payload("growth_intelligence", view="events"),
        "meta": build_growth_meta(settings, dataset, refresh),
        "events": page_events,
        "pagination": pagination,
        "strategic_insights": dataset["strategic_insights"],
        "signal_clusters": dataset["signal_clusters"],
    }


def build_growth_history_response(settings: Settings, filters: dict | None = None) -> dict:
    dataset = collect_growth_dataset(settings, refresh=False, filters=filters or {})
    return {
        "contract": build_contract_payload("growth_intelligence", view="history"),
        "meta": build_growth_meta(settings, dataset, False),
        "snapshots": dataset["snapshots"],
        "comparisons": dataset["comparisons"],
        "trend_series": dataset["trend_series"],
        "company_rollups": dataset["company_rollups"],
    }


def build_growth_trends_response(settings: Settings, filters: dict | None = None) -> dict:
    dataset = collect_growth_dataset(settings, refresh=False, filters=filters or {})
    return {
        "contract": build_contract_payload("growth_intelligence", view="trends"),
        "meta": build_growth_meta(settings, dataset, False),
        "kpis": build_growth_kpis(dataset),
        "trend_series": dataset["trend_series"],
        "company_rollups": dataset["company_rollups"],
        "current_activity": dataset["current_activity"],
    }


def collect_growth_dataset(settings: Settings, refresh: bool, filters: dict) -> dict:
    try:
        all_sources = load_source_catalog(settings)
    except FileNotFoundError as exc:
        raise GrowthConfigError("source_config_missing", str(exc))
    except ValueError as exc:
        raise GrowthConfigError("source_config_invalid", str(exc), status_code=500)
    sources = build_growth_sources(all_sources)
    validate_growth_sources(sources)
    if refresh and not settings.tinyfish_configured:
        raise GrowthConfigError(
            "tinyfish_not_configured",
            "TinyFish is required for refresh=true. Set TINYFISH_API_KEY in the .env file.",
        )
    refresh_sources = select_growth_refresh_sources(sources, filters)
    if refresh and refresh_sources:
        run_source_refreshes(settings, refresh_sources)
    all_snapshots = load_snapshots(settings)
    snapshots = build_growth_snapshots(sources, all_snapshots)
    filtered_sources, filtered_snapshots = apply_growth_source_filters(sources, snapshots, filters)
    comparisons = build_growth_comparisons(filtered_sources, filtered_snapshots)
    all_events = build_growth_events(filtered_sources, filtered_snapshots, comparisons)
    filtered_events = apply_growth_event_filters(all_events, filters)
    signal_clusters = build_growth_signal_clusters(filtered_events)
    strategic_insights = build_growth_insights(signal_clusters, filtered_events, filtered_snapshots, settings)
    company_rollups = build_growth_company_rollups(filtered_sources, filtered_snapshots, filtered_events, strategic_insights)
    source_health = build_source_health(settings, filtered_sources, all_snapshots)
    return {
        "sources": filtered_sources,
        "companies": build_company_catalog(filtered_sources, filtered_snapshots),
        "snapshots": filtered_snapshots,
        "comparisons": comparisons,
        "events": filtered_events,
        "signal_clusters": signal_clusters,
        "strategic_insights": strategic_insights,
        "current_activity": build_current_growth_activity(filtered_snapshots),
        "company_rollups": company_rollups,
        "trend_series": build_growth_time_series(filtered_snapshots),
        "source_health": source_health,
        "filters": filters,
    }


def build_growth_sources(all_sources: list[dict]) -> list[dict]:
    return [source for source in all_sources if source["category"] == "growth_intelligence"]


def validate_growth_sources(sources: list[dict]) -> None:
    for source in sources:
        missing_fields = [field for field in GROWTH_REQUIRED_SOURCE_FIELDS if field not in source]
        if missing_fields:
            raise GrowthConfigError(
                "growth_source_invalid",
                f"Growth source {source['id']} is missing fields: {', '.join(missing_fields)}",
                status_code=500,
            )
        if source["source_type"] not in GROWTH_VALID_SOURCE_TYPES:
            raise GrowthConfigError(
                "growth_source_invalid",
                f"Growth source {source['id']} has invalid source_type: {source['source_type']}",
                status_code=500,
            )
        schedule = source["schedule"]
        if not isinstance(schedule, dict) or not isinstance(schedule.get("interval_minutes"), int):
            raise GrowthConfigError(
                "growth_source_invalid",
                f"Growth source {source['id']} must define schedule.interval_minutes as an integer.",
                status_code=500,
            )


def build_growth_snapshots(sources: list[dict], all_snapshots: list[dict]) -> list[dict]:
    source_map = {source["id"]: source for source in sources}
    snapshots = []
    for snapshot in all_snapshots:
        source = source_map.get(snapshot["source_id"])
        if not source:
            continue
        normalized = normalize_growth_snapshot(snapshot, source)
        if normalized:
            snapshots.append(normalized)
    return sorted(snapshots, key=lambda item: parse_iso_datetime(item["captured_at"]))


def normalize_growth_snapshot(snapshot: dict, source: dict) -> dict | None:
    if snapshot["capture_status"] != "COMPLETED":
        return None
    result = snapshot.get("result")
    if not isinstance(result, dict):
        return None
    captured_at = read_required_string(result, "captured_at")
    if not captured_at:
        return None
    if has_valid_growth_result_structure(result):
        return build_normalized_growth_snapshot(snapshot, source, result, captured_at)
    if has_valid_legacy_growth_result_structure(result):
        return build_normalized_legacy_growth_snapshot(snapshot, source, result, captured_at)
    return None


def build_normalized_growth_snapshot(snapshot: dict, source: dict, result: dict, captured_at: str) -> dict | None:
    jobs = collect_job_entries(result, source)
    product_announcements = collect_product_announcements(result)
    funding_mentions = collect_funding_mentions(result)
    expansion_indicators = collect_expansion_indicators(result)
    markets = build_market_list(jobs, expansion_indicators)
    role_cluster_counts = build_role_cluster_counts(jobs)
    metrics = build_growth_metrics(result, jobs, product_announcements, funding_mentions, expansion_indicators, markets)
    if metrics is None:
        return None
    return {
        "snapshot_id": snapshot["snapshot_id"],
        "captured_at": captured_at,
        "source_id": source["id"],
        "source_name": source["name"],
        "source_type": source["source_type"],
        "company_id": source["company_id"],
        "company_name": source["company_name"],
        "target_url": snapshot["target_url"],
        "schedule": source["schedule"],
        "jobs": jobs,
        "product_announcements": product_announcements,
        "funding_mentions": funding_mentions,
        "expansion_indicators": expansion_indicators,
        "markets": markets,
        "role_cluster_counts": role_cluster_counts,
        "metrics": metrics,
        "map_points": result.get("map_points") if isinstance(result.get("map_points"), list) else [],
        "schema_version": "v2",
        "raw_result": result,
    }


def build_normalized_legacy_growth_snapshot(snapshot: dict, source: dict, result: dict, captured_at: str) -> dict | None:
    jobs = collect_legacy_job_entries(result, source, captured_at)
    product_announcements = []
    funding_mentions = []
    expansion_indicators = []
    markets = build_market_list(jobs, expansion_indicators)
    role_cluster_counts = build_role_cluster_counts(jobs)
    metrics = build_growth_metrics(result, jobs, product_announcements, funding_mentions, expansion_indicators, markets)
    if metrics is None:
        return None
    return {
        "snapshot_id": snapshot["snapshot_id"],
        "captured_at": captured_at,
        "source_id": source["id"],
        "source_name": source["name"],
        "source_type": source["source_type"],
        "company_id": source["company_id"],
        "company_name": source["company_name"],
        "target_url": snapshot["target_url"],
        "schedule": source["schedule"],
        "jobs": jobs,
        "product_announcements": product_announcements,
        "funding_mentions": funding_mentions,
        "expansion_indicators": expansion_indicators,
        "markets": markets,
        "role_cluster_counts": role_cluster_counts,
        "metrics": metrics,
        "map_points": result.get("map_points") if isinstance(result.get("map_points"), list) else [],
        "schema_version": "legacy_v1",
        "raw_result": result,
    }


def has_valid_growth_result_structure(result: dict) -> bool:
    required_array_fields = [
        "jobs",
        "product_announcements",
        "funding_mentions",
        "expansion_indicators",
        "map_points",
    ]
    if any(not isinstance(result.get(field_name), list) for field_name in required_array_fields):
        return False
    return isinstance(result.get("metrics"), dict)


def has_valid_legacy_growth_result_structure(result: dict) -> bool:
    return isinstance(result.get("signals"), list) and isinstance(result.get("metrics"), dict)


def collect_job_entries(result: dict, source: dict) -> list[dict]:
    raw_jobs = result.get("jobs")
    if not isinstance(raw_jobs, list):
        return []
    jobs = []
    for item in raw_jobs:
        normalized = normalize_job_entry(item, source)
        if normalized:
            jobs.append(normalized)
    return jobs


def collect_legacy_job_entries(result: dict, source: dict, captured_at: str) -> list[dict]:
    raw_items = result.get("signals")
    if not isinstance(raw_items, list):
        return []
    jobs = []
    for item in raw_items:
        normalized = normalize_legacy_job_entry(item, source, captured_at)
        if normalized:
            jobs.append(normalized)
    return jobs


def normalize_job_entry(item: dict, source: dict) -> dict | None:
    if not isinstance(item, dict):
        return None
    role = read_required_string(item, "title")
    team = read_required_string(item, "team")
    location = read_required_string(item, "location")
    timestamp = read_required_string(item, "timestamp")
    evidence_url = read_required_string(item, "evidence_url")
    if not role or not team or not location or not timestamp or not evidence_url:
        return None
    role_clusters = assign_role_clusters(role, team)
    return {
        "id": build_entity_id("job", evidence_url),
        "role": role,
        "team": team,
        "location": location,
        "timestamp": timestamp,
        "evidence_url": evidence_url,
        "role_clusters": role_clusters,
        "source_type": source["source_type"],
    }


def normalize_legacy_job_entry(item: dict, source: dict, captured_at: str) -> dict | None:
    if not isinstance(item, dict):
        return None
    signal_type = read_required_string(item, "signal_type")
    if signal_type not in {"hiring_opening", "job_posting", "role_opening"}:
        return None
    role = read_required_string(item, "title")
    team = read_required_string(item, "team")
    location = read_required_string(item, "location")
    evidence_url = read_required_string(item, "evidence_url")
    timestamp = read_optional_string(item, "timestamp") or captured_at
    if not role or not team or not location or not evidence_url:
        return None
    role_clusters = assign_role_clusters(role, team)
    return {
        "id": build_entity_id("job", evidence_url),
        "role": role,
        "team": team,
        "location": location,
        "timestamp": timestamp,
        "evidence_url": evidence_url,
        "role_clusters": role_clusters,
        "source_type": source["source_type"],
    }


def collect_product_announcements(result: dict) -> list[dict]:
    raw_items = result.get("product_announcements")
    if not isinstance(raw_items, list):
        return []
    announcements = []
    for item in raw_items:
        normalized = normalize_product_announcement(item)
        if normalized:
            announcements.append(normalized)
    return announcements


def normalize_product_announcement(item: dict) -> dict | None:
    if not isinstance(item, dict):
        return None
    title = read_required_string(item, "title")
    published_at = read_required_string(item, "published_at")
    evidence_url = read_required_string(item, "evidence_url")
    if not title or not published_at or not evidence_url:
        return None
    return {
        "id": build_entity_id("product", evidence_url),
        "signal_type": read_optional_string(item, "signal_type"),
        "title": title,
        "product_name": read_optional_string(item, "product_name"),
        "summary": read_optional_string(item, "summary"),
        "published_at": published_at,
        "evidence_url": evidence_url,
    }


def collect_funding_mentions(result: dict) -> list[dict]:
    raw_items = result.get("funding_mentions")
    if not isinstance(raw_items, list):
        return []
    mentions = []
    for item in raw_items:
        normalized = normalize_funding_mention(item)
        if normalized:
            mentions.append(normalized)
    return mentions


def normalize_funding_mention(item: dict) -> dict | None:
    if not isinstance(item, dict):
        return None
    title = read_required_string(item, "title")
    published_at = read_required_string(item, "published_at")
    evidence_url = read_required_string(item, "evidence_url")
    if not title or not published_at or not evidence_url:
        return None
    return {
        "id": build_entity_id("funding", evidence_url),
        "signal_type": read_optional_string(item, "signal_type"),
        "title": title,
        "amount": item.get("amount"),
        "currency": item.get("currency"),
        "summary": read_optional_string(item, "summary"),
        "published_at": published_at,
        "evidence_url": evidence_url,
    }


def collect_expansion_indicators(result: dict) -> list[dict]:
    raw_items = result.get("expansion_indicators")
    if not isinstance(raw_items, list):
        return []
    indicators = []
    for item in raw_items:
        normalized = normalize_expansion_indicator(item)
        if normalized:
            indicators.append(normalized)
    return indicators


def normalize_expansion_indicator(item: dict) -> dict | None:
    if not isinstance(item, dict):
        return None
    title = read_required_string(item, "title")
    location = read_required_string(item, "location")
    published_at = read_required_string(item, "published_at")
    evidence_url = read_required_string(item, "evidence_url")
    if not title or not location or not published_at or not evidence_url:
        return None
    return {
        "id": build_entity_id("expansion", evidence_url),
        "signal_type": read_optional_string(item, "signal_type"),
        "title": title,
        "location": location,
        "summary": read_optional_string(item, "summary"),
        "published_at": published_at,
        "evidence_url": evidence_url,
    }


def build_market_list(jobs: list[dict], expansion_indicators: list[dict]) -> list[str]:
    markets = []
    for job in jobs:
        location = normalize_location_label(job["location"])
        if location and location not in markets:
            markets.append(location)
    for indicator in expansion_indicators:
        location = normalize_location_label(indicator["location"])
        if location and location not in markets:
            markets.append(location)
    return markets


def normalize_location_label(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if not normalized:
        return None
    lower_value = normalized.lower()
    if lower_value.endswith(" locations") or lower_value == "multiple locations":
        return None
    return normalized


def build_role_cluster_counts(jobs: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for job in jobs:
        for cluster_name in job["role_clusters"]:
            counts[cluster_name] += 1
    return dict(sorted(counts.items()))


def assign_role_clusters(role: str, team: str | None) -> list[str]:
    haystack = canonicalize_text(" ".join(part for part in (role, team or "") if part))
    clusters = [
        cluster_name
        for cluster_name, keywords in ROLE_CLUSTER_KEYWORDS.items()
        if any(keyword in haystack for keyword in keywords)
    ]
    return clusters or ["general"]


def canonicalize_text(value: str) -> str:
    return value.strip().lower().replace("&", " and ")


def build_growth_metrics(
    result: dict,
    jobs: list[dict],
    product_announcements: list[dict],
    funding_mentions: list[dict],
    expansion_indicators: list[dict],
    markets: list[str],
) -> dict | None:
    raw_metrics = result.get("metrics")
    if not isinstance(raw_metrics, dict):
        return None
    growth_value = raw_metrics.get("growth")
    if not isinstance(growth_value, (int, float)) or isinstance(growth_value, bool):
        return None
    return {
        "growth": float(growth_value),
        "jobs_count": len(jobs),
        "product_launch_count": len(product_announcements),
        "funding_mention_count": len(funding_mentions),
        "expansion_indicator_count": len(expansion_indicators),
        "market_count": len(markets),
    }


def apply_growth_source_filters(
    sources: list[dict],
    snapshots: list[dict],
    filters: dict,
) -> tuple[list[dict], list[dict]]:
    if not filters:
        return sources, snapshots
    filtered_sources = [source for source in sources if source_matches_growth_filters(source, filters)]
    source_ids = {source["id"] for source in filtered_sources}
    filtered_snapshots = [snapshot for snapshot in snapshots if snapshot["source_id"] in source_ids]
    return filtered_sources, filtered_snapshots


def select_growth_refresh_sources(sources: list[dict], filters: dict) -> list[dict]:
    if not any(filters.get(key) for key in SOURCE_FILTER_KEYS):
        return sources
    return [source for source in sources if source_matches_growth_filters(source, filters)]


def source_matches_growth_filters(source: dict, filters: dict) -> bool:
    source_key_map = {
        "source_id": "id",
        "company_id": "company_id",
        "source_type": "source_type",
    }
    for key in SOURCE_FILTER_KEYS:
        filter_value = filters.get(key)
        if not filter_value:
            continue
        source_value = source.get(source_key_map[key])
        if source_value != filter_value:
            return False
    return True


def apply_growth_event_filters(events: list[dict], filters: dict) -> list[dict]:
    if not filters:
        return events
    return [event for event in events if event_matches_growth_filters(event, filters)]


def event_matches_growth_filters(event: dict, filters: dict) -> bool:
    signal_type = filters.get("signal_type")
    if signal_type and event.get("signal_type") != signal_type:
        return False
    cluster_name = filters.get("cluster_name")
    if cluster_name and event.get("cluster_name") != cluster_name:
        return False
    location = filters.get("location")
    if location and location not in event.get("locations", []):
        return False
    return True


def build_growth_comparisons(sources: list[dict], snapshots: list[dict]) -> list[dict]:
    snapshots_by_source: dict[str, list[dict]] = defaultdict(list)
    for snapshot in snapshots:
        snapshots_by_source[snapshot["source_id"]].append(snapshot)
    comparisons = []
    for source in sources:
        records = sorted(snapshots_by_source.get(source["id"], []), key=lambda item: item["captured_at"])
        if len(records) < 2:
            continue
        comparison = build_source_growth_comparison(source, records[-2], records[-1])
        if comparison:
            comparisons.append(comparison)
    return sorted(comparisons, key=lambda item: parse_iso_datetime(item["captured_at"]), reverse=True)


def build_growth_comparison_history(sources: list[dict], snapshots: list[dict]) -> list[dict]:
    snapshots_by_source: dict[str, list[dict]] = defaultdict(list)
    for snapshot in snapshots:
        snapshots_by_source[snapshot["source_id"]].append(snapshot)
    comparisons = []
    source_map = {source["id"]: source for source in sources}
    for source_id, records in snapshots_by_source.items():
        source = source_map.get(source_id)
        if not source:
            continue
        ordered = sorted(records, key=lambda item: parse_iso_datetime(item["captured_at"]))
        for previous_snapshot, current_snapshot in zip(ordered, ordered[1:]):
            comparison = build_source_growth_comparison(source, previous_snapshot, current_snapshot)
            if comparison:
                comparisons.append(comparison)
    return sorted(comparisons, key=lambda item: parse_iso_datetime(item["captured_at"]), reverse=True)


def build_source_growth_comparison(source: dict, previous_snapshot: dict, current_snapshot: dict) -> dict | None:
    jobs_added = build_added_entities(previous_snapshot["jobs"], current_snapshot["jobs"])
    product_announcements_added = build_added_entities(
        previous_snapshot["product_announcements"],
        current_snapshot["product_announcements"],
    )
    funding_mentions_added = build_added_entities(
        previous_snapshot["funding_mentions"],
        current_snapshot["funding_mentions"],
    )
    expansion_indicators_added = build_added_entities(
        previous_snapshot["expansion_indicators"],
        current_snapshot["expansion_indicators"],
    )
    new_markets = [
        market
        for market in current_snapshot["markets"]
        if market not in set(previous_snapshot["markets"])
    ]
    role_cluster_deltas = build_role_cluster_deltas(
        previous_snapshot["role_cluster_counts"],
        current_snapshot["role_cluster_counts"],
    )
    if not any(
        (
            jobs_added,
            product_announcements_added,
            funding_mentions_added,
            expansion_indicators_added,
            new_markets,
            role_cluster_deltas,
        )
    ):
        return None
    return {
        "comparison_id": current_snapshot["snapshot_id"],
        "source_id": source["id"],
        "source_name": source["name"],
        "source_type": source["source_type"],
        "company_id": source["company_id"],
        "company_name": source["company_name"],
        "captured_at": current_snapshot["captured_at"],
        "current_snapshot_id": current_snapshot["snapshot_id"],
        "previous_snapshot_id": previous_snapshot["snapshot_id"],
        "current_metrics": current_snapshot["metrics"],
        "previous_metrics": previous_snapshot["metrics"],
        "jobs_added": jobs_added,
        "product_announcements_added": product_announcements_added,
        "funding_mentions_added": funding_mentions_added,
        "expansion_indicators_added": expansion_indicators_added,
        "new_markets": new_markets,
        "role_cluster_deltas": role_cluster_deltas,
    }


def build_added_entities(previous_items: list[dict], current_items: list[dict]) -> list[dict]:
    previous_ids = {item["id"] for item in previous_items}
    return [item for item in current_items if item["id"] not in previous_ids]


def build_role_cluster_deltas(previous_counts: dict[str, int], current_counts: dict[str, int]) -> list[dict]:
    deltas = []
    for cluster_name in sorted(set(previous_counts) | set(current_counts)):
        previous_value = previous_counts.get(cluster_name, 0)
        current_value = current_counts.get(cluster_name, 0)
        if current_value <= previous_value:
            continue
        deltas.append(
            {
                "cluster_name": cluster_name,
                "previous_value": previous_value,
                "current_value": current_value,
                "delta": current_value - previous_value,
            }
        )
    return deltas


def build_growth_events(sources: list[dict], snapshots: list[dict], comparisons: list[dict]) -> list[dict]:
    contexts = build_company_growth_context(comparisons)
    events = []
    for context in contexts:
        hiring_signal = create_hiring_spike_signal(context)
        if hiring_signal:
            events.append(hiring_signal)
        events.extend(create_role_cluster_signals(context))
        events.extend(create_product_launch_signals(context))
        events.extend(create_funding_signals(context))
        events.extend(create_market_entry_signals(context))
        events.extend(create_expansion_signals(context))
    return sorted(
        events,
        key=lambda item: (build_growth_severity_rank(item["severity"]), item["timestamp"]),
        reverse=True,
    )


def build_company_growth_context(comparisons: list[dict]) -> list[dict]:
    grouped: dict[str, dict] = {}
    for comparison in comparisons:
        context = grouped.setdefault(
            comparison["company_id"],
            {
                "company_id": comparison["company_id"],
                "company_name": comparison["company_name"],
                "source_ids": set(),
                "source_types": set(),
                "timestamps": [],
                "snapshot_ids": set(),
                "extraction_timestamps": set(),
                "evidence_urls": set(),
                "current_jobs_count": 0,
                "previous_jobs_count": 0,
                "jobs_added": [],
                "role_cluster_deltas": defaultdict(lambda: {"previous_value": 0, "current_value": 0, "delta": 0}),
                "product_announcements_added": [],
                "funding_mentions_added": [],
                "expansion_indicators_added": [],
                "new_markets": [],
            },
        )
        context["source_ids"].add(comparison["source_id"])
        context["source_types"].add(comparison["source_type"])
        context["timestamps"].append(comparison["captured_at"])
        context["snapshot_ids"].update([comparison["current_snapshot_id"], comparison["previous_snapshot_id"]])
        context["extraction_timestamps"].add(comparison["captured_at"])
        if comparison["source_type"] in {"career_page", "job_board"}:
            context["current_jobs_count"] += comparison["current_metrics"]["jobs_count"]
            context["previous_jobs_count"] += comparison["previous_metrics"]["jobs_count"]
        context["jobs_added"].extend(comparison["jobs_added"])
        context["product_announcements_added"].extend(comparison["product_announcements_added"])
        context["funding_mentions_added"].extend(comparison["funding_mentions_added"])
        context["expansion_indicators_added"].extend(comparison["expansion_indicators_added"])
        for item in comparison["jobs_added"]:
            context["evidence_urls"].add(item["evidence_url"])
        for item in comparison["product_announcements_added"]:
            context["evidence_urls"].add(item["evidence_url"])
            context["extraction_timestamps"].add(item["published_at"])
        for item in comparison["funding_mentions_added"]:
            context["evidence_urls"].add(item["evidence_url"])
            context["extraction_timestamps"].add(item["published_at"])
        for item in comparison["expansion_indicators_added"]:
            context["evidence_urls"].add(item["evidence_url"])
            context["extraction_timestamps"].add(item["published_at"])
        for market in comparison["new_markets"]:
            if market not in context["new_markets"]:
                context["new_markets"].append(market)
        for delta in comparison["role_cluster_deltas"]:
            aggregate = context["role_cluster_deltas"][delta["cluster_name"]]
            aggregate["previous_value"] += delta["previous_value"]
            aggregate["current_value"] += delta["current_value"]
            aggregate["delta"] += delta["delta"]
    contexts = []
    for context in grouped.values():
        contexts.append(
            {
                **context,
                "source_ids": sorted(context["source_ids"]),
                "source_types": sorted(context["source_types"]),
                "snapshot_ids": sorted(context["snapshot_ids"]),
                "extraction_timestamps": sorted(context["extraction_timestamps"]),
                "evidence_urls": sorted(context["evidence_urls"]),
                "timestamp": max(context["timestamps"], key=parse_iso_datetime),
                "role_cluster_deltas": [
                    {"cluster_name": cluster_name, **values}
                    for cluster_name, values in sorted(context["role_cluster_deltas"].items())
                    if values["delta"] > 0
                ],
            }
        )
    return contexts


def build_growth_event_provenance(
    context: dict,
    evidence_urls: list[str] | None = None,
    extraction_timestamps: list[str] | None = None,
) -> dict:
    combined_timestamps = list(context["extraction_timestamps"])
    if extraction_timestamps:
        combined_timestamps.extend(extraction_timestamps)
    combined_urls = list(context["evidence_urls"])
    if evidence_urls:
        combined_urls.extend(evidence_urls)
    return {
        "source_ids": context["source_ids"],
        "snapshot_ids": context["snapshot_ids"],
        "extraction_timestamps": sorted({timestamp for timestamp in combined_timestamps if isinstance(timestamp, str) and timestamp}),
        "evidence_urls": sorted({url for url in combined_urls if isinstance(url, str) and url}),
    }


def create_hiring_spike_signal(context: dict) -> dict | None:
    previous_jobs_count = context["previous_jobs_count"]
    current_jobs_count = context["current_jobs_count"]
    if current_jobs_count <= previous_jobs_count:
        return None
    delta = current_jobs_count - previous_jobs_count
    delta_ratio = delta / previous_jobs_count if previous_jobs_count else 1.0
    if delta < 3 and delta_ratio < 0.15:
        return None
    return {
        "id": build_signal_id(context["company_id"], "hiring_spike", context["timestamp"]),
        "signal_type": "hiring_spike",
        "category": "growth_intelligence",
        "severity": build_hiring_severity(delta, delta_ratio),
        "company_id": context["company_id"],
        "company_name": context["company_name"],
        "source_ids": context["source_ids"],
        "source_types": context["source_types"],
        "timestamp": context["timestamp"],
        "title": f"{context['company_name']} hiring activity increased",
        "summary": (
            f"Open roles increased from {previous_jobs_count} to {current_jobs_count} across monitored career sources."
        ),
        "current_value": current_jobs_count,
        "previous_value": previous_jobs_count,
        "delta": delta,
        "delta_ratio": round(delta_ratio, 4),
        "cluster_name": "hiring",
        "locations": sorted({job["location"] for job in context["jobs_added"] if normalize_location_label(job["location"])}),
        "evidence": [
            {
                "label": job["role"],
                "url": job["evidence_url"],
                "location": job["location"],
                "timestamp": job["timestamp"],
            }
            for job in context["jobs_added"][:5]
        ],
        "provenance": build_growth_event_provenance(
            context,
            evidence_urls=[job["evidence_url"] for job in context["jobs_added"][:5]],
            extraction_timestamps=[job["timestamp"] for job in context["jobs_added"][:5]],
        ),
    }


def create_role_cluster_signals(context: dict) -> list[dict]:
    signals = []
    for delta in context["role_cluster_deltas"]:
        if delta["delta"] < 2 and delta["current_value"] < 3:
            continue
        matching_jobs = [
            job
            for job in context["jobs_added"]
            if delta["cluster_name"] in job["role_clusters"]
        ]
        signals.append(
            {
                "id": build_signal_id(context["company_id"], f"role_cluster_{delta['cluster_name']}", context["timestamp"]),
                "signal_type": "role_cluster_surge",
                "category": "growth_intelligence",
                "severity": build_role_cluster_severity(delta["delta"]),
                "company_id": context["company_id"],
                "company_name": context["company_name"],
                "source_ids": context["source_ids"],
                "source_types": context["source_types"],
                "timestamp": context["timestamp"],
                "title": f"{context['company_name']} increased {delta['cluster_name']} hiring",
                "summary": (
                    f"{delta['cluster_name'].replace('_', ' ').title()} roles increased by {delta['delta']} "
                    f"between the latest growth snapshots."
                ),
                "current_value": delta["current_value"],
                "previous_value": delta["previous_value"],
                "delta": delta["delta"],
                "delta_ratio": round(
                    delta["delta"] / delta["previous_value"],
                    4,
                ) if delta["previous_value"] else 1.0,
                "cluster_name": delta["cluster_name"],
                "locations": sorted({job["location"] for job in matching_jobs if normalize_location_label(job["location"])}),
                "evidence": [
                    {
                        "label": job["role"],
                        "url": job["evidence_url"],
                        "location": job["location"],
                        "timestamp": job["timestamp"],
                    }
                    for job in matching_jobs[:5]
                ],
                "provenance": build_growth_event_provenance(
                    context,
                    evidence_urls=[job["evidence_url"] for job in matching_jobs[:5]],
                    extraction_timestamps=[job["timestamp"] for job in matching_jobs[:5]],
                ),
            }
        )
    return signals


def create_product_launch_signals(context: dict) -> list[dict]:
    signals = []
    for announcement in context["product_announcements_added"]:
        signals.append(
            {
                "id": build_signal_id(context["company_id"], announcement["id"], announcement["published_at"]),
                "signal_type": "product_launch",
                "category": "growth_intelligence",
                "severity": "high",
                "company_id": context["company_id"],
                "company_name": context["company_name"],
                "source_ids": context["source_ids"],
                "source_types": context["source_types"],
                "timestamp": announcement["published_at"],
                "title": announcement["title"],
                "summary": announcement["summary"] or "New product-facing announcement detected on a monitored source.",
                "current_value": 1,
                "previous_value": 0,
                "delta": 1,
                "delta_ratio": 1.0,
                "cluster_name": "product",
                "locations": [],
                "evidence": [
                    {
                        "label": announcement["title"],
                        "url": announcement["evidence_url"],
                        "location": None,
                        "timestamp": announcement["published_at"],
                    }
                ],
                "provenance": build_growth_event_provenance(
                    context,
                    evidence_urls=[announcement["evidence_url"]],
                    extraction_timestamps=[announcement["published_at"]],
                ),
            }
        )
    return signals


def create_funding_signals(context: dict) -> list[dict]:
    signals = []
    for mention in context["funding_mentions_added"]:
        signals.append(
            {
                "id": build_signal_id(context["company_id"], mention["id"], mention["published_at"]),
                "signal_type": "funding_mention",
                "category": "growth_intelligence",
                "severity": "high",
                "company_id": context["company_id"],
                "company_name": context["company_name"],
                "source_ids": context["source_ids"],
                "source_types": context["source_types"],
                "timestamp": mention["published_at"],
                "title": mention["title"],
                "summary": mention["summary"] or "New funding-related language detected on a monitored source.",
                "current_value": 1,
                "previous_value": 0,
                "delta": 1,
                "delta_ratio": 1.0,
                "cluster_name": "funding",
                "locations": [],
                "evidence": [
                    {
                        "label": mention["title"],
                        "url": mention["evidence_url"],
                        "location": None,
                        "timestamp": mention["published_at"],
                    }
                ],
                "provenance": build_growth_event_provenance(
                    context,
                    evidence_urls=[mention["evidence_url"]],
                    extraction_timestamps=[mention["published_at"]],
                ),
            }
        )
    return signals


def create_market_entry_signals(context: dict) -> list[dict]:
    signals = []
    for market in context["new_markets"]:
        signals.append(
            {
                "id": build_signal_id(context["company_id"], f"market_{market}", context["timestamp"]),
                "signal_type": "market_entry",
                "category": "growth_intelligence",
                "severity": "medium",
                "company_id": context["company_id"],
                "company_name": context["company_name"],
                "source_ids": context["source_ids"],
                "source_types": context["source_types"],
                "timestamp": context["timestamp"],
                "title": f"{context['company_name']} showed activity in a new market",
                "summary": f"New location signal detected in {market}.",
                "current_value": 1,
                "previous_value": 0,
                "delta": 1,
                "delta_ratio": 1.0,
                "cluster_name": "market_expansion",
                "locations": [market],
                "evidence": [],
                "provenance": build_growth_event_provenance(context),
            }
        )
    return signals


def create_expansion_signals(context: dict) -> list[dict]:
    signals = []
    for indicator in context["expansion_indicators_added"]:
        location = normalize_location_label(indicator["location"])
        signals.append(
            {
                "id": build_signal_id(context["company_id"], indicator["id"], indicator["published_at"]),
                "signal_type": "geographic_expansion",
                "category": "growth_intelligence",
                "severity": "medium",
                "company_id": context["company_id"],
                "company_name": context["company_name"],
                "source_ids": context["source_ids"],
                "source_types": context["source_types"],
                "timestamp": indicator["published_at"],
                "title": indicator["title"],
                "summary": indicator["summary"] or "New expansion-related language detected on a monitored source.",
                "current_value": 1,
                "previous_value": 0,
                "delta": 1,
                "delta_ratio": 1.0,
                "cluster_name": "market_expansion",
                "locations": [location] if location else [],
                "evidence": [
                    {
                        "label": indicator["title"],
                        "url": indicator["evidence_url"],
                        "location": location,
                        "timestamp": indicator["published_at"],
                    }
                ],
                "provenance": build_growth_event_provenance(
                    context,
                    evidence_urls=[indicator["evidence_url"]],
                    extraction_timestamps=[indicator["published_at"]],
                ),
            }
        )
    return signals


def build_growth_history_event_occurrences(comparisons: list[dict]) -> list[dict]:
    events = []
    for comparison in comparisons:
        events.extend(build_growth_occurrences_for_comparison(comparison))
    return sorted(events, key=lambda item: (build_growth_severity_rank(item["severity"]), item["timestamp"]), reverse=True)


def build_growth_occurrences_for_comparison(comparison: dict) -> list[dict]:
    events = []
    current_jobs_count = comparison["current_metrics"]["jobs_count"]
    previous_jobs_count = comparison["previous_metrics"]["jobs_count"]
    jobs_delta = current_jobs_count - previous_jobs_count
    if comparison["source_type"] in {"career_page", "job_board"} and jobs_delta > 0:
        delta_ratio = jobs_delta / previous_jobs_count if previous_jobs_count else 1.0
        if jobs_delta >= 3 or delta_ratio >= 0.15:
            events.append(
                {
                    "id": f"{comparison['comparison_id']}::hiring_spike",
                    "signal_type": "hiring_spike",
                    "category": "growth_intelligence",
                    "severity": build_hiring_severity(jobs_delta, delta_ratio),
                    "company_id": comparison["company_id"],
                    "company_name": comparison["company_name"],
                    "source_ids": [comparison["source_id"]],
                    "source_types": [comparison["source_type"]],
                    "timestamp": comparison["captured_at"],
                    "title": f"{comparison['company_name']} hiring activity increased",
                    "summary": (
                        f"Open roles increased from {previous_jobs_count} to {current_jobs_count} on {comparison['source_name']}."
                    ),
                    "current_value": current_jobs_count,
                    "previous_value": previous_jobs_count,
                    "delta": jobs_delta,
                    "delta_ratio": round(delta_ratio, 4),
                    "cluster_name": "hiring",
                    "locations": sorted(
                        {
                            job["location"]
                            for job in comparison["jobs_added"]
                            if normalize_location_label(job["location"])
                        }
                    ),
                    "evidence": [
                        {
                            "label": job["role"],
                            "url": job["evidence_url"],
                            "location": job["location"],
                            "timestamp": job["timestamp"],
                        }
                        for job in comparison["jobs_added"][:5]
                    ],
                    "provenance": {
                        "source_ids": [comparison["source_id"]],
                        "snapshot_ids": [
                            comparison["current_snapshot_id"],
                            comparison["previous_snapshot_id"],
                        ],
                        "extraction_timestamps": [comparison["captured_at"]],
                    },
                }
            )
    for delta in comparison["role_cluster_deltas"]:
        events.append(
            {
                "id": f"{comparison['comparison_id']}::role_cluster::{delta['cluster_name']}",
                "signal_type": "role_cluster_surge",
                "category": "growth_intelligence",
                "severity": build_role_cluster_severity(delta["delta"]),
                "company_id": comparison["company_id"],
                "company_name": comparison["company_name"],
                "source_ids": [comparison["source_id"]],
                "source_types": [comparison["source_type"]],
                "timestamp": comparison["captured_at"],
                "title": f"{comparison['company_name']} increased {delta['cluster_name']} hiring",
                "summary": (
                    f"{delta['cluster_name'].replace('_', ' ').title()} roles increased by {delta['delta']} on "
                    f"{comparison['source_name']}."
                ),
                "current_value": delta["current_value"],
                "previous_value": delta["previous_value"],
                "delta": delta["delta"],
                "delta_ratio": round(delta["delta"] / delta["previous_value"], 4) if delta["previous_value"] else 1.0,
                "cluster_name": delta["cluster_name"],
                "locations": [],
                "evidence": [],
                "provenance": {
                    "source_ids": [comparison["source_id"]],
                    "snapshot_ids": [
                        comparison["current_snapshot_id"],
                        comparison["previous_snapshot_id"],
                    ],
                    "extraction_timestamps": [comparison["captured_at"]],
                },
            }
        )
    for announcement in comparison["product_announcements_added"]:
        events.append(
            {
                "id": f"{comparison['comparison_id']}::{announcement['id']}",
                "signal_type": "product_launch",
                "category": "growth_intelligence",
                "severity": "high",
                "company_id": comparison["company_id"],
                "company_name": comparison["company_name"],
                "source_ids": [comparison["source_id"]],
                "source_types": [comparison["source_type"]],
                "timestamp": announcement["published_at"],
                "title": announcement["title"],
                "summary": announcement["summary"] or "Product-facing announcement detected.",
                "current_value": 1,
                "previous_value": 0,
                "delta": 1,
                "delta_ratio": 1.0,
                "cluster_name": "product",
                "locations": [],
                "evidence": [
                    {
                        "label": announcement["title"],
                        "url": announcement["evidence_url"],
                        "location": None,
                        "timestamp": announcement["published_at"],
                    }
                ],
                "provenance": {
                    "source_ids": [comparison["source_id"]],
                    "snapshot_ids": [
                        comparison["current_snapshot_id"],
                        comparison["previous_snapshot_id"],
                    ],
                    "extraction_timestamps": [comparison["captured_at"], announcement["published_at"]],
                },
            }
        )
    for mention in comparison["funding_mentions_added"]:
        events.append(
            {
                "id": f"{comparison['comparison_id']}::{mention['id']}",
                "signal_type": "funding_mention",
                "category": "growth_intelligence",
                "severity": "high",
                "company_id": comparison["company_id"],
                "company_name": comparison["company_name"],
                "source_ids": [comparison["source_id"]],
                "source_types": [comparison["source_type"]],
                "timestamp": mention["published_at"],
                "title": mention["title"],
                "summary": mention["summary"] or "Funding-related language detected.",
                "current_value": 1,
                "previous_value": 0,
                "delta": 1,
                "delta_ratio": 1.0,
                "cluster_name": "funding",
                "locations": [],
                "evidence": [
                    {
                        "label": mention["title"],
                        "url": mention["evidence_url"],
                        "location": None,
                        "timestamp": mention["published_at"],
                    }
                ],
                "provenance": {
                    "source_ids": [comparison["source_id"]],
                    "snapshot_ids": [
                        comparison["current_snapshot_id"],
                        comparison["previous_snapshot_id"],
                    ],
                    "extraction_timestamps": [comparison["captured_at"], mention["published_at"]],
                },
            }
        )
    for market in comparison["new_markets"]:
        events.append(
            {
                "id": f"{comparison['comparison_id']}::market::{market}",
                "signal_type": "market_entry",
                "category": "growth_intelligence",
                "severity": "medium",
                "company_id": comparison["company_id"],
                "company_name": comparison["company_name"],
                "source_ids": [comparison["source_id"]],
                "source_types": [comparison["source_type"]],
                "timestamp": comparison["captured_at"],
                "title": f"{comparison['company_name']} showed activity in a new market",
                "summary": f"New location signal detected in {market}.",
                "current_value": 1,
                "previous_value": 0,
                "delta": 1,
                "delta_ratio": 1.0,
                "cluster_name": "market_expansion",
                "locations": [market],
                "evidence": [],
                "provenance": {
                    "source_ids": [comparison["source_id"]],
                    "snapshot_ids": [
                        comparison["current_snapshot_id"],
                        comparison["previous_snapshot_id"],
                    ],
                    "extraction_timestamps": [comparison["captured_at"]],
                },
            }
        )
    for indicator in comparison["expansion_indicators_added"]:
        location = normalize_location_label(indicator["location"])
        events.append(
            {
                "id": f"{comparison['comparison_id']}::{indicator['id']}",
                "signal_type": "geographic_expansion",
                "category": "growth_intelligence",
                "severity": "medium",
                "company_id": comparison["company_id"],
                "company_name": comparison["company_name"],
                "source_ids": [comparison["source_id"]],
                "source_types": [comparison["source_type"]],
                "timestamp": indicator["published_at"],
                "title": indicator["title"],
                "summary": indicator["summary"] or "Expansion-related language detected.",
                "current_value": 1,
                "previous_value": 0,
                "delta": 1,
                "delta_ratio": 1.0,
                "cluster_name": "market_expansion",
                "locations": [location] if location else [],
                "evidence": [
                    {
                        "label": indicator["title"],
                        "url": indicator["evidence_url"],
                        "location": location,
                        "timestamp": indicator["published_at"],
                    }
                ],
                "provenance": {
                    "source_ids": [comparison["source_id"]],
                    "snapshot_ids": [
                        comparison["current_snapshot_id"],
                        comparison["previous_snapshot_id"],
                    ],
                    "extraction_timestamps": [comparison["captured_at"], indicator["published_at"]],
                },
            }
        )
    return events


def build_hiring_severity(delta: int, delta_ratio: float) -> str:
    if delta >= 20 or delta_ratio >= 0.5:
        return "critical"
    if delta >= 10 or delta_ratio >= 0.25:
        return "high"
    return "medium"


def build_role_cluster_severity(delta: int) -> str:
    if delta >= 5:
        return "high"
    if delta >= 2:
        return "medium"
    return "low"


def build_growth_signal_clusters(events: list[dict]) -> list[dict]:
    grouped: dict[str, dict] = {}
    for event in events:
        cluster = grouped.setdefault(
            event["company_id"],
            {
                "id": f"cluster-{event['company_id']}",
                "company_id": event["company_id"],
                "company_name": event["company_name"],
                "signal_ids": [],
                "signal_types": set(),
                "cluster_names": set(),
                "locations": set(),
                "latest_timestamp": event["timestamp"],
            },
        )
        cluster["signal_ids"].append(event["id"])
        cluster["signal_types"].add(event["signal_type"])
        if event.get("cluster_name"):
            cluster["cluster_names"].add(event["cluster_name"])
        for location in event.get("locations", []):
            cluster["locations"].add(location)
        if parse_iso_datetime(event["timestamp"]) > parse_iso_datetime(cluster["latest_timestamp"]):
            cluster["latest_timestamp"] = event["timestamp"]
    return [
        {
            "id": cluster["id"],
            "company_id": cluster["company_id"],
            "company_name": cluster["company_name"],
            "signal_ids": sorted(cluster["signal_ids"]),
            "signal_types": sorted(cluster["signal_types"]),
            "cluster_names": sorted(cluster["cluster_names"]),
            "locations": sorted(cluster["locations"]),
            "latest_timestamp": cluster["latest_timestamp"],
        }
        for cluster in grouped.values()
    ]


def build_current_growth_activity(snapshots: list[dict]) -> dict:
    latest_by_source = {}
    for snapshot in snapshots:
        latest_by_source[snapshot["source_id"]] = snapshot
    jobs = []
    product_announcements = []
    funding_mentions = []
    expansion_indicators = []
    markets = set()
    for snapshot in latest_by_source.values():
        jobs.extend(snapshot["jobs"])
        product_announcements.extend(snapshot["product_announcements"])
        funding_mentions.extend(snapshot["funding_mentions"])
        expansion_indicators.extend(snapshot["expansion_indicators"])
        markets.update(snapshot["markets"])
    jobs = sorted(jobs, key=lambda item: parse_iso_datetime(item["timestamp"]), reverse=True)
    product_announcements = sorted(
        product_announcements,
        key=lambda item: parse_iso_datetime(item["published_at"]),
        reverse=True,
    )
    funding_mentions = sorted(
        funding_mentions,
        key=lambda item: parse_iso_datetime(item["published_at"]),
        reverse=True,
    )
    expansion_indicators = sorted(
        expansion_indicators,
        key=lambda item: parse_iso_datetime(item["published_at"]),
        reverse=True,
    )
    return {
        "jobs": jobs[:25],
        "product_announcements": product_announcements[:20],
        "funding_mentions": funding_mentions[:20],
        "expansion_indicators": expansion_indicators[:20],
        "markets": sorted(markets),
    }


def build_growth_company_rollups(
    sources: list[dict],
    snapshots: list[dict],
    events: list[dict],
    strategic_insights: list[dict],
) -> list[dict]:
    latest_by_company: dict[str, dict] = {}
    for snapshot in snapshots:
        company = latest_by_company.setdefault(
            snapshot["company_id"],
            {
                "company_id": snapshot["company_id"],
                "company_name": snapshot["company_name"],
                "latest_timestamp": snapshot["captured_at"],
                "jobs_count": 0,
                "product_launch_count": 0,
                "funding_mention_count": 0,
                "market_count": 0,
                "source_ids": set(),
            },
        )
        if parse_iso_datetime(snapshot["captured_at"]) > parse_iso_datetime(company["latest_timestamp"]):
            company["latest_timestamp"] = snapshot["captured_at"]
        company["jobs_count"] += snapshot["metrics"]["jobs_count"]
        company["product_launch_count"] += snapshot["metrics"]["product_launch_count"]
        company["funding_mention_count"] += snapshot["metrics"]["funding_mention_count"]
        company["market_count"] = max(company["market_count"], snapshot["metrics"]["market_count"])
        company["source_ids"].add(snapshot["source_id"])
    events_by_company: dict[str, list[dict]] = defaultdict(list)
    for event in events:
        events_by_company[event["company_id"]].append(event)
    insights_by_company: dict[str, list[dict]] = defaultdict(list)
    for insight in strategic_insights:
        insights_by_company[insight["company_id"]].append(insight)
    return [
        {
            "company_id": company["company_id"],
            "company_name": company["company_name"],
            "latest_timestamp": company["latest_timestamp"],
            "jobs_count": company["jobs_count"],
            "product_launch_count": company["product_launch_count"],
            "funding_mention_count": company["funding_mention_count"],
            "market_count": company["market_count"],
            "source_ids": sorted(company["source_ids"]),
            "active_event_count": len(events_by_company.get(company["company_id"], [])),
            "strategic_insights": insights_by_company.get(company["company_id"], []),
        }
        for company in sorted(latest_by_company.values(), key=lambda item: item["company_name"])
    ]


def build_growth_time_series(snapshots: list[dict]) -> dict:
    series = {
        "jobs": [],
        "products": [],
        "funding": [],
        "markets": [],
        "role_clusters": [],
    }
    for snapshot in snapshots:
        timestamp = snapshot["captured_at"]
        series["jobs"].append(build_growth_series_point(snapshot, timestamp, "jobs_count"))
        series["products"].append(build_growth_series_point(snapshot, timestamp, "product_launch_count"))
        series["funding"].append(build_growth_series_point(snapshot, timestamp, "funding_mention_count"))
        series["markets"].append(build_growth_series_point(snapshot, timestamp, "market_count"))
        for cluster_name, count in snapshot["role_cluster_counts"].items():
            series["role_clusters"].append(
                {
                    "timestamp": timestamp,
                    "company_id": snapshot["company_id"],
                    "source_id": snapshot["source_id"],
                    "cluster_name": cluster_name,
                    "value": count,
                }
            )
    return series


def build_growth_series_point(snapshot: dict, timestamp: str, metric_name: str) -> dict:
    return {
        "timestamp": timestamp,
        "company_id": snapshot["company_id"],
        "source_id": snapshot["source_id"],
        "value": snapshot["metrics"][metric_name],
    }


def build_growth_meta(settings: Settings, dataset: dict, refresh: bool) -> dict:
    latest_snapshot_at = dataset["snapshots"][-1]["captured_at"] if dataset["snapshots"] else None
    return {
        "api_version": "v1",
        "module": "growth_intelligence",
        "contract_version": build_contract_payload("growth_intelligence")["contract_version"],
        "generated_at": to_iso_timestamp(datetime.now(timezone.utc)),
        "refresh_requested": refresh,
        "source_count": len(dataset["sources"]),
        "snapshot_count": len(dataset["snapshots"]),
        "latest_snapshot_at": latest_snapshot_at,
        "filters": dataset["filters"],
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


def build_growth_kpis(dataset: dict) -> list[dict]:
    now = datetime.now(timezone.utc)
    event_count_last_24h = len(
        [
            event
            for event in dataset["events"]
            if parse_iso_datetime(event["timestamp"]) >= now - timedelta(hours=24)
        ]
    )
    return [
        {"id": "growth_events", "label": "Growth Events", "value": len(dataset["events"])},
        {"id": "events_last_24h", "label": "Events (24h)", "value": event_count_last_24h},
        {"id": "tracked_companies", "label": "Tracked Companies", "value": len(dataset["companies"])},
        {"id": "strategic_insights", "label": "Strategic Insights", "value": len(dataset["strategic_insights"])},
    ]


def build_entity_id(prefix: str, raw_value: str) -> str:
    normalized = canonicalize_text(raw_value)
    normalized = normalized.replace("https://", "").replace("http://", "")
    normalized = normalized.replace("/", "-").replace("?", "-").replace("=", "-").replace(" ", "-")
    return f"{prefix}-{normalized}"


def build_signal_id(company_id: str, fragment: str, timestamp: str) -> str:
    normalized_fragment = canonicalize_text(fragment).replace(" ", "-").replace("/", "-")
    normalized_timestamp = timestamp.replace(":", "").replace("-", "")
    return f"{company_id}-{normalized_fragment}-{normalized_timestamp}"


def read_required_string(payload: dict, field_name: str) -> str | None:
    value = payload.get(field_name)
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if not normalized:
        return None
    return normalized


def read_optional_string(payload: dict, field_name: str) -> str | None:
    value = payload.get(field_name)
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if not normalized:
        return None
    return normalized


def build_growth_severity_rank(severity: str) -> int:
    severity_map = {
        "critical": 4,
        "high": 3,
        "medium": 2,
        "low": 1,
    }
    return severity_map.get(severity, 0)
