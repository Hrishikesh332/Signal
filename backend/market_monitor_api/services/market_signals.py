from collections import defaultdict
from datetime import datetime, timezone

from market_monitor_api.config import Settings
from market_monitor_api.services.commerce_intelligence import CommerceConfigError, collect_commerce_dataset
from market_monitor_api.services.growth_intelligence import GrowthConfigError, collect_growth_dataset
from market_monitor_api.services.tinyfish import parse_iso_datetime, to_iso_timestamp


VALID_MARKET_SIGNAL_CATEGORIES = {
    "commerce_intelligence",
    "growth_intelligence",
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

SEVERITY_RANKS = {
    "critical": 4,
    "high": 3,
    "medium": 2,
    "low": 1,
}


class MarketSignalsConfigError(Exception):
    def __init__(self, code: str, message: str, status_code: int = 503):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


def build_market_signals_response(settings: Settings, refresh: bool = False, filters: dict | None = None) -> dict:
    dataset = collect_market_signals_dataset(settings, refresh=refresh, filters=filters or {})
    return {
        "meta": build_market_signals_meta(settings, dataset, refresh),
        "summary": build_market_signals_summary(dataset["items"]),
        "wire": {
            "stats": build_wire_stats(dataset["items"]),
            "items": dataset["items"],
        },
        "category_status": dataset["category_status"],
        "source_health": dataset["source_health"],
        "sources": dataset["sources"],
        "companies": dataset["companies"],
        "facets": build_market_signal_facets(dataset),
    }


def collect_market_signals_dataset(settings: Settings, refresh: bool, filters: dict) -> dict:
    requested_categories = select_requested_categories(filters)
    collected_items = []
    collected_source_health = []
    collected_sources = []
    collected_companies = []
    category_status = []
    latest_snapshot_candidates = []
    for category in requested_categories:
        category_result = collect_market_signal_category(settings, category, refresh, filters)
        category_status.append(category_result["status"])
        collected_source_health.extend(category_result["source_health"])
        collected_sources.extend(category_result["sources"])
        collected_companies.extend(category_result["companies"])
        latest_snapshot_at = category_result["status"].get("latest_snapshot_at")
        if latest_snapshot_at:
            latest_snapshot_candidates.append(latest_snapshot_at)
        collected_items.extend(category_result["items"])
    filtered_items = sort_market_signal_items(apply_market_signal_filters(collected_items, filters))
    limited_items = apply_market_signal_limit(filtered_items, filters)
    return {
        "all_items": filtered_items,
        "items": sort_market_signal_items(limited_items),
        "category_status": category_status,
        "source_health": sort_market_signal_source_health(collected_source_health),
        "sources": deduplicate_market_signal_sources(collected_sources),
        "companies": deduplicate_market_signal_companies(collected_companies),
        "filters": filters,
        "latest_snapshot_at": max(latest_snapshot_candidates, key=parse_iso_datetime) if latest_snapshot_candidates else None,
    }


def select_requested_categories(filters: dict) -> list[str]:
    category = filters.get("category")
    if category in VALID_MARKET_SIGNAL_CATEGORIES:
        return [category]
    return sorted(VALID_MARKET_SIGNAL_CATEGORIES)


def collect_market_signal_category(settings: Settings, category: str, refresh: bool, filters: dict) -> dict:
    if category == "commerce_intelligence":
        return collect_market_signal_commerce_category(settings, refresh, filters)
    if category == "growth_intelligence":
        return collect_market_signal_growth_category(settings, refresh, filters)
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
    items = [
        build_commerce_market_signal_item(signal, source_map)
        for signal in dataset["signals"]
    ]
    return {
        "items": [item for item in items if item],
        "source_health": build_market_signal_source_health("commerce_intelligence", dataset["sources"], dataset["source_health"]),
        "sources": build_market_signal_sources("commerce_intelligence", dataset["sources"]),
        "companies": build_market_signal_companies("commerce_intelligence", dataset["sources"]),
        "status": build_market_signal_category_status(
            "commerce_intelligence",
            len(dataset["sources"]),
            len(dataset["snapshots"]),
            len(dataset["signals"]),
            dataset["snapshots"][-1]["captured_at"] if dataset["snapshots"] else None,
            dataset["source_health"],
        ),
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
    insight_map = build_growth_signal_insight_map(dataset["strategic_insights"])
    items = [
        build_growth_market_signal_item(event, source_map, insight_map)
        for event in dataset["events"]
    ]
    return {
        "items": [item for item in items if item],
        "source_health": build_market_signal_source_health("growth_intelligence", dataset["sources"], dataset["source_health"]),
        "sources": build_market_signal_sources("growth_intelligence", dataset["sources"]),
        "companies": build_market_signal_companies("growth_intelligence", dataset["sources"]),
        "status": build_market_signal_category_status(
            "growth_intelligence",
            len(dataset["sources"]),
            len(dataset["snapshots"]),
            len(dataset["events"]),
            dataset["snapshots"][-1]["captured_at"] if dataset["snapshots"] else None,
            dataset["source_health"],
        ),
    }


def build_market_signal_error_category(category: str, exc: Exception) -> dict:
    return {
        "items": [],
        "source_health": [],
        "sources": [],
        "companies": [],
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


def build_market_signal_sources(category: str, sources: list[dict]) -> list[dict]:
    return [
        {
            "category": category,
            "source_id": source["id"],
            "source_name": source["name"],
            "company_id": source["company_id"],
            "company_name": source["company_name"],
            "target_url": source["target_url"],
        }
        for source in sources
    ]


def build_market_signal_companies(category: str, sources: list[dict]) -> list[dict]:
    companies = {}
    for source in sources:
        companies[source["company_id"]] = {
            "company_id": source["company_id"],
            "company_name": source["company_name"],
            "categories": [],
        }
    for company in companies.values():
        company["categories"].append(category)
    return sorted(companies.values(), key=lambda item: item["company_name"])


def build_market_signal_source_health(category: str, sources: list[dict], records: list[dict]) -> list[dict]:
    source_map = {source["id"]: source for source in sources}
    enriched_records = []
    for record in records:
        source = source_map.get(record["source_id"])
        enriched_records.append(
            {
                **record,
                "category": category,
                "source_name": source["name"] if source else None,
                "company_id": source["company_id"] if source else None,
                "company_name": source["company_name"] if source else None,
            }
        )
    return enriched_records


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
        last_error = record.get("last_error")
        if last_error:
            return last_error
    return None


def build_commerce_market_signal_item(signal: dict, source_map: dict[str, dict]) -> dict:
    source = source_map.get(signal["source_id"])
    timestamp = signal["timestamp"]
    primary_context = signal.get("marketplace")
    return {
        "id": signal["id"],
        "title": build_commerce_market_signal_title(signal),
        "summary": build_commerce_market_signal_summary(signal),
        "category": signal["category"],
        "signal_type": signal["signal_type"],
        "severity": signal["severity"],
        "wire_level": build_wire_level(signal["severity"]),
        "timestamp": timestamp,
        "relative_time_label": build_relative_time_label(timestamp),
        "company_id": signal["company_id"],
        "company_name": signal["company_name"],
        "competitor_id": signal["competitor_id"],
        "competitor_name": signal["competitor_name"],
        "product_name": signal["product_name"],
        "marketplace": signal["marketplace"],
        "location_label": primary_context,
        "locations": [primary_context] if primary_context else [],
        "tracking_group_id": signal["tracking_group_id"],
        "source_id": signal["source_id"],
        "source_ids": [signal["source_id"]],
        "source_name": signal["source_name"],
        "source_names": [signal["source_name"]],
        "source_types": [],
        "confidence_score": signal.get("confidence_score"),
        "impact_score": signal.get("impact_score"),
        "tags": build_commerce_market_signal_tags(signal),
        "evidence": build_commerce_market_signal_evidence(signal),
        "detail": {
            "current_value": signal["current_value"],
            "previous_value": signal["previous_value"],
            "delta": signal["delta"],
            "seller": signal["seller"],
            "product_url": signal["product_url"],
            "image_url": signal["image_url"],
            "target_url": source["target_url"] if source else None,
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
        return (
            f"{product_name} now shows {signal['current_value'].get('discount_percent')}% discount on {marketplace_label}."
        )
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


def build_growth_market_signal_item(
    event: dict,
    source_map: dict[str, dict],
    insight_map: dict[str, dict],
) -> dict:
    primary_source_id = event["source_ids"][0] if event["source_ids"] else None
    primary_source = source_map.get(primary_source_id) if primary_source_id else None
    primary_location = event["locations"][0] if event["locations"] else None
    insight = insight_map.get(event["id"])
    source_names = [
        source_map[source_id]["name"]
        for source_id in event["source_ids"]
        if source_id in source_map
    ]
    return {
        "id": event["id"],
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
        "product_name": None,
        "marketplace": None,
        "location_label": primary_location,
        "locations": event["locations"],
        "tracking_group_id": None,
        "source_id": primary_source_id,
        "source_ids": event["source_ids"],
        "source_name": primary_source["name"] if primary_source else None,
        "source_names": source_names,
        "source_types": event.get("source_types", []),
        "confidence_score": insight["confidence_score"] if insight else None,
        "impact_score": insight["impact_score"] if insight else None,
        "tags": build_growth_market_signal_tags(event),
        "evidence": event["evidence"],
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


def build_growth_market_signal_tags(event: dict) -> list[str]:
    tags = [event["signal_type"], event["severity"]]
    if event.get("cluster_name"):
        tags.append(event["cluster_name"])
    for location in event.get("locations", [])[:2]:
        tags.append(location)
    return tags


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


def apply_market_signal_filters(items: list[dict], filters: dict) -> list[dict]:
    filtered_items = items
    if filters.get("severity"):
        filtered_items = [item for item in filtered_items if item["severity"] == filters["severity"]]
    if filters.get("wire_level"):
        filtered_items = [item for item in filtered_items if item["wire_level"] == filters["wire_level"]]
    if filters.get("company_id"):
        filtered_items = [item for item in filtered_items if item["company_id"] == filters["company_id"]]
    if filters.get("source_id"):
        filtered_items = [item for item in filtered_items if filters["source_id"] in item["source_ids"]]
    if filters.get("signal_type"):
        filtered_items = [item for item in filtered_items if item["signal_type"] == filters["signal_type"]]
    if filters.get("location"):
        filtered_items = [item for item in filtered_items if filters["location"] in item["locations"]]
    if filters.get("marketplace"):
        filtered_items = [item for item in filtered_items if item.get("marketplace") == filters["marketplace"]]
    return filtered_items


def apply_market_signal_limit(items: list[dict], filters: dict) -> list[dict]:
    limit = filters.get("limit")
    if not isinstance(limit, int):
        return items
    return items[:limit]


def sort_market_signal_items(items: list[dict]) -> list[dict]:
    return sorted(
        items,
        key=lambda item: (SEVERITY_RANKS.get(item["severity"], 0), item["timestamp"]),
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
    unique_sources = {}
    for source in sources:
        unique_sources[source["source_id"]] = source
    return [unique_sources[source_id] for source_id in sorted(unique_sources)]


def deduplicate_market_signal_companies(companies: list[dict]) -> list[dict]:
    merged = {}
    for company in companies:
        record = merged.setdefault(
            company["company_id"],
            {
                "company_id": company["company_id"],
                "company_name": company["company_name"],
                "categories": set(),
            },
        )
        for category in company["categories"]:
            record["categories"].add(category)
    return [
        {
            "company_id": record["company_id"],
            "company_name": record["company_name"],
            "categories": sorted(record["categories"]),
        }
        for record in sorted(merged.values(), key=lambda item: item["company_name"])
    ]


def build_market_signals_meta(settings: Settings, dataset: dict, refresh: bool) -> dict:
    return {
        "api_version": "v1",
        "module": "market_signals",
        "view": "wire",
        "generated_at": to_iso_timestamp(datetime.now(timezone.utc)),
        "refresh_requested": refresh,
        "filters": dataset["filters"],
        "source_count": len(dataset["sources"]),
        "company_count": len(dataset["companies"]),
        "active_count": len(dataset["all_items"]),
        "latest_snapshot_at": dataset["latest_snapshot_at"],
        "latest_signal_at": dataset["all_items"][0]["timestamp"] if dataset["all_items"] else None,
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
    for item in items:
        by_category[item["category"]] += 1
        by_signal_type[item["signal_type"]] += 1
    return {
        "high_priority_count": len([item for item in items if item["wire_level"] == "high"]),
        "elevated_count": len([item for item in items if item["wire_level"] == "elevated"]),
        "watch_count": len([item for item in items if item["wire_level"] == "watch"]),
        "active_count": len(items),
        "by_category": [
            {"category": category, "count": count}
            for category, count in sorted(by_category.items())
        ],
        "by_signal_type": [
            {"signal_type": signal_type, "count": count}
            for signal_type, count in sorted(by_signal_type.items())
        ],
    }


def build_wire_stats(items: list[dict]) -> list[dict]:
    return [
        {"id": "high_priority", "label": "High+", "value": len([item for item in items if item["wire_level"] == "high"])},
        {"id": "elevated", "label": "Elevated", "value": len([item for item in items if item["wire_level"] == "elevated"])},
        {"id": "watch", "label": "Watch", "value": len([item for item in items if item["wire_level"] == "watch"])},
        {"id": "active", "label": "Active", "value": len(items)},
    ]


def build_market_signal_facets(dataset: dict) -> dict:
    signal_types = sorted({item["signal_type"] for item in dataset["all_items"]})
    locations = sorted({location for item in dataset["all_items"] for location in item["locations"]})
    marketplaces = sorted({item["marketplace"] for item in dataset["all_items"] if item.get("marketplace")})
    return {
        "categories": sorted({status["category"] for status in dataset["category_status"]}),
        "severities": sorted({item["severity"] for item in dataset["all_items"]}, key=lambda item: SEVERITY_RANKS[item], reverse=True),
        "wire_levels": ["high", "elevated", "watch"],
        "signal_types": signal_types,
        "locations": locations,
        "marketplaces": marketplaces,
    }


def build_growth_signal_insight_map(strategic_insights: list[dict]) -> dict[str, dict]:
    insight_map = {}
    for insight in strategic_insights:
        for signal_id in insight["signal_ids"]:
            current = insight_map.get(signal_id)
            if not current or insight["impact_score"] > current["impact_score"]:
                insight_map[signal_id] = insight
    return insight_map
