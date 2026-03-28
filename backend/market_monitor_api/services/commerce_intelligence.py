from collections import defaultdict
from datetime import datetime, timezone

from market_monitor_api.config import Settings
from market_monitor_api.services.openai_service import build_commerce_insight_map
from market_monitor_api.services.tinyfish import (
    build_source_health,
    load_snapshots,
    load_source_catalog,
    parse_iso_datetime,
    run_source_refreshes,
    to_iso_timestamp,
)


COMMERCE_REQUIRED_SOURCE_FIELDS = [
    "sku",
    "marketplace",
    "tracking_group_id",
    "schedule",
]

COMMERCE_REQUIRED_RESULT_FIELDS = [
    "price",
    "discount_percent",
    "stock_status",
    "seller",
]

STOCK_STATUS_SCORES = {
    "out_of_stock": 0,
    "limited_stock": 1,
    "in_stock": 2,
    "preorder": 1,
    "unknown": -1,
}


class CommerceConfigError(Exception):
    def __init__(self, code: str, message: str, status_code: int = 503):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


def build_commerce_response(settings: Settings, refresh: bool = False, filters: dict | None = None) -> dict:
    dataset = collect_commerce_dataset(settings, refresh=refresh, filters=filters or {})
    return {
        "meta": build_commerce_meta(settings, dataset, refresh),
        "kpis": build_commerce_kpis(dataset["signals"], dataset["current_listings"], dataset["snapshots"]),
        "signals": dataset["signals"],
        "current_listings": dataset["current_listings"],
        "competitor_matrix": dataset["competitor_matrix"],
        "time_series": dataset["time_series"],
        "images": dataset["images"],
        "source_health": dataset["source_health"],
        "sources": dataset["sources"],
    }


def build_commerce_signals_response(
    settings: Settings,
    refresh: bool = False,
    filters: dict | None = None,
) -> dict:
    dataset = collect_commerce_dataset(settings, refresh=refresh, filters=filters or {})
    return {
        "meta": build_commerce_meta(settings, dataset, refresh),
        "signals": dataset["signals"],
        "images": dataset["images"],
    }


def build_commerce_history_response(settings: Settings, filters: dict | None = None) -> dict:
    dataset = collect_commerce_dataset(settings, refresh=False, filters=filters or {})
    return {
        "meta": build_commerce_meta(settings, dataset, False),
        "snapshots": dataset["snapshots"],
        "comparisons": dataset["comparisons"],
        "time_series": dataset["time_series"],
        "images": dataset["images"],
    }


def collect_commerce_dataset(settings: Settings, refresh: bool, filters: dict) -> dict:
    try:
        all_sources = load_source_catalog(settings)
    except FileNotFoundError as exc:
        raise CommerceConfigError("source_config_missing", str(exc))
    except ValueError as exc:
        raise CommerceConfigError("source_config_invalid", str(exc), status_code=500)
    sources = build_commerce_sources(all_sources)
    validate_commerce_sources(sources)
    if refresh and not settings.tinyfish_configured:
        raise CommerceConfigError(
            "tinyfish_not_configured",
            "TinyFish is required for refresh=true. Set TINYFISH_API_KEY in the .env file.",
        )
    if refresh and sources:
        run_source_refreshes(settings, sources)
    all_snapshots = load_snapshots(settings)
    snapshots = build_commerce_snapshots(sources, all_snapshots)
    filtered_sources, filtered_snapshots = apply_commerce_filters(sources, snapshots, filters)
    current_listings = build_current_listings(filtered_snapshots)
    comparisons = build_commerce_comparisons(filtered_snapshots)
    raw_signals = build_commerce_signals(filtered_sources, filtered_snapshots, comparisons, current_listings)
    insight_map = build_commerce_insight_map(raw_signals, filtered_snapshots, settings)
    signals = merge_commerce_insights(raw_signals, insight_map, settings)
    time_series = build_commerce_time_series(filtered_snapshots)
    images = build_commerce_images(current_listings)
    source_health = build_source_health(settings, filtered_sources, all_snapshots)
    competitor_matrix = build_competitor_matrix(current_listings)
    return {
        "sources": filtered_sources,
        "snapshots": filtered_snapshots,
        "current_listings": current_listings,
        "comparisons": comparisons,
        "signals": signals,
        "time_series": time_series,
        "images": images,
        "source_health": source_health,
        "competitor_matrix": competitor_matrix,
        "filters": filters,
    }


def build_commerce_sources(all_sources: list[dict]) -> list[dict]:
    return [source for source in all_sources if source["category"] == "commerce_intelligence"]


def validate_commerce_sources(sources: list[dict]) -> None:
    for source in sources:
        missing_fields = [field for field in COMMERCE_REQUIRED_SOURCE_FIELDS if field not in source]
        if missing_fields:
            raise CommerceConfigError(
                "commerce_source_invalid",
                f"Commerce source {source['id']} is missing fields: {', '.join(missing_fields)}",
                status_code=500,
            )
        schedule = source["schedule"]
        if not isinstance(schedule, dict) or not isinstance(schedule.get("interval_minutes"), int):
            raise CommerceConfigError(
                "commerce_source_invalid",
                f"Commerce source {source['id']} must define schedule.interval_minutes as an integer.",
                status_code=500,
            )


def build_commerce_snapshots(sources: list[dict], all_snapshots: list[dict]) -> list[dict]:
    source_map = {source["id"]: source for source in sources}
    snapshots = []
    for snapshot in all_snapshots:
        source = source_map.get(snapshot["source_id"])
        if not source:
            continue
        normalized = normalize_commerce_snapshot(snapshot, source)
        if normalized:
            snapshots.append(normalized)
    return sorted(snapshots, key=lambda item: parse_iso_datetime(item["captured_at"]))


def normalize_commerce_snapshot(snapshot: dict, source: dict) -> dict | None:
    if snapshot["capture_status"] != "COMPLETED":
        return None
    result = snapshot.get("result")
    if not isinstance(result, dict):
        return None
    numeric_price = coerce_number(result.get("price"))
    discount_percent = coerce_number(result.get("discount_percent"))
    stock_status = normalize_stock_status(result.get("stock_status"))
    seller = result.get("seller")
    if numeric_price is None or discount_percent is None or stock_status is None or not isinstance(seller, str):
        return None
    image_url = result.get("image_url") or result.get("primary_image_url")
    gallery_image_urls = collect_gallery_images(result)
    return {
        "snapshot_id": snapshot["snapshot_id"],
        "captured_at": result.get("captured_at") or snapshot["captured_at"],
        "source_id": source["id"],
        "source_name": source["name"],
        "company_id": source["company_id"],
        "company_name": source["company_name"],
        "competitor_id": source.get("competitor_id", source["company_id"]),
        "competitor_name": source.get("competitor_name", source["company_name"]),
        "tracking_group_id": source["tracking_group_id"],
        "marketplace": source["marketplace"],
        "sku": result.get("sku") or source["sku"],
        "product_id": source.get("product_id"),
        "product_name": result.get("product_name") or source.get("product_name") or source["name"],
        "schedule": source["schedule"],
        "price": numeric_price,
        "currency": result.get("currency") or source.get("currency"),
        "discount_percent": discount_percent,
        "stock_status": stock_status,
        "seller": seller.strip(),
        "target_url": snapshot["target_url"],
        "product_url": result.get("product_url") or snapshot["target_url"],
        "image_url": image_url,
        "gallery_image_urls": gallery_image_urls,
        "raw_result": result,
    }


def coerce_number(value):
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return None


def normalize_stock_status(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower().replace(" ", "_")
    if normalized in STOCK_STATUS_SCORES:
        return normalized
    return "unknown"


def collect_gallery_images(result: dict) -> list[str]:
    gallery = []
    raw_gallery = result.get("gallery_image_urls") or result.get("image_urls") or []
    if isinstance(raw_gallery, list):
        for item in raw_gallery:
            if isinstance(item, str) and item not in gallery:
                gallery.append(item)
    image_url = result.get("image_url") or result.get("primary_image_url")
    if isinstance(image_url, str) and image_url not in gallery:
        gallery.insert(0, image_url)
    return gallery


def apply_commerce_filters(
    sources: list[dict],
    snapshots: list[dict],
    filters: dict,
) -> tuple[list[dict], list[dict]]:
    if not filters:
        return sources, snapshots
    filtered_sources = [
        source
        for source in sources
        if source_matches_filters(source, filters)
    ]
    source_ids = {source["id"] for source in filtered_sources}
    filtered_snapshots = [
        snapshot
        for snapshot in snapshots
        if snapshot["source_id"] in source_ids and snapshot_matches_filters(snapshot, filters)
    ]
    return filtered_sources, filtered_snapshots


def source_matches_filters(source: dict, filters: dict) -> bool:
    for key in ("source_id", "sku", "marketplace", "tracking_group_id", "company_id", "competitor_id"):
        filter_value = filters.get(key)
        if not filter_value:
            continue
        source_value = source.get(key)
        if source_value != filter_value:
            return False
    return True


def snapshot_matches_filters(snapshot: dict, filters: dict) -> bool:
    for key in ("source_id", "sku", "marketplace", "tracking_group_id", "company_id", "competitor_id"):
        filter_value = filters.get(key)
        if not filter_value:
            continue
        if snapshot.get(key) != filter_value:
            return False
    return True


def build_current_listings(snapshots: list[dict]) -> list[dict]:
    latest_by_source = {}
    for snapshot in snapshots:
        latest_by_source[snapshot["source_id"]] = snapshot
    current_listings = []
    for snapshot in latest_by_source.values():
        current_listings.append(
            {
                "source_id": snapshot["source_id"],
                "source_name": snapshot["source_name"],
                "company_id": snapshot["company_id"],
                "company_name": snapshot["company_name"],
                "competitor_id": snapshot["competitor_id"],
                "competitor_name": snapshot["competitor_name"],
                "tracking_group_id": snapshot["tracking_group_id"],
                "marketplace": snapshot["marketplace"],
                "sku": snapshot["sku"],
                "product_name": snapshot["product_name"],
                "price": snapshot["price"],
                "currency": snapshot["currency"],
                "discount_percent": snapshot["discount_percent"],
                "stock_status": snapshot["stock_status"],
                "seller": snapshot["seller"],
                "timestamp": snapshot["captured_at"],
                "image_url": snapshot["image_url"],
                "gallery_image_urls": snapshot["gallery_image_urls"],
                "product_url": snapshot["product_url"],
                "schedule": snapshot["schedule"],
            }
        )
    return sorted(
        current_listings,
        key=lambda item: (item["tracking_group_id"], item["marketplace"], item["price"]),
    )


def build_commerce_comparisons(snapshots: list[dict]) -> list[dict]:
    snapshots_by_source: dict[str, list[dict]] = defaultdict(list)
    for snapshot in snapshots:
        snapshots_by_source[snapshot["source_id"]].append(snapshot)
    comparisons = []
    for source_id, records in snapshots_by_source.items():
        ordered = sorted(records, key=lambda item: parse_iso_datetime(item["captured_at"]))
        for previous, current in zip(ordered, ordered[1:]):
            comparisons.append(build_comparison_record(previous, current))
    return sorted(comparisons, key=lambda item: item["timestamp"], reverse=True)


def build_comparison_record(previous: dict, current: dict) -> dict:
    price_delta = round(current["price"] - previous["price"], 4)
    price_delta_percent = round((price_delta / previous["price"]) * 100, 4) if previous["price"] else None
    discount_delta = round(current["discount_percent"] - previous["discount_percent"], 4)
    return {
        "comparison_id": f"{current['source_id']}::{current['snapshot_id']}",
        "source_id": current["source_id"],
        "source_name": current["source_name"],
        "company_id": current["company_id"],
        "company_name": current["company_name"],
        "competitor_id": current["competitor_id"],
        "competitor_name": current["competitor_name"],
        "tracking_group_id": current["tracking_group_id"],
        "marketplace": current["marketplace"],
        "sku": current["sku"],
        "product_name": current["product_name"],
        "current_snapshot_id": current["snapshot_id"],
        "previous_snapshot_id": previous["snapshot_id"],
        "timestamp": current["captured_at"],
        "current": current,
        "previous": previous,
        "delta": {
            "price": price_delta,
            "price_percent": price_delta_percent,
            "discount_percent": discount_delta,
            "stock_status_changed": current["stock_status"] != previous["stock_status"],
            "seller_changed": current["seller"] != previous["seller"],
        },
    }


def build_commerce_signals(
    sources: list[dict],
    snapshots: list[dict],
    comparisons: list[dict],
    current_listings: list[dict],
) -> list[dict]:
    signals = []
    source_map = {source["id"]: source for source in sources}
    for comparison in comparisons:
        source = source_map[comparison["source_id"]]
        signals.extend(build_price_change_signals(comparison, source))
        signals.extend(build_flash_sale_signals(comparison, source))
        signals.extend(build_inventory_shift_signals(comparison))
    signals.extend(build_undercutting_signals(current_listings, source_map))
    return sorted(
        signals,
        key=lambda item: (build_severity_rank(item["severity"]), item["timestamp"]),
        reverse=True,
    )


def build_price_change_signals(comparison: dict, source: dict) -> list[dict]:
    delta_percent = comparison["delta"]["price_percent"]
    if delta_percent is None or delta_percent == 0:
        return []
    signal_type = "price_drop" if delta_percent < 0 else "price_increase"
    return [
        {
            "id": f"{comparison['comparison_id']}::price",
            "signal_type": signal_type,
            "category": "commerce_intelligence",
            "severity": classify_price_change_severity(abs(delta_percent)),
            "timestamp": comparison["timestamp"],
            "source_id": comparison["source_id"],
            "source_name": comparison["source_name"],
            "company_id": comparison["company_id"],
            "company_name": comparison["company_name"],
            "competitor_id": comparison["competitor_id"],
            "competitor_name": comparison["competitor_name"],
            "tracking_group_id": comparison["tracking_group_id"],
            "marketplace": comparison["marketplace"],
            "sku": comparison["sku"],
            "product_name": comparison["product_name"],
            "seller": comparison["current"]["seller"],
            "image_url": comparison["current"]["image_url"],
            "gallery_image_urls": comparison["current"]["gallery_image_urls"],
            "product_url": comparison["current"]["product_url"],
            "current_value": {
                "price": comparison["current"]["price"],
                "currency": comparison["current"]["currency"],
            },
            "previous_value": {
                "price": comparison["previous"]["price"],
                "currency": comparison["previous"]["currency"],
            },
            "delta": comparison["delta"],
        }
    ]


def build_flash_sale_signals(comparison: dict, source: dict) -> list[dict]:
    thresholds = source.get("thresholds", {})
    discount_jump = comparison["delta"]["discount_percent"]
    current_discount = comparison["current"]["discount_percent"]
    flash_sale_delta = thresholds.get("flash_sale_discount_jump", 10)
    flash_sale_floor = thresholds.get("flash_sale_discount_floor", 20)
    if discount_jump < flash_sale_delta and current_discount < flash_sale_floor:
        return []
    return [
        {
            "id": f"{comparison['comparison_id']}::flash_sale",
            "signal_type": "flash_sale",
            "category": "commerce_intelligence",
            "severity": classify_flash_sale_severity(discount_jump, current_discount),
            "timestamp": comparison["timestamp"],
            "source_id": comparison["source_id"],
            "source_name": comparison["source_name"],
            "company_id": comparison["company_id"],
            "company_name": comparison["company_name"],
            "competitor_id": comparison["competitor_id"],
            "competitor_name": comparison["competitor_name"],
            "tracking_group_id": comparison["tracking_group_id"],
            "marketplace": comparison["marketplace"],
            "sku": comparison["sku"],
            "product_name": comparison["product_name"],
            "seller": comparison["current"]["seller"],
            "image_url": comparison["current"]["image_url"],
            "gallery_image_urls": comparison["current"]["gallery_image_urls"],
            "product_url": comparison["current"]["product_url"],
            "current_value": {
                "price": comparison["current"]["price"],
                "discount_percent": comparison["current"]["discount_percent"],
            },
            "previous_value": {
                "price": comparison["previous"]["price"],
                "discount_percent": comparison["previous"]["discount_percent"],
            },
            "delta": comparison["delta"],
        }
    ]


def build_inventory_shift_signals(comparison: dict) -> list[dict]:
    if not comparison["delta"]["stock_status_changed"]:
        return []
    current_status = comparison["current"]["stock_status"]
    signal_type = "inventory_shift"
    if current_status == "out_of_stock":
        severity = "high"
    elif current_status == "in_stock":
        severity = "medium"
    else:
        severity = "low"
    return [
        {
            "id": f"{comparison['comparison_id']}::inventory",
            "signal_type": signal_type,
            "category": "commerce_intelligence",
            "severity": severity,
            "timestamp": comparison["timestamp"],
            "source_id": comparison["source_id"],
            "source_name": comparison["source_name"],
            "company_id": comparison["company_id"],
            "company_name": comparison["company_name"],
            "competitor_id": comparison["competitor_id"],
            "competitor_name": comparison["competitor_name"],
            "tracking_group_id": comparison["tracking_group_id"],
            "marketplace": comparison["marketplace"],
            "sku": comparison["sku"],
            "product_name": comparison["product_name"],
            "seller": comparison["current"]["seller"],
            "image_url": comparison["current"]["image_url"],
            "gallery_image_urls": comparison["current"]["gallery_image_urls"],
            "product_url": comparison["current"]["product_url"],
            "current_value": {"stock_status": comparison["current"]["stock_status"]},
            "previous_value": {"stock_status": comparison["previous"]["stock_status"]},
            "delta": comparison["delta"],
        }
    ]


def build_undercutting_signals(current_listings: list[dict], source_map: dict[str, dict]) -> list[dict]:
    listings_by_group: dict[str, list[dict]] = defaultdict(list)
    for listing in current_listings:
        listings_by_group[listing["tracking_group_id"]].append(listing)
    signals = []
    for tracking_group_id, listings in listings_by_group.items():
        priced_listings = [listing for listing in listings if listing["price"] is not None]
        if len(priced_listings) < 2:
            continue
        ordered = sorted(priced_listings, key=lambda item: item["price"])
        cheapest = ordered[0]
        runner_up = ordered[1]
        price_gap = runner_up["price"] - cheapest["price"]
        price_gap_percent = round((price_gap / runner_up["price"]) * 100, 4) if runner_up["price"] else 0
        thresholds = source_map[cheapest["source_id"]].get("thresholds", {})
        undercut_floor = thresholds.get("undercut_percent_floor", 2)
        if price_gap_percent < undercut_floor:
            continue
        signals.append(
            {
                "id": f"{tracking_group_id}::{cheapest['source_id']}::undercut",
                "signal_type": "undercutting",
                "category": "commerce_intelligence",
                "severity": classify_undercut_severity(price_gap_percent),
                "timestamp": cheapest["timestamp"],
                "source_id": cheapest["source_id"],
                "source_name": cheapest["source_name"],
                "company_id": cheapest["company_id"],
                "company_name": cheapest["company_name"],
                "competitor_id": cheapest["competitor_id"],
                "competitor_name": cheapest["competitor_name"],
                "tracking_group_id": tracking_group_id,
                "marketplace": cheapest["marketplace"],
                "sku": cheapest["sku"],
                "product_name": cheapest["product_name"],
                "seller": cheapest["seller"],
                "image_url": cheapest["image_url"],
                "gallery_image_urls": cheapest["gallery_image_urls"],
                "product_url": cheapest["product_url"],
                "current_value": {
                    "price": cheapest["price"],
                    "currency": cheapest["currency"],
                    "competitor_reference_price": runner_up["price"],
                    "competitor_reference_source_id": runner_up["source_id"],
                    "competitor_reference_marketplace": runner_up["marketplace"],
                },
                "previous_value": None,
                "delta": {
                    "price": round(-price_gap, 4),
                    "price_percent": round(-price_gap_percent, 4),
                },
            }
        )
    return signals


def classify_price_change_severity(delta_percent: float) -> str:
    if delta_percent >= 15:
        return "critical"
    if delta_percent >= 8:
        return "high"
    if delta_percent >= 3:
        return "medium"
    return "low"


def classify_flash_sale_severity(discount_jump: float, current_discount: float) -> str:
    if discount_jump >= 20 or current_discount >= 40:
        return "critical"
    if discount_jump >= 10 or current_discount >= 25:
        return "high"
    return "medium"


def classify_undercut_severity(price_gap_percent: float) -> str:
    if price_gap_percent >= 10:
        return "critical"
    if price_gap_percent >= 5:
        return "high"
    return "medium"


def build_severity_rank(severity: str) -> int:
    severity_map = {
        "critical": 4,
        "high": 3,
        "medium": 2,
        "low": 1,
    }
    return severity_map.get(severity, 0)


def merge_commerce_insights(signals: list[dict], insight_map: dict[str, dict], settings: Settings) -> list[dict]:
    enriched = []
    for signal in signals:
        insight = insight_map.get(signal["id"])
        enriched_signal = dict(signal)
        enriched_signal["insight"] = {
            "provider": "OpenAI",
            "model": settings.openai_model or None,
            "status": build_insight_status(insight, settings),
            "pattern": insight["pattern"] if insight else None,
            "summary": insight["summary"] if insight else None,
            "confidence_score": insight["confidence_score"] if insight else None,
            "impact_score": insight["impact_score"] if insight else None,
        }
        if insight:
            enriched_signal["confidence_score"] = insight["confidence_score"]
            enriched_signal["impact_score"] = insight["impact_score"]
        else:
            enriched_signal["confidence_score"] = None
            enriched_signal["impact_score"] = None
        enriched.append(enriched_signal)
    return enriched


def build_insight_status(insight: dict | None, settings: Settings) -> str:
    if insight:
        return "completed"
    if settings.openai_configured:
        return "unavailable"
    return "not_configured"


def build_commerce_time_series(snapshots: list[dict]) -> dict:
    by_source = []
    snapshots_by_source: dict[str, list[dict]] = defaultdict(list)
    for snapshot in snapshots:
        snapshots_by_source[snapshot["source_id"]].append(snapshot)
    for source_id, records in sorted(snapshots_by_source.items()):
        ordered = sorted(records, key=lambda item: parse_iso_datetime(item["captured_at"]))
        latest = ordered[-1]
        by_source.append(
            {
                "source_id": source_id,
                "source_name": latest["source_name"],
                "tracking_group_id": latest["tracking_group_id"],
                "marketplace": latest["marketplace"],
                "sku": latest["sku"],
                "product_name": latest["product_name"],
                "price": [
                    {"timestamp": item["captured_at"], "value": item["price"]}
                    for item in ordered
                ],
                "discount_percent": [
                    {"timestamp": item["captured_at"], "value": item["discount_percent"]}
                    for item in ordered
                ],
                "stock_status": [
                    {
                        "timestamp": item["captured_at"],
                        "value": STOCK_STATUS_SCORES[item["stock_status"]],
                        "label": item["stock_status"],
                    }
                    for item in ordered
                ],
            }
        )
    return {
        "by_source": by_source,
        "by_tracking_group": build_tracking_group_series(by_source),
    }


def build_tracking_group_series(by_source: list[dict]) -> list[dict]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for series in by_source:
        grouped[series["tracking_group_id"]].append(series)
    records = []
    for tracking_group_id, entries in sorted(grouped.items()):
        latest_prices = [
            {
                "source_id": entry["source_id"],
                "marketplace": entry["marketplace"],
                "value": entry["price"][-1]["value"] if entry["price"] else None,
            }
            for entry in entries
        ]
        valid_prices = [item["value"] for item in latest_prices if item["value"] is not None]
        records.append(
            {
                "tracking_group_id": tracking_group_id,
                "sku": entries[0]["sku"],
                "product_name": entries[0]["product_name"],
                "latest_prices": latest_prices,
                "price_spread": round(max(valid_prices) - min(valid_prices), 4) if len(valid_prices) >= 2 else None,
            }
        )
    return records


def build_commerce_images(current_listings: list[dict]) -> list[dict]:
    images = []
    for listing in current_listings:
        if not listing["image_url"] and not listing["gallery_image_urls"]:
            continue
        images.append(
            {
                "source_id": listing["source_id"],
                "tracking_group_id": listing["tracking_group_id"],
                "sku": listing["sku"],
                "product_name": listing["product_name"],
                "marketplace": listing["marketplace"],
                "seller": listing["seller"],
                "timestamp": listing["timestamp"],
                "image_url": listing["image_url"],
                "gallery_image_urls": listing["gallery_image_urls"],
            }
        )
    return images


def build_competitor_matrix(current_listings: list[dict]) -> list[dict]:
    groups: dict[str, list[dict]] = defaultdict(list)
    for listing in current_listings:
        groups[listing["tracking_group_id"]].append(listing)
    matrix = []
    for tracking_group_id, entries in sorted(groups.items()):
        ordered = sorted(entries, key=lambda item: item["price"])
        matrix.append(
            {
                "tracking_group_id": tracking_group_id,
                "sku": ordered[0]["sku"],
                "product_name": ordered[0]["product_name"],
                "marketplaces": [
                    {
                        "source_id": entry["source_id"],
                        "marketplace": entry["marketplace"],
                        "seller": entry["seller"],
                        "price": entry["price"],
                        "currency": entry["currency"],
                        "discount_percent": entry["discount_percent"],
                        "stock_status": entry["stock_status"],
                        "timestamp": entry["timestamp"],
                        "image_url": entry["image_url"],
                    }
                    for entry in ordered
                ],
            }
        )
    return matrix


def build_commerce_meta(settings: Settings, dataset: dict, refresh: bool) -> dict:
    latest_snapshot_at = dataset["snapshots"][-1]["captured_at"] if dataset["snapshots"] else None
    return {
        "api_version": "v1",
        "module": "commerce_intelligence",
        "platform": settings.app_name,
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
                "model": settings.openai_model or None,
                "base_url": settings.openai_base_url,
            },
        },
    }


def build_commerce_kpis(signals: list[dict], current_listings: list[dict], snapshots: list[dict]) -> list[dict]:
    tracked_skus = len({listing["tracking_group_id"] for listing in current_listings})
    flash_sales = len([signal for signal in signals if signal["signal_type"] == "flash_sale"])
    undercutting = len([signal for signal in signals if signal["signal_type"] == "undercutting"])
    out_of_stock = len([listing for listing in current_listings if listing["stock_status"] == "out_of_stock"])
    return [
        {"id": "tracked_skus", "label": "Tracked SKUs", "value": tracked_skus},
        {"id": "active_signals", "label": "Active Signals", "value": len(signals)},
        {"id": "flash_sales", "label": "Flash Sales", "value": flash_sales},
        {"id": "undercutting", "label": "Undercutting", "value": undercutting},
        {"id": "out_of_stock", "label": "Out of Stock", "value": out_of_stock},
        {"id": "snapshots_stored", "label": "Snapshots Stored", "value": len(snapshots)},
    ]
