import socket
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import json

from market_monitor_api.config import Settings


def build_event_analysis_map(comparisons: list[dict], settings: Settings) -> dict[str, dict]:
    if not settings.openai_configured or not comparisons:
        return {}
    payload = build_openai_request_payload(comparisons, settings)
    request = Request(
        f"{settings.openai_base_url.rstrip('/')}/responses",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {settings.openai_api_key}",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=settings.openai_timeout_seconds) as response:
            response_payload = json.loads(response.read().decode("utf-8"))
    except HTTPError:
        return {}
    except (TimeoutError, socket.timeout):
        return {}
    except URLError:
        return {}
    output_text = extract_openai_output_text(response_payload)
    if not output_text:
        return {}
    analysis_payload = json.loads(output_text)
    analyses = analysis_payload.get("analyses", [])
    return {
        analysis["comparison_id"]: analysis
        for analysis in analyses
        if isinstance(analysis, dict) and analysis.get("comparison_id")
    }


def build_openai_request_payload(comparisons: list[dict], settings: Settings) -> dict:
    return {
        "model": settings.openai_model,
        "input": [
            {
                "role": "developer",
                "content": [
                    {
                        "type": "input_text",
                        "text": (
                            "You classify market-monitor snapshot comparisons. "
                            "Return only JSON that matches the provided schema. "
                            "Use only the supplied evidence."
                        ),
                    }
                ],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": json.dumps(
                            {
                                "comparisons": [
                                    {
                                        "comparison_id": comparison["comparison_id"],
                                        "category": comparison["category"],
                                        "source_name": comparison["source_name"],
                                        "company_name": comparison["company_name"],
                                        "product_name": comparison.get("product_name"),
                                        "captured_at": comparison["captured_at"],
                                        "target_url": comparison["target_url"],
                                        "changes": comparison["changes"],
                                        "current": comparison["current"],
                                        "previous": comparison["previous"],
                                    }
                                    for comparison in comparisons
                                ]
                            },
                            separators=(",", ":"),
                            ensure_ascii=True,
                        ),
                    }
                ],
            },
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": "market_monitor_event_analysis",
                "schema": build_openai_response_schema(),
            }
        },
    }


def build_openai_response_schema() -> dict:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["analyses"],
        "properties": {
            "analyses": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "comparison_id",
                        "signal_type",
                        "severity",
                        "headline",
                        "narrative",
                        "anomaly_classification",
                        "confidence_score",
                        "impact_score",
                    ],
                    "properties": {
                        "comparison_id": {"type": "string"},
                        "signal_type": {"type": "string"},
                        "severity": {"type": "string"},
                        "headline": {"type": "string"},
                        "narrative": {"type": "string"},
                        "anomaly_classification": {"type": "string"},
                        "confidence_score": {"type": "number"},
                        "impact_score": {"type": "number"},
                    },
                },
            }
        },
    }


def extract_openai_output_text(response_payload: dict) -> str | None:
    output_items = response_payload.get("output", [])
    for item in output_items:
        for content in item.get("content", []):
            if content.get("type") == "output_text":
                return content.get("text")
    return None


def build_event_entities(comparisons: list[dict], analysis_map: dict[str, dict], settings: Settings) -> list[dict]:
    events = [
        build_event_entity(comparison, analysis_map.get(comparison["comparison_id"]), settings)
        for comparison in comparisons
    ]
    return sorted(
        events,
        key=lambda event: (build_severity_rank(event.get("severity")), event["timestamp"]),
        reverse=True,
    )


def build_event_entity(comparison: dict, analysis: dict | None, settings: Settings) -> dict:
    return {
        "id": f"event-{comparison['comparison_id']}",
        "type": analysis["signal_type"] if analysis else "snapshot_change",
        "signal_type": analysis["signal_type"] if analysis else None,
        "category": comparison["category"],
        "severity": analysis["severity"] if analysis else None,
        "company_id": comparison["company_id"],
        "company_name": comparison["company_name"],
        "product_id": comparison.get("product_id"),
        "product_name": comparison.get("product_name"),
        "source_ids": [comparison["source_id"]],
        "source_name": comparison["source_name"],
        "snapshot_ids": {
            "current": comparison["snapshot_id"],
            "previous": comparison["previous_snapshot_id"],
        },
        "headline": analysis["headline"] if analysis else None,
        "narrative": analysis["narrative"] if analysis else None,
        "confidence_score": analysis["confidence_score"] if analysis else None,
        "impact_score": analysis["impact_score"] if analysis else None,
        "timestamp": comparison["captured_at"],
        "changes": comparison["changes"],
        "provenance": {
            "source_ids": [comparison["source_id"]],
            "snapshot_ids": [comparison["snapshot_id"], comparison["previous_snapshot_id"]],
            "extraction_timestamps": [comparison["captured_at"]],
            "evidence_urls": extract_comparison_evidence_urls(comparison),
            "target_urls": [comparison["target_url"]],
        },
        "analysis": {
            "provider": "OpenAI",
            "model": settings.openai_model or None,
            "configured": settings.openai_configured,
            "status": build_analysis_status(analysis, settings),
            "anomaly_classification": analysis["anomaly_classification"] if analysis else None,
        },
    }


def build_analysis_status(analysis: dict | None, settings: Settings) -> str:
    if analysis:
        return "completed"
    if settings.openai_configured:
        return "unavailable"
    return "not_configured"


def build_alert_entities(events: list[dict]) -> list[dict]:
    return [
        {
            "id": f"alert-{event['id']}",
            "event_id": event["id"],
            "severity": event["severity"],
            "category": event["category"],
            "title": event["headline"],
            "narrative": event["narrative"],
            "confidence_score": event["confidence_score"],
            "impact_score": event["impact_score"],
            "status": "open",
            "timestamp": event["timestamp"],
            "provenance": event.get("provenance"),
        }
        for event in events
        if event.get("severity") in {"critical", "high"}
    ]


def extract_comparison_evidence_urls(comparison: dict) -> list[str]:
    urls = set()
    for payload in (comparison.get("current"), comparison.get("previous")):
        urls.update(extract_nested_urls(payload))
    return sorted(urls)


def extract_nested_urls(value) -> set[str]:
    urls = set()
    if isinstance(value, dict):
        for key, child in value.items():
            if isinstance(child, str) and key.endswith("_url") and child.strip():
                urls.add(child.strip())
            else:
                urls.update(extract_nested_urls(child))
    if isinstance(value, list):
        for child in value:
            urls.update(extract_nested_urls(child))
    return urls


def build_severity_rank(severity: str | None) -> int:
    severity_map = {
        "critical": 4,
        "high": 3,
        "medium": 2,
        "low": 1,
    }
    return severity_map.get(severity or "", 0)


def build_commerce_insight_map(signals: list[dict], snapshots: list[dict], settings: Settings) -> dict[str, dict]:
    if not settings.openai_configured or not signals:
        return {}
    payload = build_commerce_request_payload(signals, snapshots, settings)
    request = Request(
        f"{settings.openai_base_url.rstrip('/')}/responses",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {settings.openai_api_key}",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=settings.openai_timeout_seconds) as response:
            response_payload = json.loads(response.read().decode("utf-8"))
    except HTTPError:
        return {}
    except (TimeoutError, socket.timeout):
        return {}
    except URLError:
        return {}
    output_text = extract_openai_output_text(response_payload)
    if not output_text:
        return {}
    analysis_payload = json.loads(output_text)
    insights = analysis_payload.get("insights", [])
    return {
        insight["signal_id"]: insight
        for insight in insights
        if isinstance(insight, dict) and insight.get("signal_id")
    }


def build_commerce_request_payload(signals: list[dict], snapshots: list[dict], settings: Settings) -> dict:
    signal_history = build_commerce_history_context(snapshots)
    return {
        "model": settings.openai_model,
        "input": [
            {
                "role": "developer",
                "content": [
                    {
                        "type": "input_text",
                        "text": (
                            "You analyze commerce monitoring signals. "
                            "Classify patterns, generate concise actionable summaries, "
                            "and return only JSON that matches the provided schema."
                        ),
                    }
                ],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": json.dumps(
                            {
                                "signals": [
                                    {
                                        "signal_id": signal["id"],
                                        "signal_type": signal["signal_type"],
                                        "severity": signal["severity"],
                                        "tracking_group_id": signal["tracking_group_id"],
                                        "sku": signal["sku"],
                                        "marketplace": signal["marketplace"],
                                        "competitor_name": signal["competitor_name"],
                                        "product_name": signal["product_name"],
                                        "timestamp": signal["timestamp"],
                                        "current_value": signal["current_value"],
                                        "previous_value": signal["previous_value"],
                                        "delta": signal["delta"],
                                        "history": signal_history.get(signal["source_id"], []),
                                    }
                                    for signal in signals
                                ]
                            },
                            separators=(",", ":"),
                            ensure_ascii=True,
                        ),
                    }
                ],
            },
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": "commerce_intelligence_insights",
                "schema": build_commerce_response_schema(),
            }
        },
    }


def build_commerce_history_context(snapshots: list[dict]) -> dict[str, list[dict]]:
    history: dict[str, list[dict]] = {}
    snapshots_by_source: dict[str, list[dict]] = {}
    for snapshot in snapshots:
        snapshots_by_source.setdefault(snapshot["source_id"], []).append(snapshot)
    for source_id, records in snapshots_by_source.items():
        ordered = sorted(records, key=lambda item: item["captured_at"])[-6:]
        history[source_id] = [
            {
                "timestamp": record["captured_at"],
                "price": record["price"],
                "discount_percent": record["discount_percent"],
                "stock_status": record["stock_status"],
                "seller": record["seller"],
            }
            for record in ordered
        ]
    return history


def build_commerce_response_schema() -> dict:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["insights"],
        "properties": {
            "insights": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "signal_id",
                        "pattern",
                        "summary",
                        "confidence_score",
                        "impact_score",
                    ],
                    "properties": {
                        "signal_id": {"type": "string"},
                        "pattern": {"type": "string"},
                        "summary": {"type": "string"},
                        "confidence_score": {"type": "number"},
                        "impact_score": {"type": "number"},
                    },
                },
            }
        },
    }


def build_growth_insights(
    signal_clusters: list[dict],
    events: list[dict],
    snapshots: list[dict],
    settings: Settings,
) -> list[dict]:
    if not settings.openai_configured or not signal_clusters or not events:
        return []
    payload = build_growth_request_payload(signal_clusters, events, snapshots, settings)
    request = Request(
        f"{settings.openai_base_url.rstrip('/')}/responses",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {settings.openai_api_key}",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=settings.openai_timeout_seconds) as response:
            response_payload = json.loads(response.read().decode("utf-8"))
    except HTTPError:
        return []
    except (TimeoutError, socket.timeout):
        return []
    except URLError:
        return []
    output_text = extract_openai_output_text(response_payload)
    if not output_text:
        return []
    try:
        analysis_payload = json.loads(output_text)
    except json.JSONDecodeError:
        return []
    insights = analysis_payload.get("insights", [])
    cluster_map = {cluster["id"]: cluster for cluster in signal_clusters}
    event_map = {event["id"]: event for event in events}
    normalized_insights = []
    for insight in insights:
        if not isinstance(insight, dict):
            continue
        cluster = cluster_map.get(insight.get("cluster_id"))
        if not cluster:
            continue
        signal_ids = insight.get("signal_ids")
        strategic_direction = insight.get("strategic_direction")
        summary = insight.get("summary")
        confidence_score = insight.get("confidence_score")
        impact_score = insight.get("impact_score")
        if not isinstance(signal_ids, list) or not all(isinstance(signal_id, str) for signal_id in signal_ids):
            continue
        if not isinstance(strategic_direction, str) or not strategic_direction.strip():
            continue
        if not isinstance(summary, str) or not summary.strip():
            continue
        if not isinstance(confidence_score, (int, float)) or isinstance(confidence_score, bool):
            continue
        if not isinstance(impact_score, (int, float)) or isinstance(impact_score, bool):
            continue
        normalized_insights.append(
            {
                "id": insight["cluster_id"],
                "cluster_id": insight["cluster_id"],
                "company_id": cluster["company_id"],
                "company_name": cluster["company_name"],
                "signal_ids": signal_ids,
                "strategic_direction": strategic_direction.strip(),
                "summary": summary.strip(),
                "confidence_score": confidence_score,
                "impact_score": impact_score,
                "provenance": build_growth_insight_provenance(signal_ids, event_map),
            }
        )
    return normalized_insights


def build_growth_insight_provenance(signal_ids: list[str], event_map: dict[str, dict]) -> dict:
    source_ids = set()
    snapshot_ids = set()
    extraction_timestamps = set()
    evidence_urls = set()
    for signal_id in signal_ids:
        event = event_map.get(signal_id)
        if not event:
            continue
        provenance = event.get("provenance") or {}
        source_ids.update(provenance.get("source_ids", []))
        snapshot_ids.update(provenance.get("snapshot_ids", []))
        extraction_timestamps.update(provenance.get("extraction_timestamps", []))
        evidence_urls.update(provenance.get("evidence_urls", []))
    return {
        "source_ids": sorted(source_ids),
        "snapshot_ids": sorted(snapshot_ids),
        "extraction_timestamps": sorted(extraction_timestamps),
        "evidence_urls": sorted(evidence_urls),
    }


def build_growth_request_payload(
    signal_clusters: list[dict],
    events: list[dict],
    snapshots: list[dict],
    settings: Settings,
) -> dict:
    history = build_growth_history_context(snapshots)
    events_by_company: dict[str, list[dict]] = {}
    for event in events:
        events_by_company.setdefault(event["company_id"], []).append(event)
    return {
        "model": settings.openai_model,
        "input": [
            {
                "role": "developer",
                "content": [
                    {
                        "type": "input_text",
                        "text": (
                            "You analyze growth intelligence signals. "
                            "Cluster related change signals for each company, infer strategic direction, "
                            "and return only JSON matching the provided schema."
                        ),
                    }
                ],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": json.dumps(
                            {
                                "clusters": [
                                    {
                                        "cluster_id": cluster["id"],
                                        "company_id": cluster["company_id"],
                                        "company_name": cluster["company_name"],
                                        "signal_ids": cluster["signal_ids"],
                                        "signal_types": cluster["signal_types"],
                                        "cluster_names": cluster["cluster_names"],
                                        "locations": cluster["locations"],
                                        "latest_timestamp": cluster["latest_timestamp"],
                                        "events": [
                                            {
                                                "signal_id": event["id"],
                                                "signal_type": event["signal_type"],
                                                "severity": event["severity"],
                                                "title": event["title"],
                                                "summary": event["summary"],
                                                "timestamp": event["timestamp"],
                                                "delta": event["delta"],
                                                "delta_ratio": event["delta_ratio"],
                                                "locations": event["locations"],
                                            }
                                            for event in events_by_company[cluster["company_id"]]
                                        ],
                                        "history": history[cluster["company_id"]],
                                    }
                                    for cluster in signal_clusters
                                ]
                            },
                            separators=(",", ":"),
                            ensure_ascii=True,
                        ),
                    }
                ],
            },
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": "growth_intelligence_insights",
                "schema": build_growth_response_schema(),
            }
        },
    }


def build_growth_history_context(snapshots: list[dict]) -> dict[str, list[dict]]:
    history: dict[str, list[dict]] = {}
    snapshots_by_company: dict[str, list[dict]] = {}
    for snapshot in snapshots:
        snapshots_by_company.setdefault(snapshot["company_id"], []).append(snapshot)
    for company_id, records in snapshots_by_company.items():
        ordered = sorted(records, key=lambda item: item["captured_at"])[-6:]
        history[company_id] = [
            {
                "timestamp": record["captured_at"],
                "jobs_count": record["metrics"]["jobs_count"],
                "product_launch_count": record["metrics"]["product_launch_count"],
                "funding_mention_count": record["metrics"]["funding_mention_count"],
                "market_count": record["metrics"]["market_count"],
                "role_cluster_counts": record["role_cluster_counts"],
            }
            for record in ordered
        ]
    return history


def build_growth_response_schema() -> dict:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["insights"],
        "properties": {
            "insights": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "cluster_id",
                        "signal_ids",
                        "strategic_direction",
                        "summary",
                        "confidence_score",
                        "impact_score",
                    ],
                    "properties": {
                        "cluster_id": {"type": "string"},
                        "signal_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                        "strategic_direction": {"type": "string"},
                        "summary": {"type": "string"},
                        "confidence_score": {"type": "number"},
                        "impact_score": {"type": "number"},
                    },
                },
            }
        },
    }


def build_cross_category_correlations(
    signals: list[dict],
    company_profiles: list[dict],
    settings: Settings,
) -> list[dict]:
    if not settings.openai_configured:
        return []
    candidate_groups = build_cross_category_candidate_groups(signals, company_profiles)
    if not candidate_groups:
        return []
    payload = build_cross_category_request_payload(candidate_groups, settings)
    request = Request(
        f"{settings.openai_base_url.rstrip('/')}/responses",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {settings.openai_api_key}",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=settings.openai_timeout_seconds) as response:
            response_payload = json.loads(response.read().decode("utf-8"))
    except HTTPError:
        return []
    except (TimeoutError, socket.timeout):
        return []
    except URLError:
        return []
    output_text = extract_openai_output_text(response_payload)
    if not output_text:
        return []
    try:
        analysis_payload = json.loads(output_text)
    except json.JSONDecodeError:
        return []
    correlations = analysis_payload.get("correlations", [])
    group_map = {group["correlation_id"]: group for group in candidate_groups}
    normalized = []
    for correlation in correlations:
        if not isinstance(correlation, dict):
            continue
        group = group_map.get(correlation.get("correlation_id"))
        if not group:
            continue
        headline = correlation.get("headline")
        narrative = correlation.get("narrative")
        confidence_score = correlation.get("confidence_score")
        if not isinstance(headline, str) or not headline.strip():
            continue
        if not isinstance(narrative, str) or not narrative.strip():
            continue
        if not isinstance(confidence_score, (int, float)) or isinstance(confidence_score, bool):
            continue
        normalized.append(
            {
                "id": correlation["correlation_id"],
                "correlation_id": correlation["correlation_id"],
                "company_id": group["company_id"],
                "company_name": group["company_name"],
                "signal_ids": group["signal_ids"],
                "categories": group["categories"],
                "headline": headline.strip(),
                "narrative": narrative.strip(),
                "confidence_score": confidence_score,
                "provenance": group["provenance"],
            }
        )
    return normalized


def build_cross_category_candidate_groups(signals: list[dict], company_profiles: list[dict]) -> list[dict]:
    profile_map = {profile["company_id"]: profile for profile in company_profiles}
    groups: dict[str, dict] = {}
    for signal in signals:
        key = signal["company_id"]
        group = groups.setdefault(
            key,
            {
                "correlation_id": f"correlation-{signal['company_id']}",
                "company_id": signal["company_id"],
                "company_name": signal["company_name"],
                "signal_ids": [],
                "categories": set(),
                "signals": [],
                "provenance": {
                    "source_ids": set(),
                    "snapshot_ids": set(),
                    "evidence_urls": set(),
                },
            },
        )
        group["signal_ids"].append(signal["id"])
        group["categories"].add(signal["category"])
        group["signals"].append(
            {
                "signal_id": signal["id"],
                "category": signal["category"],
                "signal_type": signal["signal_type"],
                "severity": signal["severity"],
                "title": signal["title"],
                "summary": signal["summary"],
                "timestamp": signal["timestamp"],
                "benchmark": signal.get("benchmark"),
                "impact_rubric": signal.get("impact_rubric"),
            }
        )
        provenance = signal.get("provenance", {})
        for source_id in provenance.get("source_ids", []):
            group["provenance"]["source_ids"].add(source_id)
        for snapshot_id in provenance.get("snapshot_ids", []):
            group["provenance"]["snapshot_ids"].add(snapshot_id)
        for url in provenance.get("evidence_urls", []):
            group["provenance"]["evidence_urls"].add(url)
    candidate_groups = []
    for group in groups.values():
        if len(group["categories"]) < 2:
            continue
        profile = profile_map.get(group["company_id"], {})
        candidate_groups.append(
            {
                "correlation_id": group["correlation_id"],
                "company_id": group["company_id"],
                "company_name": group["company_name"],
                "signal_ids": sorted(group["signal_ids"]),
                "categories": sorted(group["categories"]),
                "signals": sorted(group["signals"], key=lambda item: item["timestamp"], reverse=True),
                "company_profile": profile,
                "provenance": {
                    "source_ids": sorted(group["provenance"]["source_ids"]),
                    "snapshot_ids": sorted(group["provenance"]["snapshot_ids"]),
                    "evidence_urls": sorted(group["provenance"]["evidence_urls"]),
                },
            }
        )
    return candidate_groups


def build_cross_category_request_payload(candidate_groups: list[dict], settings: Settings) -> dict:
    return {
        "model": settings.openai_model,
        "input": [
            {
                "role": "developer",
                "content": [
                    {
                        "type": "input_text",
                        "text": (
                            "You correlate cross-category market intelligence signals. "
                            "Use only the supplied evidence, benchmarks, and provenance. "
                            "Return only JSON matching the provided schema."
                        ),
                    }
                ],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": json.dumps(
                            {
                                "companies": candidate_groups,
                            },
                            separators=(",", ":"),
                            ensure_ascii=True,
                        ),
                    }
                ],
            },
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": "market_monitor_cross_category_correlations",
                "schema": build_cross_category_response_schema(),
            }
        },
    }


def build_cross_category_response_schema() -> dict:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["correlations"],
        "properties": {
            "correlations": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "correlation_id",
                        "headline",
                        "narrative",
                        "confidence_score",
                    ],
                    "properties": {
                        "correlation_id": {"type": "string"},
                        "headline": {"type": "string"},
                        "narrative": {"type": "string"},
                        "confidence_score": {"type": "number"},
                    },
                },
            }
        },
    }
