from datetime import datetime, timezone
from urllib.parse import urlparse
import hashlib

from market_monitor_api.config import Settings
from market_monitor_api.services.api_contract import build_contract_payload
from market_monitor_api.services.market_signals import (
    VALID_MARKET_TOPICS,
    build_canonical_id,
    build_relative_time_label,
    collect_market_signals_dataset,
    deduplicate_strings,
)
from market_monitor_api.services.openai_service import (
    build_competitor_candidate_map,
    build_competitor_landscape_analysis,
)
from market_monitor_api.services.tinyfish import (
    build_latest_snapshot_by_source,
    build_snapshot_record,
    load_snapshots,
    persist_refresh_result,
    run_tinyfish_source,
    to_iso_timestamp,
)


class CompetitorIntelligenceConfigError(Exception):
    def __init__(self, code: str, message: str, status_code: int = 400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


def build_competitor_intelligence_response(
    settings: Settings,
    company_url: str,
    refresh: bool = False,
    top_n: int = 4,
) -> dict:
    normalized_company_url = validate_competitor_request_url(company_url)
    if not settings.tinyfish_configured:
        raise CompetitorIntelligenceConfigError(
            "tinyfish_not_configured",
            "TinyFish is required for competitor analysis. Set TINYFISH_API_KEY in the .env file.",
            status_code=503,
        )
    if not settings.openai_configured:
        raise CompetitorIntelligenceConfigError(
            "openai_not_configured",
            "OpenAI is required for competitor analysis. Set OPENAI_API_KEY and OPENAI_MODEL in the .env file.",
            status_code=503,
        )
    requested_top_n = max(1, min(top_n, 8))
    target_source = build_company_profile_source_definition(
        normalized_company_url,
        role="target",
        company_name_hint=None,
    )
    target_snapshot = load_or_refresh_company_profile_snapshot(settings, target_source, refresh=refresh)
    target_profile = normalize_company_profile_snapshot(target_snapshot, target_source)
    if not target_profile:
        raise CompetitorIntelligenceConfigError(
            "target_profile_unavailable",
            "Unable to extract a usable company profile from the provided URL.",
            status_code=502,
        )
    market_category = normalize_company_market_category(target_profile.get("market_category"))
    market_dataset = collect_market_signals_dataset(
        settings,
        refresh=False,
        filters={"market_category": market_category} if market_category in VALID_MARKET_TOPICS else {},
        include_correlations=False,
    )
    market_context = build_competitor_market_context(target_profile, market_dataset["active_items"], market_category)
    candidate_limit = max(requested_top_n + 2, 6)
    candidate_map = build_competitor_candidate_map(target_profile, market_context, settings, candidate_limit)
    competitor_runs = [build_competitor_analysis_run(target_snapshot, target_source, target_profile["company_name"], "target")]
    verified_competitors = []
    verified_urls = set()
    for candidate in candidate_map:
        if len(verified_competitors) >= requested_top_n:
            break
        candidate_url = candidate.get("homepage_url")
        if not isinstance(candidate_url, str):
            continue
        normalized_candidate_url = normalize_company_url(candidate_url)
        if not normalized_candidate_url or normalized_candidate_url == normalized_company_url:
            continue
        if normalized_candidate_url in verified_urls:
            continue
        competitor_source = build_company_profile_source_definition(
            normalized_candidate_url,
            role="competitor",
            company_name_hint=candidate.get("company_name"),
        )
        competitor_snapshot = load_or_refresh_company_profile_snapshot(settings, competitor_source, refresh=refresh)
        competitor_profile = normalize_company_profile_snapshot(competitor_snapshot, competitor_source)
        competitor_runs.append(
            build_competitor_analysis_run(
                competitor_snapshot,
                competitor_source,
                candidate.get("company_name") or competitor_source["company_name"],
                "competitor",
            )
        )
        if not competitor_profile:
            continue
        if not competitor_profile_matches_candidate(candidate, competitor_profile):
            continue
        related_signals = build_related_competitor_market_signals(
            competitor_profile["company_name"],
            market_dataset["active_items"],
        )
        verified_urls.add(normalized_candidate_url)
        verified_competitors.append(
            build_verified_competitor_entry(
                candidate,
                competitor_profile,
                competitor_snapshot,
                competitor_source,
                related_signals,
            )
        )
    landscape_analysis = build_competitor_landscape_analysis(
        target_profile,
        verified_competitors,
        market_context,
        settings,
        requested_top_n,
    )
    competitors = merge_competitor_landscape_analysis(verified_competitors, landscape_analysis)
    latest_snapshot_at = build_latest_competitor_snapshot_at([target_snapshot] + [item["snapshot"] for item in verified_competitors])
    return {
        "contract": build_contract_payload("competitor_intelligence", view="overview"),
        "meta": build_competitor_meta(
            settings,
            normalized_company_url,
            requested_top_n,
            refresh,
            target_profile,
            competitors,
            competitor_runs,
            latest_snapshot_at,
            market_category,
        ),
        "summary_cards": build_competitor_summary_cards(target_profile, competitors, market_context),
        "target_company": build_target_company_payload(target_profile, target_snapshot, target_source),
        "landscape": build_competitor_landscape_payload(landscape_analysis, competitors),
        "competitors": competitors,
        "market_context": market_context,
        "analysis_runs": competitor_runs,
        "source_health": market_dataset["source_health"],
        "sources": market_dataset["sources"],
    }


def validate_competitor_request_url(company_url: str | None) -> str:
    normalized = normalize_company_url(company_url)
    if normalized:
        return normalized
    raise CompetitorIntelligenceConfigError(
        "invalid_company_url",
        "A valid public company URL is required.",
        status_code=400,
    )


def normalize_company_url(company_url: str | None) -> str | None:
    if not isinstance(company_url, str):
        return None
    normalized = company_url.strip()
    if not normalized:
        return None
    parsed = urlparse(normalized)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    if parsed.query:
        normalized = normalized.split("?", 1)[0]
    if normalized.endswith("/"):
        return normalized[:-1]
    return normalized


def build_company_profile_source_definition(
    company_url: str,
    role: str,
    company_name_hint: str | None,
) -> dict:
    hostname = build_url_hostname(company_url) or "company"
    source_id = build_company_profile_source_id(role, company_url)
    company_id = build_company_profile_company_id(company_url)
    company_name = company_name_hint.strip() if isinstance(company_name_hint, str) and company_name_hint.strip() else hostname
    return {
        "id": source_id,
        "name": build_company_profile_source_name(role, company_name),
        "category": "reputation_intelligence",
        "company_id": company_id,
        "company_name": company_name,
        "target_url": company_url,
        "goal": build_company_profile_goal(role, company_name),
        "output_schema": build_company_profile_schema(),
        "stop_conditions": build_company_profile_stop_conditions(),
        "error_handling": build_company_profile_error_handling(),
        "browser_profile": "lite",
        "proxy_config": {
            "enabled": True,
            "country_code": "US",
        },
        "use_vault": False,
        "credential_item_ids": [],
        "sector": None,
        "geography": None,
        "product_category": None,
        "revenue_exposure_weight": None,
    }


def build_company_profile_source_id(role: str, company_url: str) -> str:
    digest = hashlib.sha1(company_url.encode("utf-8")).hexdigest()[:16]
    return f"source-competitor-{role}-{digest}"


def build_company_profile_company_id(company_url: str) -> str:
    digest = hashlib.sha1(company_url.encode("utf-8")).hexdigest()[:16]
    return f"company-profile-{digest}"


def build_company_profile_source_name(role: str, company_name: str) -> str:
    role_label = "Target" if role == "target" else "Peer"
    return f"Competitor {role_label}: {company_name}"


def build_company_profile_goal(role: str, company_name: str) -> str:
    role_label = "target company" if role == "target" else "competitor candidate"
    return (
        f"Visit the provided public company website for the {role_label} and extract a structured competitive profile. "
        f"Identify the company name, homepage URL, a concise company summary, the primary market category, sector, geography, "
        f"target customers, core products or services, visible differentiators, visible pricing or packaging signals, and supporting evidence URLs. "
        f"Do not invent claims that are not visible on the site. Return facts that help compare {company_name} in a competitor landscape."
    )


def build_company_profile_schema() -> dict:
    return {
        "type": "object",
        "required": [
            "captured_at",
            "company_name",
            "homepage_url",
            "summary",
            "market_category",
            "sector",
            "geography",
            "target_customers",
            "products",
            "differentiators",
            "pricing_signals",
            "evidence_urls",
        ],
        "properties": {
            "captured_at": {"type": "string"},
            "company_name": {"type": "string"},
            "homepage_url": {"type": "string"},
            "summary": {"type": "string"},
            "market_category": {"type": "string"},
            "sector": {"type": "string"},
            "geography": {"type": "string"},
            "target_customers": {
                "type": "array",
                "items": {"type": "string"},
            },
            "products": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["name", "category", "summary"],
                    "properties": {
                        "name": {"type": "string"},
                        "category": {"type": "string"},
                        "summary": {"type": "string"},
                    },
                },
            },
            "differentiators": {
                "type": "array",
                "items": {"type": "string"},
            },
            "pricing_signals": {
                "type": "array",
                "items": {"type": "string"},
            },
            "evidence_urls": {
                "type": "array",
                "items": {"type": "string"},
            },
        },
    }


def build_company_profile_stop_conditions() -> list[str]:
    return [
        "Stop after the homepage and up to two visibly linked product, solutions, or pricing pages have been reviewed.",
        "Stop after eight relevant visible sections have been extracted.",
        "Stop after two consecutive navigation or parsing failures.",
    ]


def build_company_profile_error_handling() -> dict:
    return {
        "timeout": {
            "action": "retry",
            "max_retries": 2,
            "emit_status": "failed",
        },
        "layout_changed": {
            "action": "store_raw_payload",
            "emit_status": "failed",
        },
        "schema_validation_failed": {
            "action": "store_raw_payload",
            "emit_status": "validation_error",
        },
    }


def load_or_refresh_company_profile_snapshot(settings: Settings, source: dict, refresh: bool) -> dict:
    snapshots = load_snapshots(settings)
    latest_snapshot = build_latest_snapshot_by_source(snapshots).get(source["id"])
    usable_snapshot = select_latest_usable_company_profile_snapshot(snapshots, source["id"])
    if usable_snapshot and not refresh:
        return usable_snapshot
    run_response = run_tinyfish_source(settings, source)
    snapshot = build_snapshot_record(source, run_response)
    return persist_refresh_result(settings, snapshot, latest_snapshot)


def select_latest_usable_company_profile_snapshot(snapshots: list[dict], source_id: str) -> dict | None:
    candidates = [
        snapshot
        for snapshot in snapshots
        if snapshot.get("source_id") == source_id
        and snapshot.get("capture_status") in {"COMPLETED", "VALIDATION_ERROR"}
        and isinstance(snapshot.get("result"), dict)
    ]
    if not candidates:
        return None
    return candidates[-1]


def normalize_company_profile_snapshot(snapshot: dict, source: dict) -> dict | None:
    if not snapshot or snapshot.get("capture_status") not in {"COMPLETED", "VALIDATION_ERROR"}:
        return None
    result = snapshot.get("result")
    if not isinstance(result, dict):
        return None
    company_name = read_non_empty_string(result.get("company_name"))
    summary = read_non_empty_string(result.get("summary"))
    homepage_url = normalize_company_url(result.get("homepage_url")) or source["target_url"]
    captured_at = read_non_empty_string(result.get("captured_at")) or snapshot.get("captured_at")
    if not company_name or not summary or not homepage_url or not isinstance(captured_at, str):
        return None
    products = build_company_profile_products(result.get("products"))
    target_customers = build_string_list(result.get("target_customers"))
    differentiators = build_string_list(result.get("differentiators"))
    pricing_signals = build_string_list(result.get("pricing_signals"))
    evidence_urls = build_url_list(result.get("evidence_urls"))
    market_category = normalize_company_market_category(result.get("market_category") or result.get("sector"))
    sector = read_non_empty_string(result.get("sector")) or "Unknown"
    geography = read_non_empty_string(result.get("geography")) or "Unknown"
    return {
        "company_id": build_canonical_id("competitor_company", homepage_url or company_name),
        "company_name": company_name,
        "homepage_url": homepage_url,
        "summary": summary,
        "market_category": market_category,
        "sector": sector,
        "geography": geography,
        "target_customers": target_customers,
        "products": products,
        "differentiators": differentiators,
        "pricing_signals": pricing_signals,
        "evidence_urls": evidence_urls,
        "captured_at": captured_at,
        "source_id": source["id"],
        "source_name": source["name"],
    }


def build_company_profile_products(raw_products) -> list[dict]:
    if not isinstance(raw_products, list):
        return []
    products = []
    for item in raw_products:
        if not isinstance(item, dict):
            continue
        name = read_non_empty_string(item.get("name"))
        category = read_non_empty_string(item.get("category")) or "General"
        summary = read_non_empty_string(item.get("summary")) or name
        if not name:
            continue
        products.append(
            {
                "name": name,
                "category": category,
                "summary": summary,
            }
        )
    return products


def read_non_empty_string(value) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if not normalized:
        return None
    return normalized


def build_string_list(value) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item.strip() for item in value if isinstance(item, str) and item.strip()]


def build_url_list(value) -> list[str]:
    urls = []
    for item in build_string_list(value):
        normalized = normalize_company_url(item)
        if normalized:
            urls.append(normalized)
    return deduplicate_strings(urls)


def normalize_company_market_category(value) -> str | None:
    normalized = read_non_empty_string(value)
    if not normalized:
        return None
    lowercase = normalized.lower()
    if lowercase in {"tech", "technology", "ai", "software", "saas", "developer tools", "artificial intelligence"}:
        return "tech"
    if lowercase in {"finance", "financial", "fintech", "payments", "banking", "insurance"}:
        return "finance"
    if any(keyword in lowercase for keyword in ["tech", "ai", "software", "developer", "infrastructure", "cloud", "data", "chip"]):
        return "tech"
    if any(keyword in lowercase for keyword in ["finance", "fintech", "bank", "payment", "payments", "insurance", "trading"]):
        return "finance"
    return None


def build_competitor_market_context(target_profile: dict, signals: list[dict], market_category: str | None) -> dict:
    relevant_signals = []
    mentioned_companies = []
    for signal in signals:
        signal_market_categories = signal.get("market_categories") or ([signal.get("market_category")] if signal.get("market_category") else [])
        if market_category in VALID_MARKET_TOPICS and market_category not in signal_market_categories:
            continue
        relevant_signals.append(build_market_context_signal(signal))
        mentioned_companies.extend(extract_signal_companies(signal))
    return {
        "target_company_name": target_profile["company_name"],
        "market_category": market_category,
        "signal_count": len(relevant_signals),
        "signals": relevant_signals[:12],
        "mentioned_companies": deduplicate_strings(mentioned_companies)[:40],
    }


def build_market_context_signal(signal: dict) -> dict:
    return {
        "signal_id": signal["id"],
        "title": signal["title"],
        "summary": signal["summary"],
        "signal_type": signal["signal_type"],
        "severity": signal["severity"],
        "market_category": signal.get("market_category"),
        "timestamp": signal["timestamp"],
        "source_name": signal.get("source_name"),
        "mentioned_companies": extract_signal_companies(signal),
        "evidence_urls": signal.get("provenance", {}).get("evidence_urls", []),
    }


def extract_signal_companies(signal: dict) -> list[str]:
    detail = signal.get("detail") or {}
    companies = detail.get("mentioned_companies")
    if isinstance(companies, list):
        return [item.strip() for item in companies if isinstance(item, str) and item.strip()]
    competitor_name = signal.get("competitor_name")
    if isinstance(competitor_name, str) and competitor_name.strip():
        return [competitor_name.strip()]
    return []


def build_related_competitor_market_signals(company_name: str, signals: list[dict]) -> list[dict]:
    related = []
    for signal in signals:
        if not signal_matches_company(signal, company_name):
            continue
        evidence_url = None
        evidence = signal.get("evidence")
        if isinstance(evidence, list) and evidence and isinstance(evidence[0], dict):
            evidence_url = evidence[0].get("url")
        related.append(
            {
                "signal_id": signal["id"],
                "title": signal["title"],
                "summary": signal["summary"],
                "severity": signal["severity"],
                "market_category": signal.get("market_category"),
                "timestamp": signal["timestamp"],
                "relative_time_label": signal.get("relative_time_label") or build_relative_time_label(signal["timestamp"]),
                "source_name": signal.get("source_name"),
                "evidence_url": evidence_url,
            }
        )
    return related[:5]


def signal_matches_company(signal: dict, company_name: str) -> bool:
    normalized_company_name = normalize_company_name(company_name)
    if not normalized_company_name:
        return False
    explicit_companies = [normalize_company_name(item) for item in extract_signal_companies(signal)]
    if normalized_company_name in explicit_companies:
        return True
    title = normalize_company_name(signal.get("title"))
    if title and normalized_company_name in title:
        return True
    return False


def normalize_company_name(value) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.lower().replace("&", " ").replace("-", " ").split())


def competitor_profile_matches_candidate(candidate: dict, profile: dict) -> bool:
    candidate_url = normalize_company_url(candidate.get("homepage_url"))
    profile_url = normalize_company_url(profile.get("homepage_url"))
    if candidate_url and profile_url and build_url_hostname(candidate_url) == build_url_hostname(profile_url):
        return True
    candidate_tokens = build_company_name_tokens(candidate.get("company_name"))
    profile_tokens = build_company_name_tokens(profile.get("company_name"))
    if not candidate_tokens or not profile_tokens:
        return False
    overlap = candidate_tokens & profile_tokens
    minimum_overlap = 1 if min(len(candidate_tokens), len(profile_tokens)) == 1 else 2
    return len(overlap) >= minimum_overlap


def build_company_name_tokens(value) -> set[str]:
    if not isinstance(value, str):
        return set()
    normalized = value.lower().replace("&", " ").replace("-", " ")
    return {token for token in normalized.split() if len(token) > 1}


def build_url_hostname(value: str | None) -> str | None:
    normalized = normalize_company_url(value)
    if not normalized:
        return None
    hostname = urlparse(normalized).netloc.lower()
    if hostname.startswith("www."):
        return hostname[4:]
    return hostname


def build_verified_competitor_entry(
    candidate: dict,
    profile: dict,
    snapshot: dict,
    source: dict,
    related_signals: list[dict],
) -> dict:
    evidence_urls = deduplicate_strings(profile["evidence_urls"] + [item["evidence_url"] for item in related_signals if item.get("evidence_url")])
    return {
        "competitor_id": profile["company_id"],
        "company_name": profile["company_name"],
        "homepage_url": profile["homepage_url"],
        "candidate_fit_score": clamp_score(candidate.get("fit_score")),
        "candidate_confidence_score": clamp_unit_score(candidate.get("confidence_score")),
        "candidate_reasoning": read_non_empty_string(candidate.get("reasoning")),
        "overlap_areas": build_string_list(candidate.get("overlap_areas")),
        "profile": profile,
        "related_signals": related_signals,
        "snapshot": snapshot,
        "source": source,
        "provenance": {
            "source_ids": [source["id"]],
            "snapshot_ids": [snapshot["snapshot_id"]],
            "run_ids": [snapshot.get("run", {}).get("run_id")] if snapshot.get("run", {}).get("run_id") else [],
            "evidence_urls": evidence_urls,
            "target_urls": deduplicate_strings([source["target_url"], profile["homepage_url"]]),
            "file_paths": [snapshot["file_path"]] if isinstance(snapshot.get("file_path"), str) else [],
        },
    }


def clamp_score(value) -> int | None:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    return max(0, min(100, int(round(float(value)))))


def clamp_unit_score(value) -> float | None:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    normalized = float(value)
    if normalized > 1:
        normalized = normalized / 100
    return round(max(0.0, min(1.0, normalized)), 3)


def build_competitor_analysis_run(snapshot: dict, source: dict, company_name: str, role: str) -> dict:
    run = snapshot.get("run", {}) if isinstance(snapshot, dict) else {}
    return {
        "analysis_run_id": build_canonical_id("competitor_run", f"{role}:{source['id']}:{snapshot.get('snapshot_id') if snapshot else source['target_url']}"),
        "role": role,
        "company_name": company_name,
        "source_id": source["id"],
        "source_name": source["name"],
        "target_url": source["target_url"],
        "capture_status": snapshot.get("capture_status") if isinstance(snapshot, dict) else None,
        "captured_at": snapshot.get("captured_at") if isinstance(snapshot, dict) else None,
        "snapshot_id": snapshot.get("snapshot_id") if isinstance(snapshot, dict) else None,
        "run_id": run.get("run_id"),
        "error": run.get("error"),
    }


def merge_competitor_landscape_analysis(verified_competitors: list[dict], landscape_analysis: dict) -> list[dict]:
    analysis_items = landscape_analysis.get("competitors", []) if isinstance(landscape_analysis, dict) else []
    analysis_by_hostname = {}
    analysis_by_name = {}
    for item in analysis_items:
        if not isinstance(item, dict):
            continue
        hostname = build_url_hostname(item.get("competitor_url"))
        if hostname:
            analysis_by_hostname[hostname] = item
        normalized_name = normalize_company_name(item.get("competitor_name"))
        if normalized_name:
            analysis_by_name[normalized_name] = item
    competitors = []
    for competitor in verified_competitors:
        hostname = build_url_hostname(competitor["homepage_url"])
        analysis = analysis_by_hostname.get(hostname) or analysis_by_name.get(normalize_company_name(competitor["company_name"])) or {}
        strengths = normalize_competitor_strengths(analysis.get("strengths"))
        pain_points = normalize_competitor_strengths(analysis.get("pain_points"))
        fit_score = clamp_score(analysis.get("fit_score"))
        confidence_score = clamp_unit_score(analysis.get("confidence_score"))
        competitor_payload = {
            "id": build_canonical_id("competitor_analysis", competitor["homepage_url"]),
            "timestamp": competitor["profile"]["captured_at"],
            "company_name": competitor["company_name"],
            "homepage_url": competitor["homepage_url"],
            "fit_score": fit_score if fit_score is not None else competitor["candidate_fit_score"],
            "confidence_score": confidence_score if confidence_score is not None else competitor["candidate_confidence_score"],
            "reasoning": read_non_empty_string(analysis.get("reasoning")) or competitor["candidate_reasoning"],
            "overlap_areas": deduplicate_strings(competitor["overlap_areas"]),
            "strengths": strengths,
            "pain_points": pain_points,
            "score_breakdown": normalize_competitor_score_breakdown(analysis.get("score_breakdown")),
            "profile": {
                "summary": competitor["profile"]["summary"],
                "market_category": competitor["profile"]["market_category"],
                "sector": competitor["profile"]["sector"],
                "geography": competitor["profile"]["geography"],
                "target_customers": competitor["profile"]["target_customers"],
                "products": competitor["profile"]["products"],
                "differentiators": competitor["profile"]["differentiators"],
                "pricing_signals": competitor["profile"]["pricing_signals"],
                "captured_at": competitor["profile"]["captured_at"],
            },
            "related_signals": competitor["related_signals"],
            "provenance": competitor["provenance"],
            "verification_status": "verified",
        }
        competitors.append(competitor_payload)
    return sorted(
        competitors,
        key=lambda item: (
            item["fit_score"] if isinstance(item.get("fit_score"), int) else 0,
            item["confidence_score"] if isinstance(item.get("confidence_score"), float) else 0,
            item["timestamp"],
        ),
        reverse=True,
    )


def normalize_competitor_strengths(items) -> list[dict]:
    if not isinstance(items, list):
        return []
    normalized = []
    for item in items:
        if not isinstance(item, dict):
            continue
        title = read_non_empty_string(item.get("title"))
        reasoning = read_non_empty_string(item.get("reasoning"))
        if not title or not reasoning:
            continue
        normalized.append(
            {
                "title": title,
                "reasoning": reasoning,
            }
        )
    return normalized[:4]


def normalize_competitor_score_breakdown(payload) -> dict:
    if not isinstance(payload, dict):
        return {}
    normalized = {}
    for key in ("product_overlap", "audience_overlap", "market_momentum", "differentiation_gap"):
        value = clamp_score(payload.get(key))
        if value is not None:
            normalized[key] = value
    return normalized


def build_target_company_payload(target_profile: dict, snapshot: dict, source: dict) -> dict:
    return {
        "company_name": target_profile["company_name"],
        "homepage_url": target_profile["homepage_url"],
        "summary": target_profile["summary"],
        "market_category": target_profile["market_category"],
        "sector": target_profile["sector"],
        "geography": target_profile["geography"],
        "target_customers": target_profile["target_customers"],
        "products": target_profile["products"],
        "differentiators": target_profile["differentiators"],
        "pricing_signals": target_profile["pricing_signals"],
        "captured_at": target_profile["captured_at"],
        "provenance": {
            "source_ids": [source["id"]],
            "snapshot_ids": [snapshot["snapshot_id"]],
            "run_ids": [snapshot.get("run", {}).get("run_id")] if snapshot.get("run", {}).get("run_id") else [],
            "evidence_urls": target_profile["evidence_urls"],
            "target_urls": deduplicate_strings([source["target_url"], target_profile["homepage_url"]]),
            "file_paths": [snapshot["file_path"]] if isinstance(snapshot.get("file_path"), str) else [],
        },
    }


def build_competitor_landscape_payload(landscape_analysis: dict, competitors: list[dict]) -> dict:
    summary = read_non_empty_string(landscape_analysis.get("summary")) if isinstance(landscape_analysis, dict) else None
    confidence_score = clamp_unit_score(landscape_analysis.get("confidence_score")) if isinstance(landscape_analysis, dict) else None
    return {
        "summary": summary,
        "confidence_score": confidence_score,
        "competitor_count": len(competitors),
        "generated": bool(summary),
    }


def build_competitor_summary_cards(target_profile: dict, competitors: list[dict], market_context: dict) -> list[dict]:
    fit_scores = [item["fit_score"] for item in competitors if isinstance(item.get("fit_score"), int)]
    highest_score = max(fit_scores) if fit_scores else 0
    return [
        {
            "id": "target_company",
            "label": "Target Company",
            "value": target_profile["company_name"],
        },
        {
            "id": "verified_competitors",
            "label": "Verified Competitors",
            "value": len(competitors),
        },
        {
            "id": "strongest_fit",
            "label": "Strongest Fit",
            "value": highest_score,
        },
        {
            "id": "market_signals",
            "label": "Relevant Signals",
            "value": market_context["signal_count"],
        },
    ]


def build_competitor_meta(
    settings: Settings,
    company_url: str,
    requested_top_n: int,
    refresh: bool,
    target_profile: dict,
    competitors: list[dict],
    analysis_runs: list[dict],
    latest_snapshot_at: str | None,
    market_category: str | None,
) -> dict:
    openai_status = "completed" if competitors else "unavailable"
    return {
        "api_version": "v1",
        "module": "competitor_intelligence",
        "generated_at": to_iso_timestamp(datetime.now(timezone.utc)),
        "company_url": company_url,
        "target_company_name": target_profile["company_name"],
        "market_category": market_category,
        "refresh_requested": refresh,
        "requested_top_n": requested_top_n,
        "verified_competitor_count": len(competitors),
        "analysis_run_count": len(analysis_runs),
        "latest_snapshot_at": latest_snapshot_at,
        "integrations": {
            "tinyfish": {
                "configured": settings.tinyfish_configured,
                "provider": "TinyFish",
                "base_url": settings.tinyfish_base_url,
            },
            "openai": {
                "configured": settings.openai_configured,
                "provider": "OpenAI",
                "model": settings.openai_model,
                "base_url": settings.openai_base_url,
                "status": openai_status,
            },
        },
    }


def build_latest_competitor_snapshot_at(snapshots: list[dict]) -> str | None:
    valid_timestamps = [snapshot.get("captured_at") for snapshot in snapshots if isinstance(snapshot.get("captured_at"), str)]
    if not valid_timestamps:
        return None
    return max(valid_timestamps)
