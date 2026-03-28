import base64
import json
import logging
import mimetypes
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote_plus

from market_monitor_api.config import Settings
from market_monitor_api.services.commerce_intelligence import (
    build_commerce_comparisons,
    build_commerce_signals,
    build_commerce_snapshots,
    build_commerce_sources,
    build_current_listings,
)
from market_monitor_api.services.growth_intelligence import (
    build_growth_comparisons,
    build_growth_events,
    build_growth_snapshots,
    build_growth_sources,
)
from market_monitor_api.services.openai_service import run_product_viability_analysis
from market_monitor_api.services.tinyfish import (
    build_company_catalog,
    build_product_catalog,
    load_snapshots,
    load_source_catalog,
    run_ad_hoc_tinyfish_research,
    to_iso_timestamp,
)


ALLOWED_IMAGE_MIME_TYPES = {
    "image/gif",
    "image/jpeg",
    "image/png",
    "image/webp",
}

PRODUCT_VIABILITY_TEXT_FIELDS = (
    "product_name",
    "description",
    "category",
    "price_point",
    "target_customer",
    "market_context",
)
PRODUCT_VIABILITY_NATURAL_LANGUAGE_FIELDS = ("query", "prompt", "request")

VALID_RESEARCH_DEPTHS = {"standard", "deep"}
DEEP_RESEARCH_LANES = ("competitors", "pricing", "demand")
TINYFISH_SEARCH_BASE_URL = "https://duckduckgo.com/"
SEARCH_STOP_WORDS = {
    "a",
    "an",
    "and",
    "are",
    "around",
    "assume",
    "be",
    "by",
    "for",
    "from",
    "how",
    "i",
    "if",
    "in",
    "is",
    "it",
    "make",
    "of",
    "on",
    "or",
    "should",
    "that",
    "the",
    "their",
    "there",
    "this",
    "to",
    "viable",
    "viability",
    "what",
    "who",
    "why",
    "with",
    "would",
}
LOGGER = logging.getLogger(__name__)


class ProductViabilityError(Exception):
    def __init__(self, code: str, message: str, status_code: int):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


@dataclass(frozen=True)
class ProductViabilityImage:
    filename: str
    content_type: str
    size_bytes: int
    data_url: str


@dataclass(frozen=True)
class ProductViabilityInput:
    natural_language_input: str
    product_name: str
    description: str
    category: str
    price_point: str
    target_customer: str
    market_context: str
    research_depth: str
    images: list[ProductViabilityImage]

    def as_prompt_payload(self) -> dict[str, Any]:
        return {
            "natural_language_input": self.natural_language_input,
            "product_name": self.product_name,
            "description": self.description,
            "category": self.category,
            "price_point": self.price_point,
            "target_customer": self.target_customer,
            "market_context": self.market_context,
            "research_depth": self.research_depth,
            "images": [
                {
                    "filename": image.filename,
                    "content_type": image.content_type,
                    "size_bytes": image.size_bytes,
                    "data_url": image.data_url,
                }
                for image in self.images
            ],
        }


def parse_product_viability_input(form, files, settings: Settings) -> ProductViabilityInput:
    if not settings.tinyfish_configured:
        raise ProductViabilityError(
            "tinyfish_not_configured",
            "TinyFish is required for product viability analysis. Set TINYFISH_API_KEY.",
            503,
        )

    text_fields = {field_name: normalize_text(form.get(field_name)) for field_name in PRODUCT_VIABILITY_TEXT_FIELDS}
    natural_language_input = parse_natural_language_input(form)
    images = parse_product_images(files.getlist("images"), settings)
    research_depth = parse_research_depth(form.get("research_depth"))

    if not text_fields["description"] and not natural_language_input and not images:
        raise ProductViabilityError(
            "product_viability_input_missing",
            "Provide a natural-language request, a description, at least one image, or a combination of them.",
            400,
        )

    if not text_fields["description"] and natural_language_input:
        text_fields["description"] = natural_language_input

    return ProductViabilityInput(
        natural_language_input=natural_language_input,
        images=images,
        research_depth=research_depth,
        **text_fields,
    )


def parse_natural_language_input(form) -> str:
    for field_name in PRODUCT_VIABILITY_NATURAL_LANGUAGE_FIELDS:
        value = normalize_text(form.get(field_name))
        if value:
            return value
    return ""


def parse_product_images(raw_images: list, settings: Settings) -> list[ProductViabilityImage]:
    if len(raw_images) > settings.product_viability_max_images:
        raise ProductViabilityError(
            "product_viability_too_many_images",
            f"Submit at most {settings.product_viability_max_images} images.",
            400,
        )

    images: list[ProductViabilityImage] = []
    for storage in raw_images:
        image_bytes = storage.read()
        storage.stream.seek(0)
        if not image_bytes:
            continue

        content_type = resolve_image_mime_type(storage)
        if content_type not in ALLOWED_IMAGE_MIME_TYPES:
            raise ProductViabilityError(
                "product_viability_unsupported_media_type",
                "Only PNG, JPEG, WEBP, and GIF images are supported.",
                415,
            )

        size_bytes = len(image_bytes)
        if size_bytes > settings.product_viability_max_image_bytes:
            raise ProductViabilityError(
                "product_viability_image_too_large",
                f"Each image must be {settings.product_viability_max_image_bytes} bytes or smaller.",
                413,
            )

        encoded = base64.b64encode(image_bytes).decode("ascii")
        images.append(
            ProductViabilityImage(
                filename=storage.filename or "upload",
                content_type=content_type,
                size_bytes=size_bytes,
                data_url=f"data:{content_type};base64,{encoded}",
            )
        )
    return images


def parse_research_depth(raw_value: str | None) -> str:
    normalized = normalize_text(raw_value).lower()
    if not normalized:
        return "standard"
    if normalized not in VALID_RESEARCH_DEPTHS:
        raise ProductViabilityError(
            "product_viability_invalid_research_depth",
            "research_depth must be one of: standard, deep.",
            400,
        )
    return normalized


def resolve_image_mime_type(storage) -> str:
    if storage.mimetype and storage.mimetype != "application/octet-stream":
        return storage.mimetype
    guessed_type, _ = mimetypes.guess_type(storage.filename or "")
    return guessed_type or ""


def build_product_viability_response(settings: Settings, payload: ProductViabilityInput) -> dict:
    live_market_research = build_product_viability_live_research(settings, payload)
    local_signal_context = build_product_viability_enrichment(settings, payload)
    research_error = extract_live_research_error(live_market_research)
    evidence_context = {
        "used_live_research": True,
        "live_market_research": live_market_research,
        "local_signal_context": local_signal_context,
    }
    decision_memo, decision_provider, openai_status, decision_status = build_product_viability_decision_memo(
        settings,
        payload,
        evidence_context,
        live_market_research,
        local_signal_context,
    )
    meta = {
        "generated_at": to_iso_timestamp(datetime.now(timezone.utc)),
        "research_depth": payload.research_depth,
        "research_status": live_market_research["status"],
        "decision_provider": decision_provider,
        "decision_status": decision_status,
        "openai_status": openai_status,
        "used_local_context": local_signal_context["used_local_context"],
        "research_error": research_error,
    }
    internal_response = {
        "meta": {
            **meta,
            "model": settings.openai_model if decision_provider == "OpenAI" else None,
            "used_live_research": True,
            "live_research_provider": "TinyFish",
            "live_research_status": live_market_research["status"],
            "live_research_lanes": live_market_research["lane_statuses"],
            "openai_configured": settings.openai_configured,
            "related_growth_events_count": local_signal_context["related_signal_context"]["growth_events_count"],
            "related_commerce_signals_count": local_signal_context["related_signal_context"]["commerce_signals_count"],
        },
        "input_echo": build_input_echo(payload),
        "matched_entities": local_signal_context["matched_entities"],
        "related_signal_context": local_signal_context["related_signal_context"],
        "live_market_research": {
            "summary": live_market_research["summary"],
            "competitors": live_market_research["competitors"],
            "pricing_landscape": live_market_research["pricing_landscape"],
            "demand_signals": live_market_research["demand_signals"],
            "risks": live_market_research["risks"],
            "source_citations": live_market_research["source_citations"],
            "lane_reports": live_market_research["lane_reports"],
        },
        "decision_memo": decision_memo,
    }
    log_product_viability_response(internal_response)
    return build_frontend_product_viability_response(meta, live_market_research, decision_memo)


def build_frontend_product_viability_response(meta: dict, live_market_research: dict, decision_memo: dict | None) -> dict:
    return {
        "status": build_frontend_response_status(meta, decision_memo),
        "summary": build_frontend_summary(live_market_research, decision_memo),
        "recommendation": decision_memo.get("recommendation") if decision_memo else None,
        "viability_score": decision_memo.get("viability_score") if decision_memo else None,
        "confidence_score": decision_memo.get("confidence_score") if decision_memo else None,
        "highlights": {
            "strengths": (decision_memo.get("strengths") or [])[:3] if decision_memo else [],
            "risks": (decision_memo.get("risks") or [])[:3] if decision_memo else [],
            "pricing_fit": decision_memo.get("pricing_fit") if decision_memo else None,
            "differentiation": decision_memo.get("differentiation") if decision_memo else None,
            "next_validation_steps": (decision_memo.get("next_validation_steps") or [])[:3] if decision_memo else [],
            "demand_signals": (decision_memo.get("demand_signals") or [])[:3] if decision_memo else [],
        },
        "competitors": build_frontend_competitors(live_market_research["competitors"]),
        "sources": build_frontend_citations(live_market_research["source_citations"]),
        "meta": meta,
    }


def build_frontend_response_status(meta: dict, decision_memo: dict | None) -> str:
    if meta["decision_status"] == "pending":
        return "pending"
    if meta["decision_status"] == "failed":
        return "failed"
    if decision_memo:
        return "completed"
    return meta["research_status"]


def build_frontend_summary(live_market_research: dict, decision_memo: dict | None) -> str:
    if decision_memo and decision_memo.get("summary"):
        return decision_memo["summary"]
    if live_market_research.get("summary"):
        return live_market_research["summary"]
    if live_market_research["status"] == "pending":
        return "TinyFish research is still running."
    error = extract_live_research_error(live_market_research)
    if error and error.get("message"):
        return error["message"]
    if live_market_research["status"] == "failed":
        return "TinyFish research could not be completed for this request."
    return "No product viability summary is available yet."


def build_frontend_competitors(competitors: list[dict]) -> list[dict]:
    return [
        {
            "name": competitor["name"],
            "price_point": competitor.get("price_point") or None,
            "url": competitor["url"],
        }
        for competitor in competitors[:4]
    ]


def build_frontend_citations(citations: list[dict]) -> list[dict]:
    return [
        {
            "title": citation["title"],
            "url": citation["url"],
        }
        for citation in citations[:4]
    ]


def log_product_viability_response(internal_response: dict) -> None:
    LOGGER.info(
        "product_viability_internal_response=%s",
        json.dumps(internal_response, separators=(",", ":"), ensure_ascii=True),
    )


def build_product_viability_decision_memo(
    settings: Settings,
    payload: ProductViabilityInput,
    evidence_context: dict,
    live_market_research: dict,
    local_signal_context: dict,
) -> tuple[dict | None, str | None, str, str]:
    if live_market_research["status"] == "pending" and not has_live_research_evidence(live_market_research):
        return None, None, "skipped", "pending"
    if live_market_research["status"] == "failed" and not has_live_research_evidence(live_market_research):
        return None, None, "skipped", "failed"

    if should_run_openai_analysis(settings, payload):
        try:
            return (
                run_product_viability_analysis(payload.as_prompt_payload(), evidence_context, settings),
                "OpenAI",
                "completed",
                "completed",
            )
        except ValueError:
            pass
        return (
            build_tinyfish_decision_memo(payload, live_market_research, local_signal_context, image_analysis_used=False),
            "TinyFish",
            "failed",
            "completed",
        )

    return (
        build_tinyfish_decision_memo(
            payload,
            live_market_research,
            local_signal_context,
            image_analysis_used=False,
        ),
        "TinyFish",
        "skipped" if settings.openai_configured else "not_configured",
        "completed",
    )


def should_run_openai_analysis(settings: Settings, payload: ProductViabilityInput) -> bool:
    return settings.openai_configured and bool(payload.images)


def build_tinyfish_decision_memo(
    payload: ProductViabilityInput,
    live_market_research: dict,
    local_signal_context: dict,
    image_analysis_used: bool,
) -> dict:
    demand_signals = live_market_research["demand_signals"][:5]
    risks = live_market_research["risks"][:5]
    competitors = live_market_research["competitors"][:3]
    competitor_names = [competitor["name"] for competitor in competitors if competitor.get("name")]
    pricing_landscape = live_market_research["pricing_landscape"][:3]
    viability_score = estimate_viability_score(demand_signals, risks, live_market_research["source_citations"])
    recommendation = recommendation_for_score(viability_score)
    strengths = build_tinyfish_strengths(demand_signals, competitors, pricing_landscape)
    next_validation_steps = build_tinyfish_next_steps(payload, live_market_research, competitor_names)
    analysis_sources = build_tinyfish_analysis_sources(payload, local_signal_context, image_analysis_used)
    return {
        "summary": live_market_research["summary"] or "TinyFish collected market evidence for this product concept.",
        "viability_score": viability_score,
        "recommendation": recommendation,
        "target_customer": payload.target_customer or infer_target_customer(payload, demand_signals),
        "strengths": strengths,
        "risks": risks,
        "differentiation": build_tinyfish_differentiation(competitor_names),
        "pricing_fit": build_tinyfish_pricing_fit(payload, pricing_landscape),
        "demand_signals": demand_signals,
        "next_validation_steps": next_validation_steps,
        "confidence_score": estimate_confidence_score(
            live_market_research["source_citations"],
            competitor_names,
            demand_signals,
            local_signal_context["used_local_context"],
        ),
        "analysis_sources": analysis_sources,
    }


def has_live_research_evidence(live_market_research: dict) -> bool:
    return bool(
        live_market_research.get("summary")
        or live_market_research.get("competitors")
        or live_market_research.get("pricing_landscape")
        or live_market_research.get("demand_signals")
        or live_market_research.get("risks")
        or live_market_research.get("source_citations")
    )


def estimate_viability_score(demand_signals: list[str], risks: list[str], citations: list[dict]) -> int:
    score = 50
    score += min(20, len(demand_signals) * 6)
    score += min(15, len(citations) * 4)
    score -= min(20, len(risks) * 4)
    return max(0, min(100, score))


def recommendation_for_score(score: int) -> str:
    if score >= 75:
        return "strong_yes"
    if score >= 60:
        return "cautious_yes"
    if score >= 40:
        return "unclear"
    return "likely_no"


def build_tinyfish_strengths(
    demand_signals: list[str],
    competitors: list[dict],
    pricing_landscape: list[str],
) -> list[str]:
    strengths = list(demand_signals[:2])
    if competitors:
        strengths.append(f"Comparable products already exist, which validates baseline market demand against {competitors[0]['name']}.")
    if pricing_landscape:
        strengths.append(pricing_landscape[0])
    return strengths[:5]


def build_tinyfish_differentiation(competitor_names: list[str]) -> str:
    if competitor_names:
        joined = ", ".join(competitor_names[:3])
        return f"Differentiate clearly on convenience, usability, or format because buyers already have options like {joined}."
    return "Differentiate around a narrow buyer problem before broadening positioning."


def build_tinyfish_pricing_fit(payload: ProductViabilityInput, pricing_landscape: list[str]) -> str:
    if payload.price_point and pricing_landscape:
        return f"{payload.price_point} should be tested against this market evidence: {pricing_landscape[0]}"
    if pricing_landscape:
        return pricing_landscape[0]
    if payload.price_point:
        return f"Validate whether {payload.price_point} matches comparable products and buyer expectations."
    return "Pricing fit is still unclear from the available evidence."


def build_tinyfish_next_steps(
    payload: ProductViabilityInput,
    live_market_research: dict,
    competitor_names: list[str],
) -> list[str]:
    steps = []
    if competitor_names:
        steps.append(f"Compare your concept directly against {', '.join(competitor_names[:3])} on convenience, taste, and portability.")
    if payload.price_point:
        steps.append(f"Test buyer willingness to pay around {payload.price_point} with a landing page or preorder experiment.")
    steps.append("Interview likely buyers to validate the biggest purchase triggers and objections found in the research.")
    if live_market_research["risks"]:
        steps.append(f"Design a prototype or message test that addresses this risk first: {live_market_research['risks'][0]}")
    return steps[:4]


def infer_target_customer(payload: ProductViabilityInput, demand_signals: list[str]) -> str:
    if payload.category:
        return f"Buyers in {payload.category}"
    if demand_signals:
        return "Buyers represented in the demand signals TinyFish found"
    return ""


def estimate_confidence_score(
    citations: list[dict],
    competitor_names: list[str],
    demand_signals: list[str],
    used_local_context: bool,
) -> float:
    confidence = 0.35
    confidence += min(0.2, len(citations) * 0.05)
    confidence += min(0.15, len(competitor_names) * 0.04)
    confidence += min(0.15, len(demand_signals) * 0.04)
    if used_local_context:
        confidence += 0.1
    return round(max(0.0, min(0.95, confidence)), 4)


def build_tinyfish_analysis_sources(
    payload: ProductViabilityInput,
    local_signal_context: dict,
    image_analysis_used: bool,
) -> list[str]:
    sources = []
    if payload.description:
        sources.append("user_description")
    if image_analysis_used and payload.images:
        sources.append("user_images")
    sources.append("tinyfish_live_research")
    if local_signal_context["used_local_context"]:
        sources.append("local_signal_context")
    return sources


def build_input_echo(payload: ProductViabilityInput) -> dict:
    return {
        "natural_language_input": payload.natural_language_input or None,
        "product_name": payload.product_name or None,
        "description": payload.description or None,
        "category": payload.category or None,
        "price_point": payload.price_point or None,
        "target_customer": payload.target_customer or None,
        "market_context": payload.market_context or None,
        "research_depth": payload.research_depth,
        "image_count": len(payload.images),
    }


def build_product_viability_live_research(settings: Settings, payload: ProductViabilityInput) -> dict:
    if payload.research_depth == "standard":
        lane_result = run_ad_hoc_tinyfish_research(settings, build_standard_research_spec(payload))
        live_market_research = aggregate_live_market_research([lane_result])
    else:
        live_market_research = aggregate_live_market_research(run_deep_tinyfish_research(settings, payload))

    if live_market_research["status"] == "failed":
        LOGGER.warning(
            "product_viability_live_research_failed=%s",
            json.dumps(
                {
                    "input_echo": build_input_echo(payload),
                    "lane_reports": live_market_research["lane_reports"],
                },
                separators=(",", ":"),
                ensure_ascii=True,
            ),
        )
    return live_market_research


def run_deep_tinyfish_research(settings: Settings, payload: ProductViabilityInput) -> list[dict]:
    specs = [build_deep_research_spec(payload, lane) for lane in DEEP_RESEARCH_LANES]
    results = []
    with ThreadPoolExecutor(max_workers=len(specs)) as executor:
        futures = {
            executor.submit(run_ad_hoc_tinyfish_research, settings, spec): spec["lane"]
            for spec in specs
        }
        for future in as_completed(futures):
            results.append(future.result())
    order_map = {lane: index for index, lane in enumerate(DEEP_RESEARCH_LANES)}
    return sorted(results, key=lambda item: order_map.get(item["lane"], 99))


def build_standard_research_spec(payload: ProductViabilityInput) -> dict:
    return build_research_spec(
        payload,
        lane="standard",
        name="TinyFish Product Viability Research",
        query_suffix="competitors pricing demand customer pain points",
        goal=(
            "Research the commercial viability of the submitted product concept. "
            "Identify direct competitors, price anchors, demand signals, commercial risks, "
            "and cite the most relevant sources."
        ),
    )


def build_deep_research_spec(payload: ProductViabilityInput, lane: str) -> dict:
    lane_prompts = {
        "competitors": (
            "competitors alternatives positioning feature overlap",
            "Research direct competitors and substitute products. "
            "Focus on positioning, overlap, and how crowded the market appears.",
        ),
        "pricing": (
            "pricing marketplaces subscriptions discounts price anchors",
            "Research pricing and monetization. "
            "Focus on current price anchors, marketplace listings, discount behavior, and tier patterns.",
        ),
        "demand": (
            "reviews communities reddit forums customer pain points demand signals",
            "Research customer demand signals. "
            "Focus on reviews, communities, pain points, recurring needs, and evidence of real buyer interest.",
        ),
    }
    query_suffix, goal = lane_prompts[lane]
    return build_research_spec(
        payload,
        lane=lane,
        name=f"TinyFish Product Viability {lane.title()} Research",
        query_suffix=query_suffix,
        goal=goal,
    )


def build_research_spec(
    payload: ProductViabilityInput,
    lane: str,
    name: str,
    query_suffix: str,
    goal: str,
) -> dict:
    query = build_research_query(payload, query_suffix)
    return {
        "name": name,
        "lane": lane,
        "target_url": f"{TINYFISH_SEARCH_BASE_URL}?q={quote_plus(query)}",
        "goal": build_research_goal_prompt(payload, lane, goal),
        "output_schema": build_live_market_research_schema(),
        "stop_conditions": [
            "Stop after collecting enough evidence to fill the schema with concise findings.",
            "Stop after reviewing a small set of the most relevant sources.",
            "Stop after repeated navigation failures or blocked pages.",
        ],
        "error_handling": {
            "timeout": {"action": "return_partial_result", "emit_status": "failed"},
            "navigation_blocked": {"action": "return_partial_result", "emit_status": "failed"},
            "schema_validation_failed": {"action": "store_raw_payload", "emit_status": "validation_error"},
        },
        "browser_profile": "lite",
        "proxy_config": {"enabled": True, "country_code": "US"},
        "use_vault": False,
        "credential_item_ids": [],
    }


def build_research_query(payload: ProductViabilityInput, query_suffix: str) -> str:
    base_terms = build_base_research_terms(payload)
    suffix_terms = build_suffix_terms(query_suffix)
    query_terms = dedupe_terms(base_terms + suffix_terms)
    return " ".join(query_terms[:8])


def build_base_research_terms(payload: ProductViabilityInput) -> list[str]:
    terms = []
    terms.extend(extract_search_terms(payload.product_name, limit=3))
    terms.extend(extract_search_terms(payload.category, limit=2))
    terms.extend(extract_search_terms(payload.target_customer, limit=2))
    terms.extend(extract_search_terms(payload.price_point, limit=1))
    if not terms:
        terms.extend(extract_search_terms(payload.natural_language_input, limit=4))
    if not terms:
        terms.extend(extract_search_terms(payload.description, limit=4))
    return dedupe_terms(terms)


def build_suffix_terms(query_suffix: str) -> list[str]:
    return dedupe_terms(extract_search_terms(query_suffix, limit=4))


def extract_search_terms(value: str | None, limit: int) -> list[str]:
    normalized = normalize_match_text(value)
    if not normalized:
        return []
    candidates = []
    for token in normalized.split():
        if token in SEARCH_STOP_WORDS:
            continue
        if len(token) < 3 and not token.isdigit():
            continue
        candidates.append(token)
        if len(candidates) >= limit:
            break
    return candidates


def dedupe_terms(values: list[str]) -> list[str]:
    deduped = []
    seen = set()
    for value in values:
        normalized = normalize_text(value).lower()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def build_research_goal_prompt(payload: ProductViabilityInput, lane: str, goal: str) -> str:
    details = {
        "natural_language_input": payload.natural_language_input or None,
        "product_name": payload.product_name or None,
        "description": payload.description or None,
        "category": payload.category or None,
        "price_point": payload.price_point or None,
        "target_customer": payload.target_customer or None,
        "market_context": payload.market_context or None,
        "research_depth": payload.research_depth,
        "lane": lane,
    }
    return f"{goal}\n\nSubmitted product context:\n{details}"


def build_live_market_research_schema() -> dict:
    return {
        "type": "object",
        "required": [
            "captured_at",
            "summary",
            "competitors",
            "pricing_landscape",
            "demand_signals",
            "risks",
            "source_citations",
        ],
        "properties": {
            "captured_at": {"type": "string"},
            "summary": {"type": "string"},
            "competitors": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["name", "url", "summary"],
                    "properties": {
                        "name": {"type": "string"},
                        "url": {"type": "string"},
                        "summary": {"type": "string"},
                        "price_point": {"type": "string"},
                    },
                },
            },
            "pricing_landscape": {"type": "array", "items": {"type": "string"}},
            "demand_signals": {"type": "array", "items": {"type": "string"}},
            "risks": {"type": "array", "items": {"type": "string"}},
            "source_citations": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["title", "url"],
                    "properties": {
                        "title": {"type": "string"},
                        "url": {"type": "string"},
                        "excerpt": {"type": "string"},
                    },
                },
            },
        },
    }


def aggregate_live_market_research(lane_results: list[dict]) -> dict:
    lane_reports = []
    competitors = []
    pricing_landscape = []
    demand_signals = []
    risks = []
    source_citations = []
    summaries = []
    lane_statuses = []

    completed_lanes = 0
    partial_lanes = 0
    pending_lanes = 0

    for lane_result in lane_results:
        normalized_result = normalize_live_market_research_result(lane_result.get("result"))
        lane_status = determine_live_research_lane_status(lane_result["status"], normalized_result)
        lane_statuses.append({"lane": lane_result["lane"], "status": lane_status})

        if lane_status == "completed":
            completed_lanes += 1
        elif lane_status == "partial":
            partial_lanes += 1
        elif lane_status == "pending":
            pending_lanes += 1

        if lane_status != "failed":
            if normalized_result["summary"]:
                summaries.append(normalized_result["summary"])
            competitors.extend(normalized_result["competitors"])
            pricing_landscape.extend(normalized_result["pricing_landscape"])
            demand_signals.extend(normalized_result["demand_signals"])
            risks.extend(normalized_result["risks"])
            source_citations.extend(normalized_result["source_citations"])

        lane_reports.append(
            {
                "lane": lane_result["lane"],
                "status": lane_status,
                "summary": normalized_result["summary"] or None,
                "competitors_count": len(normalized_result["competitors"]),
                "pricing_points_count": len(normalized_result["pricing_landscape"]),
                "demand_signals_count": len(normalized_result["demand_signals"]),
                "risks_count": len(normalized_result["risks"]),
                "citations_count": len(normalized_result["source_citations"]),
                "error": lane_result.get("error"),
            }
        )

    if completed_lanes == 0 and partial_lanes == 0 and pending_lanes > 0:
        status = "pending"
    elif completed_lanes == 0 and partial_lanes == 0:
        status = "failed"
    elif pending_lanes > 0 or partial_lanes > 0 or completed_lanes < len(lane_results):
        status = "partial"
    else:
        status = "completed"

    deduped_competitors = dedupe_competitors(competitors)
    deduped_pricing = dedupe_strings(pricing_landscape)
    deduped_demand = dedupe_strings(demand_signals)
    deduped_risks = dedupe_strings(risks)
    deduped_citations = dedupe_citations(source_citations)
    summary = build_live_research_summary(summaries, deduped_competitors, deduped_pricing, deduped_demand)

    return {
        "status": status,
        "lane_statuses": lane_statuses,
        "summary": summary,
        "competitors": deduped_competitors,
        "pricing_landscape": deduped_pricing,
        "demand_signals": deduped_demand,
        "risks": deduped_risks,
        "source_citations": deduped_citations,
        "lane_reports": lane_reports,
    }


def extract_live_research_error(live_market_research: dict) -> dict | None:
    lane_reports = live_market_research.get("lane_reports")
    if not isinstance(lane_reports, list):
        return None
    for lane_report in lane_reports:
        if not isinstance(lane_report, dict):
            continue
        error = lane_report.get("error")
        if not isinstance(error, dict):
            continue
        code = normalize_text(error.get("code")) or "tinyfish_research_unavailable"
        message = normalize_text(error.get("message")) or "TinyFish research is unavailable for this request."
        payload = {"code": code, "message": message}
        if "details" in error:
            payload["details"] = error["details"]
        return payload
    return None


def determine_live_research_lane_status(raw_status: str, normalized_result: dict) -> str:
    normalized_status = str(raw_status or "").upper()
    has_evidence = bool(
        normalized_result["summary"]
        or normalized_result["competitors"]
        or normalized_result["pricing_landscape"]
        or normalized_result["demand_signals"]
        or normalized_result["risks"]
        or normalized_result["source_citations"]
    )
    if normalized_status == "COMPLETED":
        return "completed"
    if normalized_status in {"PENDING", "QUEUED", "RUNNING", "STARTING", "IN_PROGRESS"}:
        if has_evidence:
            return "partial"
        return "pending"
    if has_evidence:
        return "partial"
    return "failed"


def normalize_live_market_research_result(result: dict | None) -> dict:
    if not isinstance(result, dict):
        return {
            "summary": "",
            "competitors": [],
            "pricing_landscape": [],
            "demand_signals": [],
            "risks": [],
            "source_citations": [],
        }
    return {
        "summary": normalize_text(result.get("summary")),
        "competitors": normalize_competitors(result.get("competitors")),
        "pricing_landscape": clean_string_list(result.get("pricing_landscape")),
        "demand_signals": clean_string_list(result.get("demand_signals")),
        "risks": clean_string_list(result.get("risks")),
        "source_citations": normalize_citations(result.get("source_citations")),
    }


def normalize_competitors(values) -> list[dict]:
    if not isinstance(values, list):
        return []
    competitors = []
    for value in values:
        if not isinstance(value, dict):
            continue
        name = normalize_text(value.get("name"))
        url = normalize_text(value.get("url"))
        summary = normalize_text(value.get("summary"))
        if not name or not url or not summary:
            continue
        competitors.append(
            {
                "name": name,
                "url": url,
                "summary": summary,
                "price_point": normalize_text(value.get("price_point")) or None,
            }
        )
    return competitors[:8]


def normalize_citations(values) -> list[dict]:
    if not isinstance(values, list):
        return []
    citations = []
    for value in values:
        if not isinstance(value, dict):
            continue
        title = normalize_text(value.get("title"))
        url = normalize_text(value.get("url"))
        if not title or not url:
            continue
        citations.append(
            {
                "title": title,
                "url": url,
                "excerpt": normalize_text(value.get("excerpt")) or None,
            }
        )
    return citations[:10]


def dedupe_competitors(values: list[dict]) -> list[dict]:
    deduped = []
    seen = set()
    for value in values:
        key = (value["name"].lower(), value["url"].lower())
        if key in seen:
            continue
        seen.add(key)
        deduped.append(value)
    return deduped[:8]


def dedupe_citations(values: list[dict]) -> list[dict]:
    deduped = []
    seen = set()
    for value in values:
        key = value["url"].lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(value)
    return deduped[:10]


def dedupe_strings(values: list[str]) -> list[str]:
    deduped = []
    seen = set()
    for value in values:
        normalized = normalize_text(value)
        if not normalized:
            continue
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(normalized)
    return deduped[:8]


def build_live_research_summary(
    summaries: list[str],
    competitors: list[dict],
    pricing_landscape: list[str],
    demand_signals: list[str],
) -> str:
    if summaries:
        return " ".join(summary for summary in summaries if summary)[:500]
    return (
        f"Collected {len(competitors)} competitor findings, "
        f"{len(pricing_landscape)} pricing signals, and "
        f"{len(demand_signals)} demand signals."
    )


def build_product_viability_enrichment(settings: Settings, payload: ProductViabilityInput) -> dict:
    try:
        sources = load_source_catalog(settings)
        snapshots = load_snapshots(settings)
        search_text = normalize_match_text(
            payload.natural_language_input,
            payload.product_name,
            payload.description,
            payload.category,
            payload.price_point,
            payload.target_customer,
            payload.market_context,
        )
        search_tokens = set(search_text.split())
        if not search_tokens and not search_text:
            return empty_enrichment_context()

        company_catalog = build_company_catalog(sources, snapshots)
        product_catalog = build_product_catalog(sources, snapshots)
        matched_companies = [
            company
            for company in company_catalog
            if label_matches(company["name"], search_text, search_tokens)
        ]
        matched_products = [
            product
            for product in product_catalog
            if label_matches(product.get("name"), search_text, search_tokens)
        ]

        growth_events = build_related_growth_events(sources, snapshots, matched_companies, search_text, search_tokens)
        commerce_signals = build_related_commerce_signals(
            sources,
            snapshots,
            matched_companies,
            matched_products,
            search_text,
            search_tokens,
        )

        used_local_context = bool(matched_companies or matched_products or growth_events or commerce_signals)
        return {
            "matched_entities": {
                "companies": [
                    {
                        "company_id": company["id"],
                        "company_name": company["name"],
                        "tracked_categories": company["tracked_categories"],
                    }
                    for company in matched_companies[:5]
                ],
                "products": [
                    {
                        "product_id": product["id"],
                        "product_name": product.get("name"),
                        "company_id": product["company_id"],
                    }
                    for product in matched_products[:5]
                ],
            },
            "related_signal_context": {
                "growth_events_count": len(growth_events),
                "commerce_signals_count": len(commerce_signals),
                "growth_events": growth_events[:5],
                "commerce_signals": commerce_signals[:5],
            },
            "used_local_context": used_local_context,
        }
    except Exception:
        return empty_enrichment_context()


def build_related_growth_events(
    sources: list[dict],
    snapshots: list[dict],
    matched_companies: list[dict],
    search_text: str,
    search_tokens: set[str],
) -> list[dict]:
    growth_sources = build_growth_sources(sources)
    growth_snapshots = build_growth_snapshots(growth_sources, snapshots)
    growth_comparisons = build_growth_comparisons(growth_sources, growth_snapshots)
    growth_events = build_growth_events(growth_sources, growth_snapshots, growth_comparisons)

    matched_company_ids = {company["id"] for company in matched_companies}
    related_events = []
    for event in growth_events:
        if event["company_id"] in matched_company_ids or label_matches(event["company_name"], search_text, search_tokens):
            related_events.append(
                {
                    "id": event["id"],
                    "signal_type": event["signal_type"],
                    "severity": event["severity"],
                    "company_id": event["company_id"],
                    "company_name": event["company_name"],
                    "title": event["title"],
                    "summary": event["summary"],
                    "timestamp": event["timestamp"],
                }
            )
    return related_events


def build_related_commerce_signals(
    sources: list[dict],
    snapshots: list[dict],
    matched_companies: list[dict],
    matched_products: list[dict],
    search_text: str,
    search_tokens: set[str],
) -> list[dict]:
    commerce_sources = build_commerce_sources(sources)
    commerce_snapshots = build_commerce_snapshots(commerce_sources, snapshots)
    current_listings = build_current_listings(commerce_snapshots)
    commerce_comparisons = build_commerce_comparisons(commerce_snapshots)
    commerce_signals = build_commerce_signals(
        commerce_sources,
        commerce_snapshots,
        commerce_comparisons,
        current_listings,
    )

    matched_company_ids = {company["id"] for company in matched_companies}
    matched_product_ids = {product["id"] for product in matched_products}
    related_signals = []
    for signal in commerce_signals:
        product_name = signal.get("product_name")
        if (
            signal["company_id"] in matched_company_ids
            or signal.get("product_id") in matched_product_ids
            or label_matches(product_name, search_text, search_tokens)
            or label_matches(signal.get("competitor_name"), search_text, search_tokens)
        ):
            related_signals.append(
                {
                    "id": signal["id"],
                    "signal_type": signal["signal_type"],
                    "severity": signal["severity"],
                    "company_id": signal["company_id"],
                    "company_name": signal["company_name"],
                    "product_name": product_name,
                    "marketplace": signal["marketplace"],
                    "timestamp": signal["timestamp"],
                    "summary": build_commerce_signal_summary(signal),
                }
            )
    return related_signals


def empty_enrichment_context() -> dict:
    return {
        "matched_entities": {
            "companies": [],
            "products": [],
        },
        "related_signal_context": {
            "growth_events_count": 0,
            "commerce_signals_count": 0,
            "growth_events": [],
            "commerce_signals": [],
        },
        "used_local_context": False,
    }


def label_matches(raw_label: str | None, search_text: str, search_tokens: set[str]) -> bool:
    if not raw_label:
        return False
    normalized_label = normalize_match_text(raw_label)
    if not normalized_label:
        return False
    if normalized_label in search_text:
        return True
    label_tokens = {token for token in normalized_label.split() if len(token) >= 3}
    if not label_tokens:
        return False
    return len(label_tokens & search_tokens) >= min(2, len(label_tokens))


def normalize_match_text(*values: str | None) -> str:
    normalized = " ".join(normalize_text(value) for value in values if normalize_text(value))
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized.lower())
    return " ".join(normalized.split())


def normalize_text(value: str | None) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.strip().split())


def clean_string_list(values) -> list[str]:
    if not isinstance(values, list):
        return []
    cleaned = []
    for value in values:
        normalized = normalize_text(value)
        if normalized:
            cleaned.append(normalized)
    return cleaned[:8]


def build_commerce_signal_summary(signal: dict) -> str:
    current_value = signal.get("current_value")
    if signal["signal_type"] in {"price_drop", "price_increase"} and isinstance(current_value, dict):
        price = current_value.get("price")
        currency = current_value.get("currency") or ""
        if isinstance(price, (int, float)):
            return f"{signal['signal_type'].replace('_', ' ').title()} detected at {price} {currency}".strip()
    if signal["signal_type"] == "flash_sale" and isinstance(current_value, dict):
        discount = current_value.get("discount_percent")
        if isinstance(discount, (int, float)):
            return f"Flash sale detected with {discount}% discount."
    if signal["signal_type"] == "inventory_shift" and isinstance(current_value, dict):
        stock_status = current_value.get("stock_status")
        if isinstance(stock_status, str):
            return f"Inventory shifted to {stock_status.replace('_', ' ')}."
    return f"{signal['signal_type'].replace('_', ' ').title()} signal observed."
