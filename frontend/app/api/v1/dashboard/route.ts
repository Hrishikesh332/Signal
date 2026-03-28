import { NextRequest, NextResponse } from "next/server"
import {
  deriveEventCoordinates,
  resolveLocationLabel,
} from "@/lib/location-resolver"
import type {
  Alert,
  DashboardEvent,
  DashboardResponse,
  KPI,
  MapPoint,
  SourceHealth,
} from "@/lib/dashboard-types"

type Severity = DashboardEvent["severity"]

interface BackendMeta {
  generated_at?: string
  latest_snapshot_at?: string
  source_count?: number
  snapshot_count?: number
  integrations?: Record<string, { provider?: string; configured?: boolean }>
}

interface BackendMapPoint {
  latitude?: number
  longitude?: number
  severity?: Severity
  entity_name?: string
  explanation?: string
  location_label?: string
}

interface BackendEvent {
  id?: string
  severity?: Severity
  category?: string
  company?: string
  company_name?: string
  timestamp?: string
  headline?: string
  title?: string
  explanation?: string
  summary?: string
  latitude?: number
  longitude?: number
  location_label?: string
  locations?: string[]
}

interface BackendDashboardPayload {
  meta?: BackendMeta
  kpis?: KPI[]
  map?: {
    points?: BackendMapPoint[]
  }
  events?: BackendEvent[]
  alerts?: Alert[]
  source_health?: SourceHealth[]
  growth_intelligence?: DashboardResponse["dashboard"]["growth_intelligence"]
}

function normalizeSeverity(value: string | undefined): Severity {
  if (value === "critical" || value === "high" || value === "medium") {
    return value
  }
  return "low"
}

function normalizeIntegrations(meta: BackendMeta | undefined): string[] {
  const integrationMap = meta?.integrations
  if (!integrationMap || typeof integrationMap !== "object") {
    return ["TinyFish", "OpenAI"]
  }

  const names = Object.values(integrationMap)
    .map((entry) => entry.provider?.trim())
    .filter((value): value is string => Boolean(value))

  return names.length > 0 ? names : ["TinyFish", "OpenAI"]
}

function normalizeEvent(event: BackendEvent, index: number): DashboardEvent {
  const normalized: DashboardEvent = {
    id: event.id ?? `event-${index}`,
    severity: normalizeSeverity(event.severity),
    category: event.category ?? "signal",
    company: event.company_name ?? event.company ?? "",
    timestamp: event.timestamp ?? new Date().toISOString(),
    headline: event.title ?? event.headline ?? "Market signal detected",
    explanation: event.summary ?? event.explanation ?? "",
    location_label: event.location_label,
    locations: Array.isArray(event.locations) ? event.locations : [],
    latitude: event.latitude,
    longitude: event.longitude,
  }

  const derived = deriveEventCoordinates(normalized)
  if (derived) {
    normalized.latitude = derived.latitude
    normalized.longitude = derived.longitude
  }

  return normalized
}

function normalizeMapPoint(point: BackendMapPoint, index: number): MapPoint | null {
  if (typeof point.latitude === "number" && typeof point.longitude === "number") {
    return {
      id: `map-point-${index}`,
      latitude: point.latitude,
      longitude: point.longitude,
      severity: normalizeSeverity(point.severity),
      entity_name: point.entity_name ?? `Signal ${index + 1}`,
      explanation: point.explanation ?? "",
    }
  }

  const derived = resolveLocationLabel(point.location_label)
  if (!derived) return null

  return {
    id: `map-point-${index}`,
    location_label: point.location_label,
    latitude: derived.latitude,
    longitude: derived.longitude,
    severity: normalizeSeverity(point.severity),
    entity_name: point.entity_name ?? `Signal ${index + 1}`,
    explanation: point.explanation ?? "",
  }
}

function normalizeSourceHealth(sources: SourceHealth[] | undefined): SourceHealth[] {
  if (!Array.isArray(sources)) return []
  return sources
}

function buildDemoResponse(now: string): DashboardResponse {
  const demoEventsInput: BackendEvent[] = [
    {
      id: "demo-1",
      severity: "high",
      category: "commerce",
      company: "TinyFish",
      timestamp: now,
      headline: "Undercutting pressure detected in Germany",
      explanation: "Competitive pricing movement surfaced from the latest marketplace scan.",
      location_label: "Germany",
      locations: ["Germany"],
    },
    {
      id: "demo-2",
      severity: "medium",
      category: "growth",
      company: "OpenAI",
      timestamp: now,
      headline: "Hiring activity picked up in Tokyo",
      explanation: "Fresh role clusters suggest expanded model operations coverage in Japan.",
      location_label: "Tokyo, Japan",
      locations: ["Tokyo, Japan"],
    },
    {
      id: "demo-3",
      severity: "critical",
      category: "risk",
      company: "Macro Desk",
      timestamp: now,
      headline: "Shipping corridor disruption flagged near Egypt",
      explanation: "Regional logistics risk is elevated around Suez-adjacent routes.",
      location_label: "Egypt",
      locations: ["Egypt"],
    },
    {
      id: "demo-4",
      severity: "low",
      category: "policy",
      company: "US Watch",
      timestamp: now,
      headline: "Regulatory chatter surfaced in Washington D.C.",
      explanation: "Monitoring detected a low-severity policy signal tied to ongoing agency updates.",
      location_label: "Washington D.C., USA",
      locations: ["Washington D.C., USA"],
    },
  ]

  const demoEvents: DashboardEvent[] = demoEventsInput.map((event, index) =>
    normalizeEvent(event, index)
  )

  const demoMapPoints: MapPoint[] = demoEvents
    .filter(
      (event): event is DashboardEvent & { latitude: number; longitude: number } =>
        typeof event.latitude === "number" && typeof event.longitude === "number"
    )
    .map((event) => ({
      id: event.id,
      category: event.category,
      company: event.company,
      timestamp: event.timestamp,
      location_label: event.location_label,
      latitude: event.latitude,
      longitude: event.longitude,
      severity: event.severity,
      entity_name: event.headline,
      explanation: event.explanation || event.headline,
    }))

  return {
    meta: {
      generated_at: now,
      latest_snapshot_at: now,
      source_count: 2,
      snapshot_count: 4,
      integrations: ["TinyFish", "OpenAI"],
    },
    dashboard: {
      kpis: [
        { id: "active_alerts", label: "Active Alerts", value: 1 },
        { id: "events_last_24h", label: "Events (24h)", value: demoEvents.length },
        { id: "healthy_sources", label: "Healthy Sources", value: 2 },
        { id: "snapshots_stored", label: "Snapshots Stored", value: 4 },
      ],
      map: {
        points: demoMapPoints,
      },
      events: demoEvents,
      alerts: [
        {
          id: "alert-1",
          severity: "critical",
          message: "Suez logistics risk elevated",
          timestamp: now,
        },
      ],
      growth_intelligence: {
        kpis: [
          { id: "growth_events", label: "Growth Events", value: 3 },
          { id: "growth_insights", label: "Growth Insights", value: 2 },
          { id: "tracked_jobs", label: "Tracked Jobs", value: 47 },
          { id: "growth_companies", label: "Growth Companies", value: 2 },
        ],
        strategic_insights: [
          {
            id: "insight-1",
            title: "Japan hiring momentum",
            description: "New AI infrastructure hiring is clustering around Tokyo.",
            category: "talent",
          },
          {
            id: "insight-2",
            title: "German marketplace pressure",
            description: "Commerce signals indicate sustained competitive pricing in central Europe.",
            category: "commerce",
          },
        ],
        company_rollups: [
          { company: "OpenAI", signals: 2, trend: "up", severity: "medium" },
          { company: "TinyFish", signals: 1, trend: "stable", severity: "high" },
        ],
        trend_series: {
          jobs: [
            { date: "2026-03-24", value: 12 },
            { date: "2026-03-25", value: 18 },
            { date: "2026-03-26", value: 24 },
            { date: "2026-03-27", value: 31 },
            { date: "2026-03-28", value: 47 },
          ],
          products: [
            { date: "2026-03-24", value: 3 },
            { date: "2026-03-25", value: 4 },
            { date: "2026-03-26", value: 4 },
            { date: "2026-03-27", value: 5 },
            { date: "2026-03-28", value: 6 },
          ],
          funding: [],
          markets: [
            { date: "2026-03-24", value: 1 },
            { date: "2026-03-25", value: 1 },
            { date: "2026-03-26", value: 2 },
            { date: "2026-03-27", value: 2 },
            { date: "2026-03-28", value: 3 },
          ],
          role_clusters: [
            { date: "2026-03-24", value: 2 },
            { date: "2026-03-25", value: 3 },
            { date: "2026-03-26", value: 4 },
            { date: "2026-03-27", value: 4 },
            { date: "2026-03-28", value: 5 },
          ],
        },
      },
      source_health: [
        {
          provider: "TinyFish",
          status: "healthy",
          last_run_at: now,
          success_rate: 0.98,
          avg_runtime_ms: 842,
          snapshots_total: 4,
        },
        {
          provider: "OpenAI",
          status: "healthy",
          last_run_at: now,
          success_rate: 0.95,
          avg_runtime_ms: 1268,
          snapshots_total: 4,
        },
      ],
    },
  }
}

function normalizeBackendPayload(payload: BackendDashboardPayload): DashboardResponse {
  const now = new Date().toISOString()
  const events = Array.isArray(payload.events)
    ? payload.events.map((event, index) => normalizeEvent(event, index))
    : []

  const mapPoints = Array.isArray(payload.map?.points)
    ? payload.map.points
        .map((point, index) => normalizeMapPoint(point, index))
        .filter((point): point is MapPoint => point !== null)
    : []

  return {
    meta: {
      generated_at: payload.meta?.generated_at ?? now,
      latest_snapshot_at: payload.meta?.latest_snapshot_at ?? payload.meta?.generated_at ?? now,
      source_count: payload.meta?.source_count ?? 0,
      snapshot_count: payload.meta?.snapshot_count ?? 0,
      integrations: normalizeIntegrations(payload.meta),
    },
    dashboard: {
      kpis: Array.isArray(payload.kpis) ? payload.kpis : [],
      map: {
        points: mapPoints,
      },
      events,
      alerts: Array.isArray(payload.alerts) ? payload.alerts : [],
      growth_intelligence: payload.growth_intelligence ?? {
        kpis: [],
        strategic_insights: [],
        company_rollups: [],
        trend_series: {
          jobs: [],
          products: [],
          funding: [],
          markets: [],
          role_clusters: [],
        },
      },
      source_health: normalizeSourceHealth(payload.source_health),
    },
  }
}

async function fetchBackendPayload(refresh: boolean): Promise<BackendDashboardPayload | null> {
  const configuredBase = process.env.MARKET_MONITOR_BACKEND_URL?.trim()
  const candidates = [configuredBase, "http://127.0.0.1:5000", "http://localhost:5000"].filter(
    (value, index, array): value is string => Boolean(value) && array.indexOf(value) === index
  )

  for (const baseUrl of candidates) {
    try {
      const controller = new AbortController()
      const timeout = setTimeout(() => controller.abort(), 1500)
      const response = await fetch(
        `${baseUrl.replace(/\/$/, "")}/api/v1/dashboard${refresh ? "?refresh=true" : ""}`,
        {
          cache: "no-store",
          signal: controller.signal,
        }
      )
      clearTimeout(timeout)

      if (response.ok) {
        return (await response.json()) as BackendDashboardPayload
      }
    } catch {
      // Fall through to the next candidate and then to the local demo payload.
    }
  }

  return null
}

export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url)
  const refresh = searchParams.get("refresh") === "true"

  if (refresh) {
    await new Promise((resolve) => setTimeout(resolve, 300))
  }

  const backendPayload = await fetchBackendPayload(refresh)
  if (backendPayload) {
    return NextResponse.json(normalizeBackendPayload(backendPayload))
  }

  return NextResponse.json(buildDemoResponse(new Date().toISOString()))
}
