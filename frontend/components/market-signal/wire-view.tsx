"use client"

import { useEffect, useMemo, useState } from "react"
import {
  Activity,
  Clock3,
  ExternalLink,
  Loader2,
  RefreshCw,
  ScanSearch,
  Tags,
} from "lucide-react"

import { fetchAppApi } from "@/lib/fetch-app"

type MarketTopic = "all" | "tech" | "finance"

type WireItem = {
  id: string
  title: string
  summary: string
  category: string
  signal_type: string
  severity: "critical" | "high" | "medium" | "low" | string
  wire_level: "high" | "elevated" | "watch" | string
  timestamp: string
  relative_time_label?: string | null
  company_name?: string | null
  source_name?: string | null
  source_names?: string[]
  source_ids?: string[]
  market_category?: string | null
  market_categories?: string[]
  lifecycle_state: string
  locations: string[]
  tags?: string[]
  detail_url?: string | null
  history_count?: number
  first_seen_at?: string | null
  previous_seen_at?: string | null
  evidence?: Array<{
    label?: string | null
    url?: string | null
  }>
  provenance?: {
    evidence_urls?: string[]
    snapshot_ids?: string[]
    run_ids?: string[]
  }
}

type SourceHealth = {
  source_id: string
  source_name?: string | null
  status: string
  last_run_at?: string | null
  last_snapshot_at?: string | null
  unchanged_runs?: number
  runs_total?: number
  snapshots_total?: number
  schedule?: {
    interval_minutes?: number
  } | null
  last_error?: {
    code?: string | null
    message?: string | null
  } | null
}

type Stat = {
  id: string
  label: string
  value: number
}

type MarketSignalsResponse = {
  meta: {
    generated_at?: string
    latest_snapshot_at?: string | null
    schedule_interval_minutes?: number | null
    filters?: {
      market_category?: string
      limit?: number
    }
    memory?: {
      snapshot_strategy?: string
      source_run_strategy?: string
      signal_deduplication?: string
      snapshot_count?: number
    }
  }
  summary: {
    active_count: number
  }
  wire: {
    stats: Stat[]
    items: WireItem[]
  }
  source_health: SourceHealth[]
}

type TopicOption = {
  id: MarketTopic
  label: string
}

const topicOptions: TopicOption[] = [
  { id: "all", label: "All Signals" },
  { id: "tech", label: "Tech" },
  { id: "finance", label: "Finance" },
]

const severityStyles: Record<string, { text: string; bg: string; border: string }> = {
  critical: { text: "#ff8a8a", bg: "rgba(255,90,90,0.12)", border: "rgba(255,90,90,0.3)" },
  high: { text: "#f5d96e", bg: "rgba(245,217,110,0.12)", border: "rgba(245,217,110,0.26)" },
  medium: { text: "#9eff7a", bg: "rgba(158,255,122,0.12)", border: "rgba(158,255,122,0.24)" },
  low: { text: "#79e8ff", bg: "rgba(121,232,255,0.12)", border: "rgba(121,232,255,0.24)" },
}

function getSeverityStyle(severity: string) {
  return severityStyles[severity] || severityStyles.medium
}

function formatTimestamp(value?: string | null) {
  if (!value) return "No timestamp"
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return new Intl.DateTimeFormat("en-US", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(date)
}

function formatRelativeTime(value?: string | null) {
  if (!value) return "No timestamp"
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  const diffMinutes = Math.round((date.getTime() - Date.now()) / 60000)
  const formatter = new Intl.RelativeTimeFormat("en", { numeric: "auto" })
  const absoluteMinutes = Math.abs(diffMinutes)
  if (absoluteMinutes < 60) return formatter.format(diffMinutes, "minute")
  const diffHours = Math.round(diffMinutes / 60)
  if (Math.abs(diffHours) < 24) return formatter.format(diffHours, "hour")
  const diffDays = Math.round(diffHours / 24)
  return formatter.format(diffDays, "day")
}

function titleCase(value: string) {
  return value
    .split("_")
    .join(" ")
    .replace(/\b\w/g, (match) => match.toUpperCase())
}

async function fetchMarketSignals(topic: MarketTopic, refresh: boolean) {
  const params = new URLSearchParams()
  params.set("limit", "25")
  if (topic !== "all") params.set("market_category", topic)
  if (refresh) params.set("refresh", "true")
  const response = await fetchAppApi(`/api/market-signals?${params.toString()}`, {
    cache: "no-store",
  })
  const payload = (await response.json()) as MarketSignalsResponse | { error?: { message?: string } }
  if (!response.ok) {
    const message = "error" in payload ? payload.error?.message : "Unable to load market signals."
    throw new Error(message || "Unable to load market signals.")
  }
  return payload as MarketSignalsResponse
}

export function WireView() {
  const [topic, setTopic] = useState<MarketTopic>("all")
  const [data, setData] = useState<MarketSignalsResponse | null>(null)
  const [selectedSignalId, setSelectedSignalId] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let active = true

    const load = async () => {
      setLoading(true)
      setError(null)
      try {
        const payload = await fetchMarketSignals(topic, false)
        if (!active) return
        setData(payload)
        setSelectedSignalId((current) => current || payload.wire.items[0]?.id || null)
      } catch (err) {
        if (!active) return
        setError(err instanceof Error ? err.message : "Unable to load market signals.")
      } finally {
        if (active) setLoading(false)
      }
    }

    load()

    return () => {
      active = false
    }
  }, [topic])

  const handleRefresh = async () => {
    setRefreshing(true)
    setError(null)
    try {
      const payload = await fetchMarketSignals(topic, true)
      setData(payload)
      setSelectedSignalId((current) => current || payload.wire.items[0]?.id || null)
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to refresh market signals.")
    } finally {
      setRefreshing(false)
    }
  }

  const selectedSignal = useMemo(
    () => data?.wire.items.find((item) => item.id === selectedSignalId) || data?.wire.items[0] || null,
    [data, selectedSignalId]
  )

  return (
    <div className="absolute inset-0 overflow-hidden bg-[radial-gradient(circle_at_top,#101614_0%,#0a0d0c_36%,#060707_100%)] pt-14">
      <div
        className="absolute inset-0 opacity-[0.08]"
        style={{
          backgroundImage:
            "linear-gradient(rgba(255,255,255,0.06) 1px, transparent 1px), linear-gradient(90deg, rgba(255,255,255,0.05) 1px, transparent 1px)",
          backgroundSize: "42px 42px",
        }}
      />

      <div className="relative flex h-full flex-col">
        <div className="border-b border-white/8 bg-[#090b0b]/80 px-5 py-4 backdrop-blur-md md:px-6">
          <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
            <div>
              <div className="flex items-center gap-2 text-[10px] uppercase tracking-[0.24em] text-[#9ab2a8]">
                <ScanSearch className="h-3.5 w-3.5" />
                Market Signal Wire
              </div>
              <h1 className="mt-2 text-xl font-semibold tracking-tight text-[#f4f7f5]">Live market intelligence feed</h1>
              <p className="mt-1 text-[12px] text-[#90a39b]">
                Persistent TinyFish memory, deduplicated snapshots, and the latest wire items from the market-signals backend.
              </p>
            </div>

            <div className="flex flex-wrap items-center gap-3">
              <div className="flex items-center gap-2 rounded-full border border-white/8 bg-white/[0.03] p-1">
                {topicOptions.map((option) => (
                  <button
                    key={option.id}
                    type="button"
                    onClick={() => setTopic(option.id)}
                    className={`rounded-full px-3 py-1.5 text-[11px] tracking-wide transition ${
                      topic === option.id
                        ? "bg-[#f4f7f5] text-[#0a0d0c]"
                        : "text-[#aabbb3] hover:text-[#f4f7f5]"
                    }`}
                  >
                    {option.label}
                  </button>
                ))}
              </div>

              <button
                type="button"
                onClick={handleRefresh}
                className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/[0.04] px-4 py-2 text-[11px] uppercase tracking-[0.18em] text-[#d8e3dd] transition hover:bg-white/[0.08]"
              >
                {refreshing ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RefreshCw className="h-3.5 w-3.5" />}
                Refresh
              </button>
            </div>
          </div>

          <div className="mt-4 grid gap-3 md:grid-cols-4">
            {(data?.wire.stats || []).map((stat) => (
              <div key={stat.id} className="rounded-2xl border border-white/8 bg-white/[0.03] px-4 py-3">
                <div className="text-[10px] uppercase tracking-[0.22em] text-[#82988f]">{stat.label}</div>
                <div className="mt-2 text-2xl font-semibold text-[#f4f7f5]">{stat.value}</div>
              </div>
            ))}
          </div>
        </div>

        <div className="grid min-h-0 flex-1 gap-0 lg:grid-cols-[minmax(0,1.55fr)_minmax(22rem,0.95fr)]">
          <section className="min-h-0 overflow-y-auto border-r border-white/8 px-5 py-5 md:px-6">
            {loading ? (
              <div className="flex h-full items-center justify-center">
                <div className="inline-flex items-center gap-3 rounded-full border border-white/10 bg-white/[0.04] px-4 py-3 text-[12px] uppercase tracking-[0.2em] text-[#d8e3dd]">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Loading wire
                </div>
              </div>
            ) : data?.wire.items.length ? (
              <div className="space-y-4">
                {data.wire.items.map((item) => {
                  const severity = getSeverityStyle(item.severity)
                  const selected = selectedSignal?.id === item.id
                  const evidenceUrl = item.evidence?.[0]?.url || item.provenance?.evidence_urls?.[0]
                  return (
                    <article
                      key={item.id}
                      className={`rounded-3xl border px-5 py-5 transition ${
                        selected
                          ? "border-white/18 bg-white/[0.05] shadow-[0_18px_60px_rgba(0,0,0,0.22)]"
                          : "border-white/8 bg-white/[0.025] hover:border-white/12 hover:bg-white/[0.04]"
                      }`}
                    >
                      <button type="button" onClick={() => setSelectedSignalId(item.id)} className="block w-full text-left">
                        <div className="flex flex-wrap items-center justify-between gap-3">
                          <div className="flex flex-wrap items-center gap-2">
                            <span
                              className="rounded-full border px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.22em]"
                              style={{
                                color: severity.text,
                                backgroundColor: severity.bg,
                                borderColor: severity.border,
                              }}
                            >
                              {item.severity}
                            </span>
                            <span className="rounded-full border border-white/8 bg-white/[0.03] px-2.5 py-1 text-[10px] uppercase tracking-[0.18em] text-[#b6c4bf]">
                              {item.market_category || item.market_categories?.[0] || "market"}
                            </span>
                            <span className="rounded-full border border-white/8 bg-white/[0.03] px-2.5 py-1 text-[10px] uppercase tracking-[0.18em] text-[#859a92]">
                              {titleCase(item.lifecycle_state)}
                            </span>
                          </div>
                          <div className="flex items-center gap-2 text-[10px] uppercase tracking-[0.18em] text-[#83978f]">
                            <Clock3 className="h-3.5 w-3.5" />
                            {item.relative_time_label || formatRelativeTime(item.timestamp)}
                          </div>
                        </div>

                        <div className="mt-4">
                          <h2 className="text-[15px] font-semibold leading-6 tracking-[0.01em] text-[#f4f7f5]">
                            {item.title}
                          </h2>
                          <p className="mt-2 text-[13px] leading-6 text-[#a5b7b0]">{item.summary}</p>
                        </div>

                        <div className="mt-4 flex flex-wrap items-center gap-2 text-[11px] text-[#90a39b]">
                          {item.company_name ? <span>{item.company_name}</span> : null}
                          {item.source_name ? <span>{item.source_name}</span> : null}
                          {item.locations.slice(0, 2).map((location) => (
                            <span key={location} className="rounded-full border border-white/8 px-2 py-1 text-[10px] text-[#b6c4bf]">
                              {location}
                            </span>
                          ))}
                        </div>

                        <div className="mt-4 flex flex-wrap items-center gap-2">
                          {(item.tags || []).slice(0, 4).map((tag) => (
                            <span
                              key={`${item.id}-${tag}`}
                              className="inline-flex items-center gap-1 rounded-full border border-white/8 bg-white/[0.03] px-2.5 py-1 text-[10px] uppercase tracking-[0.16em] text-[#7f958c]"
                            >
                              <Tags className="h-3 w-3" />
                              {tag}
                            </span>
                          ))}
                        </div>
                      </button>

                      <div className="mt-4 flex flex-wrap items-center justify-between gap-3 border-t border-white/8 pt-4 text-[11px] text-[#8ea29a]">
                        <div className="flex flex-wrap items-center gap-4">
                          <span>Seen {item.history_count || 1} time{(item.history_count || 1) === 1 ? "" : "s"}</span>
                          <span>First seen {formatTimestamp(item.first_seen_at || item.timestamp)}</span>
                        </div>
                        {evidenceUrl ? (
                          <a
                            href={evidenceUrl}
                            target="_blank"
                            rel="noreferrer"
                            className="inline-flex items-center gap-1 text-[#dbe5df] transition hover:text-white"
                          >
                            Evidence
                            <ExternalLink className="h-3.5 w-3.5" />
                          </a>
                        ) : null}
                      </div>
                    </article>
                  )
                })}
              </div>
            ) : (
              <div className="rounded-3xl border border-white/8 bg-white/[0.025] px-6 py-8 text-[13px] text-[#95a79f]">
                No live market signals are available right now. The watcher memory is still persistent, so previous runs remain stored even when the active wire is empty.
              </div>
            )}
          </section>

          <aside className="min-h-0 overflow-y-auto px-5 py-5 md:px-6">
            <div className="space-y-5">
              <section className="rounded-3xl border border-white/8 bg-white/[0.03] p-5">
                <div className="flex items-center gap-2 text-[10px] uppercase tracking-[0.22em] text-[#9ab2a8]">
                  <Activity className="h-3.5 w-3.5" />
                  Selected Signal
                </div>
                {selectedSignal ? (
                  <div className="mt-4 space-y-4">
                    <div>
                      <div className="text-[15px] font-semibold leading-6 text-[#f4f7f5]">{selectedSignal.title}</div>
                      <div className="mt-2 text-[12px] leading-6 text-[#a5b7b0]">{selectedSignal.summary}</div>
                    </div>

                    <div className="grid gap-3 text-[11px] text-[#90a39b] sm:grid-cols-2">
                      <div className="rounded-2xl border border-white/8 bg-[#0d1110] px-3 py-3">
                        <div className="text-[10px] uppercase tracking-[0.18em] text-[#7f948c]">Signal Type</div>
                        <div className="mt-1 text-[#eef5f1]">{titleCase(selectedSignal.signal_type)}</div>
                      </div>
                      <div className="rounded-2xl border border-white/8 bg-[#0d1110] px-3 py-3">
                        <div className="text-[10px] uppercase tracking-[0.18em] text-[#7f948c]">Lifecycle</div>
                        <div className="mt-1 text-[#eef5f1]">{titleCase(selectedSignal.lifecycle_state)}</div>
                      </div>
                      <div className="rounded-2xl border border-white/8 bg-[#0d1110] px-3 py-3">
                        <div className="text-[10px] uppercase tracking-[0.18em] text-[#7f948c]">First Seen</div>
                        <div className="mt-1 text-[#eef5f1]">{formatTimestamp(selectedSignal.first_seen_at || selectedSignal.timestamp)}</div>
                      </div>
                      <div className="rounded-2xl border border-white/8 bg-[#0d1110] px-3 py-3">
                        <div className="text-[10px] uppercase tracking-[0.18em] text-[#7f948c]">Previous Seen</div>
                        <div className="mt-1 text-[#eef5f1]">{formatTimestamp(selectedSignal.previous_seen_at)}</div>
                      </div>
                    </div>

                    <div className="rounded-2xl border border-white/8 bg-[#0d1110] px-4 py-4 text-[11px] text-[#a5b7b0]">
                      <div className="text-[10px] uppercase tracking-[0.18em] text-[#7f948c]">Provenance</div>
                      <div className="mt-2">Snapshots: {(selectedSignal.provenance?.snapshot_ids || []).length}</div>
                      <div className="mt-1">TinyFish runs: {(selectedSignal.provenance?.run_ids || []).length}</div>
                      <div className="mt-1">Evidence URLs: {(selectedSignal.provenance?.evidence_urls || []).length}</div>
                    </div>
                  </div>
                ) : (
                  <div className="mt-4 text-[12px] text-[#95a79f]">Select a signal to inspect its history and provenance.</div>
                )}
              </section>
            </div>
          </aside>
        </div>
      </div>

      {error ? (
        <div className="absolute inset-x-0 top-16 z-40 mx-auto w-[min(100vw-2rem,42rem)] rounded-2xl border border-[#6b342f] bg-[#1a0f0d]/92 px-5 py-4 text-[12px] text-[#f0d0c8] backdrop-blur-md">
          {error}
        </div>
      ) : null}
    </div>
  )
}
