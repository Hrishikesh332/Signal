"use client"

import { useMemo, useState } from "react"
import {
  ArrowRightLeft,
  Building2,
  ExternalLink,
  Loader2,
  Radar,
  RefreshCw,
  ScanSearch,
  ShieldAlert,
  Sparkles,
  Target,
  Users2,
} from "lucide-react"

import { fetchAppApi } from "@/lib/fetch-app"

type SummaryCard = {
  id: string
  label: string
  value: number | string
}

type TargetCompany = {
  company_name: string
  homepage_url: string
  summary: string
  market_category?: string | null
  sector: string
  geography: string
  target_customers: string[]
  products: Array<{
    name: string
    category: string
    summary: string
  }>
  differentiators: string[]
  pricing_signals: string[]
  captured_at: string
  provenance: {
    evidence_urls?: string[]
  }
}

type CompetitorSignal = {
  signal_id: string
  title: string
  summary: string
  severity: string
  market_category?: string | null
  timestamp: string
  relative_time_label?: string | null
  source_name?: string | null
  evidence_url?: string | null
}

type Competitor = {
  id: string
  company_name: string
  homepage_url: string
  fit_score?: number | null
  confidence_score?: number | null
  reasoning?: string | null
  overlap_areas: string[]
  strengths: Array<{
    title: string
    reasoning: string
  }>
  pain_points: Array<{
    title: string
    reasoning: string
  }>
  score_breakdown: Record<string, number>
  profile: {
    summary: string
    market_category?: string | null
    sector: string
    geography: string
    target_customers: string[]
    products: Array<{
      name: string
      category: string
      summary: string
    }>
    differentiators: string[]
    pricing_signals: string[]
    captured_at: string
  }
  related_signals: CompetitorSignal[]
  provenance: {
    evidence_urls?: string[]
  }
}

type MarketContext = {
  signal_count: number
  mentioned_companies: string[]
  signals: Array<{
    signal_id: string
    title: string
    summary: string
    signal_type: string
    severity: string
    market_category?: string | null
    timestamp: string
    source_name?: string | null
    mentioned_companies: string[]
    evidence_urls?: string[]
  }>
}

type AnalysisRun = {
  analysis_run_id: string
  role: string
  company_name: string
  source_name: string
  target_url: string
  capture_status?: string | null
  captured_at?: string | null
  snapshot_id?: string | null
  run_id?: string | null
  error?: {
    code?: string | null
    message?: string | null
  } | null
}

type SourceHealth = {
  source_id: string
  source_name?: string | null
  status: string
  last_run_at?: string | null
  last_error?: {
    message?: string | null
  } | null
}

type CompetitorIntelligenceResponse = {
  meta: {
    generated_at?: string
    target_company_name?: string
    verified_competitor_count?: number
    latest_snapshot_at?: string | null
    market_category?: string | null
    integrations?: {
      openai?: {
        model?: string | null
        status?: string | null
      }
    }
  }
  summary_cards: SummaryCard[]
  target_company: TargetCompany
  landscape: {
    summary?: string | null
    confidence_score?: number | null
    competitor_count?: number
    generated?: boolean
  }
  competitors: Competitor[]
  market_context: MarketContext
  analysis_runs: AnalysisRun[]
  source_health: SourceHealth[]
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

function formatScore(value?: number | null) {
  if (typeof value !== "number") return "n/a"
  return `${Math.round(value)}`
}

function formatConfidence(value?: number | null) {
  if (typeof value !== "number") return "n/a"
  return `${Math.round(value * 100)}%`
}

function formatLabel(value: string) {
  return value
    .split("_")
    .join(" ")
    .replace(/\b\w/g, (match) => match.toUpperCase())
}

function getScoreTone(value?: number | null) {
  if (typeof value !== "number") return "text-[#9ca3af]"
  if (value >= 80) return "text-[#e8f7ef]"
  if (value >= 60) return "text-[#f6e9b0]"
  return "text-[#d0d5db]"
}

async function fetchCompetitorIntelligence(companyUrl: string, refresh: boolean) {
  const params = new URLSearchParams()
  params.set("company_url", companyUrl)
  params.set("top_n", "4")
  if (refresh) params.set("refresh", "true")
  const response = await fetchAppApi(`/api/competitor-intelligence?${params.toString()}`, {
    cache: "no-store",
  })
  const payload = (await response.json()) as CompetitorIntelligenceResponse | { error?: { message?: string } }
  if (!response.ok) {
    const message = "error" in payload ? payload.error?.message : "Unable to load competitor intelligence."
    throw new Error(message || "Unable to load competitor intelligence.")
  }
  return payload as CompetitorIntelligenceResponse
}

export function CompetitorView() {
  const [companyUrl, setCompanyUrl] = useState("")
  const [submittedUrl, setSubmittedUrl] = useState("")
  const [data, setData] = useState<CompetitorIntelligenceResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const topSignals = useMemo(() => data?.market_context.signals.slice(0, 6) || [], [data])

  const handleAnalyze = async () => {
    const normalized = companyUrl.trim()
    if (!normalized) {
      setError("Enter a public company URL to analyze competitors.")
      return
    }
    setLoading(true)
    setError(null)
    try {
      const payload = await fetchCompetitorIntelligence(normalized, true)
      setData(payload)
      setSubmittedUrl(normalized)
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load competitor intelligence.")
    } finally {
      setLoading(false)
    }
  }

  const handleRerun = async () => {
    const activeUrl = submittedUrl || companyUrl.trim()
    if (!activeUrl) {
      setError("Enter a public company URL to analyze competitors.")
      return
    }
    setLoading(true)
    setError(null)
    try {
      const payload = await fetchCompetitorIntelligence(activeUrl, true)
      setData(payload)
      setSubmittedUrl(activeUrl)
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to refresh competitor intelligence.")
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="absolute inset-0 overflow-hidden bg-[radial-gradient(circle_at_top,#111717_0%,#0a0d0d_42%,#050606_100%)] pt-14">
      <div
        className="absolute inset-0 opacity-[0.07]"
        style={{
          backgroundImage:
            "linear-gradient(rgba(255,255,255,0.05) 1px, transparent 1px), linear-gradient(90deg, rgba(255,255,255,0.04) 1px, transparent 1px)",
          backgroundSize: "42px 42px",
        }}
      />

      <div className="relative flex h-full flex-col">
        <div className="border-b border-white/8 bg-[#090b0b]/85 px-5 py-4 backdrop-blur-md md:px-6">
          <div className="flex flex-col gap-4 xl:flex-row xl:items-end xl:justify-between">
            <div className="max-w-3xl">
              <div className="flex items-center gap-2 text-[10px] uppercase tracking-[0.24em] text-[#9ab2a8]">
                <ScanSearch className="h-3.5 w-3.5" />
                Competitor Intelligence
              </div>
              <h1 className="mt-2 text-xl font-semibold tracking-tight text-[#f4f7f5]">
                URL-driven competitor discovery with TinyFish and OpenAI
              </h1>
              <p className="mt-1 text-[12px] leading-6 text-[#90a39b]">
                Submit a public company URL. TinyFish extracts the company profile, OpenAI identifies likely competitors,
                then verified competitor sites are analyzed for reasoning, strengths, pain points, and fit scores.
              </p>
            </div>

            <div className="w-full max-w-3xl">
              <div className="flex flex-col gap-3 md:flex-row">
                <div className="flex-1 rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3">
                  <div className="flex items-center gap-3">
                    <Target className="h-4 w-4 text-[#8cb7a7]" />
                    <input
                      type="url"
                      value={companyUrl}
                      onChange={(event) => setCompanyUrl(event.target.value)}
                      placeholder="https://company.com"
                      className="w-full bg-transparent text-[13px] text-[#eef5f1] outline-none placeholder:text-[#677d75]"
                    />
                  </div>
                </div>
                <button
                  type="button"
                  onClick={handleAnalyze}
                  disabled={loading}
                  className="inline-flex items-center justify-center gap-2 rounded-2xl border border-white/10 bg-[#e8f2ec] px-4 py-3 text-[11px] font-semibold uppercase tracking-[0.18em] text-[#08100d] transition hover:bg-white disabled:cursor-not-allowed disabled:opacity-70"
                >
                  {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Radar className="h-4 w-4" />}
                  Analyze
                </button>
                <button
                  type="button"
                  onClick={handleRerun}
                  disabled={loading || (!submittedUrl && !companyUrl.trim())}
                  className="inline-flex items-center justify-center gap-2 rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-3 text-[11px] font-semibold uppercase tracking-[0.18em] text-[#dce7e1] transition hover:bg-white/[0.08] disabled:cursor-not-allowed disabled:opacity-60"
                >
                  <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
                  Re-Run
                </button>
              </div>
              {submittedUrl ? (
                <div className="mt-2 text-[11px] text-[#7f958c]">Last analyzed URL: {submittedUrl}</div>
              ) : null}
            </div>
          </div>

          {data?.summary_cards?.length ? (
            <div className="mt-4 grid gap-3 md:grid-cols-4">
              {data.summary_cards.map((card) => (
                <div key={card.id} className="rounded-2xl border border-white/8 bg-white/[0.03] px-4 py-3">
                  <div className="text-[10px] uppercase tracking-[0.22em] text-[#82988f]">{card.label}</div>
                  <div className="mt-2 text-lg font-semibold text-[#f4f7f5]">{card.value}</div>
                </div>
              ))}
            </div>
          ) : null}
        </div>

        <div className="grid min-h-0 flex-1 gap-0 xl:grid-cols-[minmax(0,1.55fr)_minmax(22rem,0.95fr)]">
          <section className="min-h-0 overflow-y-auto border-r border-white/8 px-5 py-5 md:px-6">
            {!data && !loading ? (
              <div className="rounded-3xl border border-white/8 bg-white/[0.03] px-6 py-8 text-[13px] leading-6 text-[#99aaa3]">
                Start with a public company homepage or product URL. The page will return a verified target profile,
                about four competitor companies, AI reasoning, strengths, pain points, and score breakdowns grounded by
                TinyFish extractions and current market signals.
              </div>
            ) : null}

            {data ? (
              <div className="space-y-5">
                <section className="rounded-3xl border border-white/8 bg-white/[0.03] p-5">
                  <div className="flex items-center gap-2 text-[10px] uppercase tracking-[0.22em] text-[#9ab2a8]">
                    <Building2 className="h-3.5 w-3.5" />
                    Target Company
                  </div>
                  <div className="mt-4 flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                    <div className="max-w-2xl">
                      <div className="text-[18px] font-semibold text-[#f4f7f5]">{data.target_company.company_name}</div>
                      <p className="mt-2 text-[13px] leading-6 text-[#a5b7b0]">{data.target_company.summary}</p>
                    </div>
                    <a
                      href={data.target_company.homepage_url}
                      target="_blank"
                      rel="noreferrer"
                      className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/[0.04] px-3 py-2 text-[11px] uppercase tracking-[0.18em] text-[#dbe5df] transition hover:bg-white/[0.08]"
                    >
                      Open Site
                      <ExternalLink className="h-3.5 w-3.5" />
                    </a>
                  </div>
                  <div className="mt-4 flex flex-wrap gap-2">
                    {[data.target_company.market_category, data.target_company.sector, data.target_company.geography]
                      .filter((value): value is string => Boolean(value))
                      .map((item) => (
                        <span
                          key={item}
                          className="rounded-full border border-white/8 bg-white/[0.03] px-2.5 py-1 text-[10px] uppercase tracking-[0.18em] text-[#b4c3be]"
                        >
                          {item}
                        </span>
                      ))}
                  </div>
                  <div className="mt-4 grid gap-3 lg:grid-cols-2">
                    <div className="rounded-2xl border border-white/8 bg-[#0d1110] px-4 py-4">
                      <div className="text-[10px] uppercase tracking-[0.18em] text-[#7f948c]">Target Customers</div>
                      <div className="mt-2 flex flex-wrap gap-2">
                        {data.target_company.target_customers.length ? (
                          data.target_company.target_customers.map((item) => (
                            <span key={item} className="rounded-full border border-white/8 px-2 py-1 text-[10px] text-[#d7e1dc]">
                              {item}
                            </span>
                          ))
                        ) : (
                          <span className="text-[12px] text-[#95a79f]">No explicit customer segments extracted.</span>
                        )}
                      </div>
                    </div>
                    <div className="rounded-2xl border border-white/8 bg-[#0d1110] px-4 py-4">
                      <div className="text-[10px] uppercase tracking-[0.18em] text-[#7f948c]">Differentiators</div>
                      <div className="mt-2 space-y-2">
                        {data.target_company.differentiators.length ? (
                          data.target_company.differentiators.slice(0, 4).map((item) => (
                            <div key={item} className="text-[12px] leading-5 text-[#d7e1dc]">
                              {item}
                            </div>
                          ))
                        ) : (
                          <div className="text-[12px] text-[#95a79f]">No explicit differentiators extracted.</div>
                        )}
                      </div>
                    </div>
                  </div>
                </section>

                {data.landscape.summary ? (
                  <section className="rounded-3xl border border-[#29443a] bg-[linear-gradient(135deg,rgba(23,44,36,0.8),rgba(11,17,15,0.92))] p-5">
                    <div className="flex items-center gap-2 text-[10px] uppercase tracking-[0.22em] text-[#a6c6bb]">
                      <Sparkles className="h-3.5 w-3.5" />
                      OpenAI Landscape Summary
                    </div>
                    <p className="mt-3 text-[13px] leading-6 text-[#ebf4ef]">{data.landscape.summary}</p>
                    <div className="mt-3 text-[11px] text-[#a6c6bb]">
                      Confidence {formatConfidence(data.landscape.confidence_score)}
                    </div>
                  </section>
                ) : null}

                {data.competitors.length ? (
                  <div className="space-y-4">
                    {data.competitors.map((competitor) => (
                      <article key={competitor.id} className="rounded-3xl border border-white/8 bg-white/[0.03] p-5">
                        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                          <div className="max-w-3xl">
                            <div className="flex flex-wrap items-center gap-3">
                              <div className="text-[17px] font-semibold text-[#f4f7f5]">{competitor.company_name}</div>
                              <span className="rounded-full border border-white/8 bg-white/[0.03] px-2.5 py-1 text-[10px] uppercase tracking-[0.18em] text-[#b0c0ba]">
                                {competitor.profile.market_category || competitor.profile.sector}
                              </span>
                            </div>
                            <div className="mt-2 flex flex-wrap items-center gap-3 text-[11px] text-[#8ea29a]">
                              <span>{competitor.profile.geography}</span>
                              <span>Confidence {formatConfidence(competitor.confidence_score)}</span>
                              <span>Captured {formatTimestamp(competitor.profile.captured_at)}</span>
                            </div>
                            <p className="mt-3 text-[13px] leading-6 text-[#a7b8b1]">{competitor.reasoning}</p>
                          </div>

                          <div className="min-w-[11rem] rounded-3xl border border-white/10 bg-[#0d1110] px-4 py-4">
                            <div className="text-[10px] uppercase tracking-[0.18em] text-[#7f948c]">Fit Score</div>
                            <div className={`mt-2 text-4xl font-semibold ${getScoreTone(competitor.fit_score)}`}>
                              {formatScore(competitor.fit_score)}
                            </div>
                            <div className="mt-2 h-2 overflow-hidden rounded-full bg-white/6">
                              <div
                                className="h-full rounded-full bg-[linear-gradient(90deg,#dfece6,#7cb59e)]"
                                style={{ width: `${competitor.fit_score || 0}%` }}
                              />
                            </div>
                            <a
                              href={competitor.homepage_url}
                              target="_blank"
                              rel="noreferrer"
                              className="mt-4 inline-flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-[#dce7e1] transition hover:text-white"
                            >
                              Open Site
                              <ExternalLink className="h-3.5 w-3.5" />
                            </a>
                          </div>
                        </div>

                        <div className="mt-4 flex flex-wrap gap-2">
                          {competitor.overlap_areas.map((item) => (
                            <span
                              key={`${competitor.id}-${item}`}
                              className="rounded-full border border-white/8 bg-white/[0.03] px-2.5 py-1 text-[10px] uppercase tracking-[0.18em] text-[#b8c7c2]"
                            >
                              {item}
                            </span>
                          ))}
                        </div>

                        <div className="mt-5 grid gap-4 xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_minmax(16rem,0.8fr)]">
                          <div className="rounded-2xl border border-white/8 bg-[#0d1110] px-4 py-4">
                            <div className="text-[10px] uppercase tracking-[0.18em] text-[#7f948c]">Strengths</div>
                            <div className="mt-3 space-y-3">
                              {competitor.strengths.length ? (
                                competitor.strengths.map((item) => (
                                  <div key={`${competitor.id}-${item.title}`} className="rounded-2xl border border-white/6 bg-white/[0.02] px-3 py-3">
                                    <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-[#e7f2ec]">{item.title}</div>
                                    <div className="mt-1 text-[12px] leading-5 text-[#a6b8b1]">{item.reasoning}</div>
                                  </div>
                                ))
                              ) : (
                                <div className="text-[12px] text-[#95a79f]">No verified strengths were returned.</div>
                              )}
                            </div>
                          </div>

                          <div className="rounded-2xl border border-white/8 bg-[#0d1110] px-4 py-4">
                            <div className="text-[10px] uppercase tracking-[0.18em] text-[#7f948c]">Pain Points</div>
                            <div className="mt-3 space-y-3">
                              {competitor.pain_points.length ? (
                                competitor.pain_points.map((item) => (
                                  <div key={`${competitor.id}-${item.title}`} className="rounded-2xl border border-white/6 bg-white/[0.02] px-3 py-3">
                                    <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-[#f3e7de]">{item.title}</div>
                                    <div className="mt-1 text-[12px] leading-5 text-[#b4aaa3]">{item.reasoning}</div>
                                  </div>
                                ))
                              ) : (
                                <div className="text-[12px] text-[#95a79f]">No verified pain points were returned.</div>
                              )}
                            </div>
                          </div>

                          <div className="rounded-2xl border border-white/8 bg-[#0d1110] px-4 py-4">
                            <div className="text-[10px] uppercase tracking-[0.18em] text-[#7f948c]">Score Breakdown</div>
                            <div className="mt-3 space-y-3">
                              {Object.entries(competitor.score_breakdown).length ? (
                                Object.entries(competitor.score_breakdown).map(([key, value]) => (
                                  <div key={`${competitor.id}-${key}`}>
                                    <div className="flex items-center justify-between text-[11px] text-[#d8e3dd]">
                                      <span>{formatLabel(key)}</span>
                                      <span>{value}</span>
                                    </div>
                                    <div className="mt-1 h-2 overflow-hidden rounded-full bg-white/6">
                                      <div
                                        className="h-full rounded-full bg-[linear-gradient(90deg,#dfece6,#7cb59e)]"
                                        style={{ width: `${value}%` }}
                                      />
                                    </div>
                                  </div>
                                ))
                              ) : (
                                <div className="text-[12px] text-[#95a79f]">No score breakdown was returned.</div>
                              )}
                            </div>
                          </div>
                        </div>

                        <div className="mt-5 grid gap-4 xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
                          <div className="rounded-2xl border border-white/8 bg-[#0d1110] px-4 py-4">
                            <div className="text-[10px] uppercase tracking-[0.18em] text-[#7f948c]">Competitor Profile</div>
                            <p className="mt-3 text-[12px] leading-6 text-[#a6b8b1]">{competitor.profile.summary}</p>
                            <div className="mt-4 flex flex-wrap gap-2">
                              {competitor.profile.target_customers.slice(0, 5).map((item) => (
                                <span key={`${competitor.id}-${item}`} className="rounded-full border border-white/8 px-2 py-1 text-[10px] text-[#d7e1dc]">
                                  {item}
                                </span>
                              ))}
                            </div>
                            <div className="mt-4 space-y-2">
                              {competitor.profile.products.slice(0, 3).map((product) => (
                                <div key={`${competitor.id}-${product.name}`} className="rounded-2xl border border-white/6 bg-white/[0.02] px-3 py-3">
                                  <div className="text-[11px] font-semibold text-[#eef5f1]">{product.name}</div>
                                  <div className="mt-1 text-[10px] uppercase tracking-[0.16em] text-[#8ca197]">{product.category}</div>
                                  <div className="mt-1 text-[12px] leading-5 text-[#a6b8b1]">{product.summary}</div>
                                </div>
                              ))}
                            </div>
                          </div>

                          <div className="rounded-2xl border border-white/8 bg-[#0d1110] px-4 py-4">
                            <div className="text-[10px] uppercase tracking-[0.18em] text-[#7f948c]">Related Market Signals</div>
                            <div className="mt-3 space-y-3">
                              {competitor.related_signals.length ? (
                                competitor.related_signals.map((signal) => (
                                  <div key={signal.signal_id} className="rounded-2xl border border-white/6 bg-white/[0.02] px-3 py-3">
                                    <div className="flex flex-wrap items-center justify-between gap-2">
                                      <div className="text-[12px] font-medium text-[#eef5f1]">{signal.title}</div>
                                      <span className="text-[10px] uppercase tracking-[0.16em] text-[#9fb2ab]">
                                        {signal.relative_time_label || formatTimestamp(signal.timestamp)}
                                      </span>
                                    </div>
                                    <div className="mt-1 text-[12px] leading-5 text-[#a6b8b1]">{signal.summary}</div>
                                    <div className="mt-2 flex flex-wrap items-center gap-3 text-[10px] uppercase tracking-[0.16em] text-[#8ca197]">
                                      <span>{signal.severity}</span>
                                      <span>{signal.source_name}</span>
                                      {signal.evidence_url ? (
                                        <a href={signal.evidence_url} target="_blank" rel="noreferrer" className="inline-flex items-center gap-1 text-[#dce7e1] hover:text-white">
                                          Evidence
                                          <ExternalLink className="h-3 w-3" />
                                        </a>
                                      ) : null}
                                    </div>
                                  </div>
                                ))
                              ) : (
                                <div className="text-[12px] text-[#95a79f]">No current market signals explicitly mention this competitor.</div>
                              )}
                            </div>
                          </div>
                        </div>
                      </article>
                    ))}
                  </div>
                ) : data ? (
                  <div className="rounded-3xl border border-white/8 bg-white/[0.03] px-6 py-8 text-[13px] leading-6 text-[#99aaa3]">
                    The target company profile was extracted, but there were not enough verified competitor sites to return the full set yet.
                  </div>
                ) : null}
              </div>
            ) : null}
          </section>

          <aside className="min-h-0 overflow-y-auto px-5 py-5 md:px-6">
            <div className="space-y-5">
              <section className="rounded-3xl border border-white/8 bg-white/[0.03] p-5">
                <div className="flex items-center gap-2 text-[10px] uppercase tracking-[0.22em] text-[#9ab2a8]">
                  <ArrowRightLeft className="h-3.5 w-3.5" />
                  Market Context
                </div>
                <div className="mt-4 flex flex-wrap gap-2">
                  {(data?.market_context.mentioned_companies || []).slice(0, 12).map((item) => (
                    <span key={item} className="rounded-full border border-white/8 bg-white/[0.03] px-2 py-1 text-[10px] text-[#d7e1dc]">
                      {item}
                    </span>
                  ))}
                  {!topSignals.length ? <div className="text-[12px] text-[#95a79f]">No relevant market context loaded yet.</div> : null}
                </div>
                <div className="mt-4 space-y-3">
                  {topSignals.map((signal) => (
                    <div key={signal.signal_id} className="rounded-2xl border border-white/8 bg-[#0d1110] px-4 py-4">
                      <div className="text-[12px] font-medium text-[#eef5f1]">{signal.title}</div>
                      <div className="mt-1 text-[11px] uppercase tracking-[0.16em] text-[#879b93]">
                        {signal.signal_type} • {signal.market_category || "market"} • {formatTimestamp(signal.timestamp)}
                      </div>
                      <div className="mt-2 text-[12px] leading-5 text-[#a6b8b1]">{signal.summary}</div>
                    </div>
                  ))}
                </div>
              </section>

              <section className="rounded-3xl border border-white/8 bg-white/[0.03] p-5">
                <div className="flex items-center gap-2 text-[10px] uppercase tracking-[0.22em] text-[#9ab2a8]">
                  <Users2 className="h-3.5 w-3.5" />
                  TinyFish Analysis Runs
                </div>
                <div className="mt-4 space-y-3">
                  {(data?.analysis_runs || []).length ? (
                    data?.analysis_runs.map((run) => (
                      <div key={run.analysis_run_id} className="rounded-2xl border border-white/8 bg-[#0d1110] px-4 py-4">
                        <div className="flex items-start justify-between gap-3">
                          <div>
                            <div className="text-[12px] font-medium text-[#eef5f1]">{run.company_name}</div>
                            <div className="mt-1 text-[10px] uppercase tracking-[0.16em] text-[#7f948c]">
                              {run.role} • {run.source_name}
                            </div>
                          </div>
                          <span className="rounded-full border border-white/8 px-2.5 py-1 text-[10px] uppercase tracking-[0.16em] text-[#d7e1dc]">
                            {run.capture_status || "unknown"}
                          </span>
                        </div>
                        <div className="mt-2 text-[11px] text-[#8fa39b]">{formatTimestamp(run.captured_at)}</div>
                        {run.error?.message ? (
                          <div className="mt-2 text-[11px] text-[#d5aaa0]">{run.error.message}</div>
                        ) : null}
                      </div>
                    ))
                  ) : (
                    <div className="rounded-2xl border border-white/8 bg-[#0d1110] px-4 py-4 text-[12px] text-[#95a79f]">
                      No TinyFish analysis runs yet.
                    </div>
                  )}
                </div>
              </section>

              <section className="rounded-3xl border border-white/8 bg-white/[0.03] p-5">
                <div className="flex items-center gap-2 text-[10px] uppercase tracking-[0.22em] text-[#9ab2a8]">
                  <ShieldAlert className="h-3.5 w-3.5" />
                  Market Source Health
                </div>
                <div className="mt-4 space-y-3">
                  {(data?.source_health || []).slice(0, 6).map((source) => (
                    <div key={source.source_id} className="rounded-2xl border border-white/8 bg-[#0d1110] px-4 py-4">
                      <div className="flex items-start justify-between gap-3">
                        <div>
                          <div className="text-[12px] font-medium text-[#eef5f1]">{source.source_name || source.source_id}</div>
                          <div className="mt-1 text-[10px] uppercase tracking-[0.16em] text-[#7f948c]">
                            {formatTimestamp(source.last_run_at)}
                          </div>
                        </div>
                        <span className="rounded-full border border-white/8 px-2.5 py-1 text-[10px] uppercase tracking-[0.16em] text-[#d7e1dc]">
                          {source.status}
                        </span>
                      </div>
                      {source.last_error?.message ? (
                        <div className="mt-2 text-[11px] text-[#d5aaa0]">{source.last_error.message}</div>
                      ) : null}
                    </div>
                  ))}
                </div>
              </section>
            </div>
          </aside>
        </div>
      </div>

      {error ? (
        <div className="absolute inset-x-0 top-16 z-40 mx-auto w-[min(100vw-2rem,46rem)] rounded-2xl border border-[#6b342f] bg-[#1a0f0d]/92 px-5 py-4 text-[12px] text-[#f0d0c8] backdrop-blur-md">
          {error}
        </div>
      ) : null}
    </div>
  )
}
