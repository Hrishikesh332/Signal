"use client"

import type { ReactNode } from "react"
import { AlertTriangle, ArrowUpRight, CheckCircle2, Clock3, ShieldAlert, Sparkles, Target } from "lucide-react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { cn } from "@/lib/utils"

export interface ProductViabilityResponse {
  status: string
  summary: string
  recommendation: string | null
  viability_score: number | null
  confidence_score: number | null
  highlights: {
    strengths: string[]
    risks: string[]
    pricing_fit: string | null
    differentiation: string | null
    next_validation_steps: string[]
    demand_signals: string[]
  }
  competitors: Array<{
    name: string
    price_point: string | null
    url: string
  }>
  sources: Array<{
    title: string
    url: string
  }>
  meta: {
    generated_at?: string
    research_depth?: string
    research_status?: string
    decision_provider?: string | null
    decision_status?: string
    openai_status?: string
    used_local_context?: boolean
  }
}

interface ProductViabilityResultProps {
  result: ProductViabilityResponse | null
  isSubmitting: boolean
  errorMessage?: string | null
}

export function ProductViabilityResult({
  result,
  isSubmitting,
  errorMessage,
}: ProductViabilityResultProps) {
  if (isSubmitting) {
    return (
      <section className="relative overflow-hidden rounded-[28px] border border-[#202024] bg-[#0b0b0d]/92 p-8 shadow-[0_40px_120px_rgba(0,0,0,0.45)]">
        <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_right,rgba(34,197,94,0.12),transparent_30%),linear-gradient(180deg,rgba(255,255,255,0.02),transparent)]" />
        <div className="relative flex min-h-[28rem] flex-col items-center justify-center text-center">
          <div className="flex h-16 w-16 items-center justify-center rounded-full border border-[#22c55e]/35 bg-[#0d1510] text-[#22c55e] shadow-[0_0_50px_rgba(34,197,94,0.12)]">
            <Clock3 className="h-6 w-6 animate-pulse" />
          </div>
          <h2 className="mt-6 text-lg font-semibold tracking-[0.08em] text-white">TinyFish Is Running Live Research</h2>
          <p className="mt-3 max-w-md text-sm leading-6 text-[#a1a1aa]">
            The system is searching the market, mapping competitors, and assembling pricing and demand evidence before the
            viability brief is rendered.
          </p>
          <div className="mt-8 h-1.5 w-full max-w-sm overflow-hidden rounded-full bg-[#16171a]">
            <div className="h-full w-1/2 animate-[pulse_1.6s_ease-in-out_infinite] rounded-full bg-gradient-to-r from-[#22c55e]/20 via-[#22c55e] to-[#22c55e]/20" />
          </div>
        </div>
      </section>
    )
  }

  if (errorMessage) {
    return (
      <section className="rounded-[28px] border border-[#331a1d] bg-[#0f090a]/92 p-6 shadow-[0_30px_80px_rgba(0,0,0,0.4)]">
        <Alert className="border-[#5f2328] bg-[#160d0e] text-[#fecaca]">
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>Product viability request failed</AlertTitle>
          <AlertDescription>{errorMessage}</AlertDescription>
        </Alert>
      </section>
    )
  }

  if (!result) {
    return (
      <section className="relative overflow-hidden rounded-[28px] border border-[#202024] bg-[#0b0b0d]/92 p-8 shadow-[0_40px_120px_rgba(0,0,0,0.45)]">
        <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_right,rgba(255,255,255,0.04),transparent_30%),linear-gradient(180deg,rgba(255,255,255,0.02),transparent)]" />
        <div className="relative flex min-h-[28rem] flex-col items-center justify-center text-center">
          <div className="flex h-16 w-16 items-center justify-center rounded-full border border-[#202024] bg-[#111214] text-[#71717a]">
            <Target className="h-6 w-6" />
          </div>
          <h2 className="mt-6 text-lg font-semibold tracking-[0.08em] text-white">Awaiting Product Brief</h2>
          <p className="mt-3 max-w-md text-sm leading-6 text-[#71717a]">
            Submit a concept from the left-hand console to populate the viability overview, competitor set, and market
            signals.
          </p>
        </div>
      </section>
    )
  }

  const recommendation = normalizeRecommendationLabel(result.recommendation)
  const recommendationTone = recommendationToneClass(result.recommendation)

  return (
    <section className="relative overflow-hidden rounded-[28px] border border-[#202024] bg-[#0b0b0d]/92 shadow-[0_40px_120px_rgba(0,0,0,0.45)]">
      <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_right,rgba(34,197,94,0.14),transparent_34%),radial-gradient(circle_at_bottom_left,rgba(255,255,255,0.03),transparent_24%)]" />
      <div className="relative border-b border-[#202024] px-6 py-5">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div className="space-y-3">
            <div className="flex flex-wrap items-center gap-2">
              <Badge className={cn("rounded-full border px-3 py-1 text-[10px] tracking-[0.22em]", recommendationTone)}>
                {recommendation}
              </Badge>
              <Badge variant="outline" className="rounded-full border-[#2c2c32] bg-[#0f1012] px-3 py-1 text-[10px] tracking-[0.22em] text-[#a1a1aa]">
                {String(result.meta.research_depth || "standard").toUpperCase()}
              </Badge>
              <Badge variant="outline" className="rounded-full border-[#2c2c32] bg-[#0f1012] px-3 py-1 text-[10px] tracking-[0.22em] text-[#a1a1aa]">
                {String(result.meta.decision_provider || "TinyFish").toUpperCase()}
              </Badge>
            </div>
            <h2 className="max-w-3xl text-2xl font-semibold leading-tight tracking-[0.06em] text-white">
              {result.summary}
            </h2>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <Metric label="Viability" value={scoreValue(result.viability_score)} accent="text-[#22c55e]" />
            <Metric label="Confidence" value={confidenceValue(result.confidence_score)} accent="text-white" />
          </div>
        </div>

        {result.status === "pending" ? (
          <Alert className="mt-5 border-[#3c3712] bg-[#14110a] text-[#fde68a]">
            <Clock3 className="h-4 w-4" />
            <AlertTitle>Research is still running</AlertTitle>
            <AlertDescription>
              TinyFish has not finished collecting market evidence yet. The current response is intentionally incomplete.
            </AlertDescription>
          </Alert>
        ) : null}
      </div>

      <div className="relative grid gap-6 px-6 py-6 xl:grid-cols-[minmax(0,1.2fr)_minmax(0,0.8fr)]">
        <div className="grid gap-6">
          <SignalPanel
            title="Strengths"
            eyebrow="WHY IT COULD WIN"
            icon={<CheckCircle2 className="h-4 w-4" />}
            items={result.highlights.strengths}
          />
          <SignalPanel
            title="Risks"
            eyebrow="WHAT COULD BREAK"
            icon={<ShieldAlert className="h-4 w-4" />}
            items={result.highlights.risks}
            danger
          />
          <div className="grid gap-6 lg:grid-cols-2">
            <NarrativePanel title="Pricing Fit" eyebrow="MONETIZATION">
              {result.highlights.pricing_fit || "Pricing fit has not been established yet."}
            </NarrativePanel>
            <NarrativePanel title="Differentiation" eyebrow="POSITIONING">
              {result.highlights.differentiation || "No clear differentiation signal was returned."}
            </NarrativePanel>
          </div>
          <div className="grid gap-6 lg:grid-cols-2">
            <SignalPanel
              title="Demand Signals"
              eyebrow="MARKET PULL"
              icon={<Sparkles className="h-4 w-4" />}
              items={result.highlights.demand_signals}
            />
            <SignalPanel
              title="Next Steps"
              eyebrow="VALIDATION"
              icon={<Target className="h-4 w-4" />}
              items={result.highlights.next_validation_steps}
            />
          </div>
        </div>

        <div className="grid gap-6">
          <div className="rounded-[24px] border border-[#202024] bg-[#09090b]/70 p-5">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-[10px] tracking-[0.24em] text-[#71717a]">COMPETITORS</p>
                <h3 className="mt-2 text-base font-semibold tracking-[0.08em] text-white">Observed Market Anchors</h3>
              </div>
              <span className="text-[10px] tracking-[0.22em] text-[#52525b]">{result.competitors.length} visible</span>
            </div>
            <div className="mt-4 space-y-3">
              {result.competitors.length > 0 ? (
                result.competitors.map((competitor) => (
                  <a
                    key={`${competitor.name}-${competitor.url}`}
                    href={competitor.url}
                    target="_blank"
                    rel="noreferrer"
                    className="group flex items-center justify-between rounded-2xl border border-[#202024] bg-[#0d0e10] px-4 py-3 transition-colors hover:border-[#2f2f35] hover:bg-[#111214]"
                  >
                    <div>
                      <p className="text-sm font-medium text-white">{competitor.name}</p>
                      <p className="mt-1 text-xs text-[#71717a]">{competitor.price_point || "Price not surfaced"}</p>
                    </div>
                    <ArrowUpRight className="h-4 w-4 text-[#71717a] transition-colors group-hover:text-white" />
                  </a>
                ))
              ) : (
                <EmptySlot label="No competitor entries returned yet." />
              )}
            </div>
          </div>

          <div className="rounded-[24px] border border-[#202024] bg-[#09090b]/70 p-5">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-[10px] tracking-[0.24em] text-[#71717a]">SOURCES</p>
                <h3 className="mt-2 text-base font-semibold tracking-[0.08em] text-white">Evidence Trail</h3>
              </div>
              <span className="text-[10px] tracking-[0.22em] text-[#52525b]">
                {result.meta.generated_at ? result.meta.generated_at.slice(0, 10) : "LIVE"}
              </span>
            </div>
            <div className="mt-4 space-y-3">
              {result.sources.length > 0 ? (
                result.sources.map((source) => (
                  <a
                    key={`${source.title}-${source.url}`}
                    href={source.url}
                    target="_blank"
                    rel="noreferrer"
                    className="group flex items-start justify-between gap-4 rounded-2xl border border-[#202024] bg-[#0d0e10] px-4 py-3 transition-colors hover:border-[#2f2f35] hover:bg-[#111214]"
                  >
                    <div>
                      <p className="text-sm text-white">{source.title}</p>
                      <p className="mt-1 break-all text-xs text-[#71717a]">{source.url}</p>
                    </div>
                    <ArrowUpRight className="mt-0.5 h-4 w-4 shrink-0 text-[#71717a] transition-colors group-hover:text-white" />
                  </a>
                ))
              ) : (
                <EmptySlot label="No sources are available for this result state." />
              )}
            </div>
          </div>
        </div>
      </div>
    </section>
  )
}

interface MetricProps {
  label: string
  value: string
  accent: string
}

function Metric({ label, value, accent }: MetricProps) {
  return (
    <div className="rounded-[22px] border border-[#202024] bg-[#0d0e10] px-4 py-4 text-right shadow-[inset_0_1px_0_rgba(255,255,255,0.03)]">
      <div className="text-[10px] tracking-[0.22em] text-[#71717a]">{label}</div>
      <div className={cn("mt-2 text-2xl font-semibold tracking-[0.08em]", accent)}>{value}</div>
    </div>
  )
}

interface SignalPanelProps {
  title: string
  eyebrow: string
  icon: ReactNode
  items: string[]
  danger?: boolean
}

function SignalPanel({ title, eyebrow, icon, items, danger = false }: SignalPanelProps) {
  return (
    <div
      className={cn(
        "rounded-[24px] border bg-[#09090b]/70 p-5",
        danger ? "border-[#3a1d20]" : "border-[#202024]",
      )}
    >
      <div className="flex items-center gap-3">
        <div
          className={cn(
            "flex h-10 w-10 items-center justify-center rounded-2xl border",
            danger ? "border-[#5b2328] bg-[#170c0d] text-[#fca5a5]" : "border-[#202024] bg-[#111214] text-[#22c55e]",
          )}
        >
          {icon}
        </div>
        <div>
          <p className="text-[10px] tracking-[0.24em] text-[#71717a]">{eyebrow}</p>
          <h3 className="mt-1 text-base font-semibold tracking-[0.08em] text-white">{title}</h3>
        </div>
      </div>
      <div className="mt-4 space-y-3">
        {items.length > 0 ? (
          items.map((item, index) => (
            <div
              key={`${title}-${index}`}
              className={cn(
                "rounded-2xl border px-4 py-3 text-sm leading-6",
                danger
                  ? "border-[#3a1d20] bg-[#110b0c] text-[#fecaca]"
                  : "border-[#202024] bg-[#0d0e10] text-[#d4d4d8]",
              )}
            >
              {item}
            </div>
          ))
        ) : (
          <EmptySlot label={`No ${title.toLowerCase()} were returned.`} />
        )}
      </div>
    </div>
  )
}

interface NarrativePanelProps {
  title: string
  eyebrow: string
  children: string
}

function NarrativePanel({ title, eyebrow, children }: NarrativePanelProps) {
  return (
    <div className="rounded-[24px] border border-[#202024] bg-[#09090b]/70 p-5">
      <p className="text-[10px] tracking-[0.24em] text-[#71717a]">{eyebrow}</p>
      <h3 className="mt-2 text-base font-semibold tracking-[0.08em] text-white">{title}</h3>
      <p className="mt-4 text-sm leading-7 text-[#d4d4d8]">{children}</p>
    </div>
  )
}

function EmptySlot({ label }: { label: string }) {
  return (
    <div className="rounded-2xl border border-dashed border-[#2a2a30] bg-[#0c0d0f] px-4 py-5 text-sm text-[#71717a]">
      {label}
    </div>
  )
}

function scoreValue(value: number | null) {
  return typeof value === "number" ? `${value}` : "--"
}

function confidenceValue(value: number | null) {
  return typeof value === "number" ? `${Math.round(value * 100)}%` : "--"
}

function normalizeRecommendationLabel(value: string | null) {
  if (!value) {
    return "NO VERDICT"
  }
  return value.replace(/_/g, " ").toUpperCase()
}

function recommendationToneClass(value: string | null) {
  switch (value) {
    case "strong_yes":
      return "border-[#1f5130] bg-[#0e1c13] text-[#86efac]"
    case "cautious_yes":
      return "border-[#5e4a17] bg-[#1b160b] text-[#fde68a]"
    case "likely_no":
      return "border-[#5f2328] bg-[#170c0d] text-[#fca5a5]"
    default:
      return "border-[#2c2c32] bg-[#101114] text-[#d4d4d8]"
  }
}
