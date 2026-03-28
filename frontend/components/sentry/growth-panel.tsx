"use client"

import { ArrowDown, ArrowUp, Minus, TrendingUp } from "lucide-react"
import type { GrowthIntelligence, TrendDataPoint } from "@/lib/dashboard-types"

interface GrowthPanelProps {
  data: GrowthIntelligence | undefined
}

const TREND_COLORS = {
  jobs: "stroke-cyan-400",
  products: "stroke-emerald-400",
  funding: "stroke-amber-400",
  markets: "stroke-lime-400",
  role_clusters: "stroke-teal-400",
}

function getSeriesTrend(series: TrendDataPoint[]) {
  if (series.length < 2) return "stable" as const

  const previous = series[series.length - 2]?.value ?? 0
  const current = series[series.length - 1]?.value ?? 0

  if (current > previous) return "up" as const
  if (current < previous) return "down" as const
  return "stable" as const
}

function Sparkline({
  points,
  className,
}: {
  points: TrendDataPoint[]
  className: string
}) {
  if (!points.length) {
    return (
      <div className="flex h-16 items-center justify-center border border-emerald-900/20 bg-black/20">
        <span className="text-[10px] font-mono uppercase tracking-[0.18em] text-emerald-800">
          No series
        </span>
      </div>
    )
  }

  const width = 180
  const height = 48
  const max = Math.max(...points.map((point) => point.value))
  const min = Math.min(...points.map((point) => point.value))
  const range = max - min || 1

  const path = points
    .map((point, index) => {
      const x = (index / Math.max(points.length - 1, 1)) * width
      const y = height - ((point.value - min) / range) * (height - 8) - 4
      return `${index === 0 ? "M" : "L"}${x.toFixed(2)},${y.toFixed(2)}`
    })
    .join(" ")

  return (
    <svg viewBox={`0 0 ${width} ${height}`} className="h-16 w-full overflow-visible">
      <path
        d={path}
        fill="none"
        className={className}
        strokeWidth="2"
        vectorEffect="non-scaling-stroke"
      />
    </svg>
  )
}

function TrendTile({
  label,
  points,
  toneClass,
}: {
  label: string
  points: TrendDataPoint[]
  toneClass: string
}) {
  const latest = points[points.length - 1]?.value
  const trend = getSeriesTrend(points)
  const TrendIcon = trend === "up" ? ArrowUp : trend === "down" ? ArrowDown : Minus

  return (
    <div className="border border-emerald-900/30 bg-black/20 px-3 py-2.5">
      <div className="flex items-center gap-2">
        <span className="text-[10px] font-mono uppercase tracking-[0.2em] text-emerald-700">
          {label}
        </span>
        <TrendIcon
          className={`ml-auto h-3.5 w-3.5 ${
            trend === "up"
              ? "text-lime-500"
              : trend === "down"
                ? "text-red-500"
                : "text-emerald-700"
          }`}
        />
      </div>

      <div className="mt-2 flex items-end justify-between gap-3">
        <div>
          <p className="text-base font-mono text-emerald-300">
            {typeof latest === "number" ? latest : "--"}
          </p>
          <p className="mt-1 text-[10px] font-mono text-emerald-800">
            {points.length > 0 ? `${points.length} points` : "Awaiting backend series"}
          </p>
        </div>
        <div className="min-w-0 flex-1">
          <Sparkline points={points} className={toneClass} />
        </div>
      </div>
    </div>
  )
}

export function GrowthPanel({ data }: GrowthPanelProps) {
  const trendSeries = data?.trend_series
  const kpis = data?.kpis ?? []

  return (
    <section className="flex h-full min-h-0 flex-col bg-black/30">
      <div className="flex shrink-0 items-center gap-2 border-b border-emerald-900/30 px-3 py-2">
        <TrendingUp className="h-4 w-4 text-emerald-500" />
        <span className="text-[11px] font-mono uppercase tracking-[0.22em] text-emerald-400">
          Trend Board
        </span>
      </div>

      <div className="flex min-h-0 flex-1 flex-col">
        <div className="grid grid-cols-2 gap-px bg-emerald-950/30 lg:grid-cols-4">
          {kpis.length > 0 ? (
            kpis.slice(0, 4).map((kpi) => (
              <div key={kpi.id} className="bg-black/20 px-3 py-2.5">
                <p className="text-[9px] font-mono uppercase tracking-[0.18em] text-emerald-800">
                  {kpi.label}
                </p>
                <p className="mt-1 text-[13px] font-mono text-emerald-300">{kpi.value}</p>
              </div>
            ))
          ) : (
            <div className="col-span-full bg-black/20 px-4 py-6 text-center">
              <p className="text-[10px] font-mono uppercase tracking-[0.18em] text-emerald-800">
                No growth KPI payload available.
              </p>
            </div>
          )}
        </div>

        <div className="min-h-0 flex-1 overflow-y-auto px-3 py-2.5 pr-2 [scrollbar-gutter:stable]">
          <div className="grid gap-2.5 md:grid-cols-2">
            <TrendTile
              label="Jobs"
              points={trendSeries?.jobs ?? []}
              toneClass={TREND_COLORS.jobs}
            />
            <TrendTile
              label="Products"
              points={trendSeries?.products ?? []}
              toneClass={TREND_COLORS.products}
            />
            <TrendTile
              label="Funding"
              points={trendSeries?.funding ?? []}
              toneClass={TREND_COLORS.funding}
            />
            <TrendTile
              label="Markets"
              points={trendSeries?.markets ?? []}
              toneClass={TREND_COLORS.markets}
            />
            <TrendTile
              label="Role Clusters"
              points={trendSeries?.role_clusters ?? []}
              toneClass={TREND_COLORS.role_clusters}
            />
          </div>
        </div>
      </div>
    </section>
  )
}
