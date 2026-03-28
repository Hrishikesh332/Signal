"use client"

import { RefreshCw, Shield } from "lucide-react"
import type { DashboardMeta } from "@/lib/dashboard-types"

interface CommandBarProps {
  meta: DashboardMeta | null
  onRefresh: () => void
  isRefreshing: boolean
}

function formatTimestamp(isoString: string | undefined): string {
  if (!isoString) return "--"
  try {
    const date = new Date(isoString)
    return date.toISOString().replace("T", " ").slice(0, 19) + " UTC"
  } catch {
    return "--"
  }
}

export function CommandBar({ meta, onRefresh, isRefreshing }: CommandBarProps) {
  return (
    <header className="flex items-center justify-between px-4 py-2 border-b border-emerald-900/50 bg-black/40">
      <div className="flex items-center gap-4">
        <div className="flex items-center gap-2">
          <Shield className="h-5 w-5 text-emerald-500" />
          <span className="font-mono text-sm font-bold tracking-wider text-emerald-400">
            AI MARKET SENTRY
          </span>
        </div>

        <div className="hidden md:flex items-center gap-4 text-xs font-mono text-emerald-600">
          <span>GEN: {formatTimestamp(meta?.generated_at)}</span>
          <span className="text-emerald-800">|</span>
          <span>SNAP: {formatTimestamp(meta?.latest_snapshot_at)}</span>
          <span className="text-emerald-800">|</span>
          <span>SOURCES: {meta?.source_count ?? 0}</span>
          <span className="text-emerald-800">|</span>
          <span>SNAPSHOTS: {meta?.snapshot_count ?? 0}</span>
        </div>
      </div>

      <div className="flex items-center gap-3">
        {meta?.integrations?.map((integration) => (
          <span
            key={integration}
            className="px-2 py-0.5 text-[10px] font-mono uppercase tracking-wider bg-emerald-950 text-emerald-500 border border-emerald-800"
          >
            {integration}
          </span>
        ))}

        <button
          onClick={onRefresh}
          disabled={isRefreshing}
          className="flex items-center gap-1.5 px-3 py-1 text-xs font-mono uppercase tracking-wider bg-emerald-950 text-emerald-400 border border-emerald-700 hover:bg-emerald-900 hover:border-emerald-600 transition-colors disabled:opacity-50"
        >
          <RefreshCw
            className={`h-3 w-3 ${isRefreshing ? "animate-spin" : ""}`}
          />
          Refresh
        </button>
      </div>
    </header>
  )
}
