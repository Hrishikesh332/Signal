"use client"

import { Activity } from "lucide-react"
import type { DashboardEvent } from "@/lib/dashboard-types"

interface WirePanelProps {
  events: DashboardEvent[] | undefined
  selectedEventId?: string | null
  onSelect?: (eventId: string) => void
}

const SEVERITY_COLORS = {
  critical: "text-red-500 border-red-500/30 bg-red-500/10",
  high: "text-amber-500 border-amber-500/30 bg-amber-500/10",
  medium: "text-lime-500 border-lime-500/30 bg-lime-500/10",
  low: "text-cyan-500 border-cyan-500/30 bg-cyan-500/10",
}

function formatTimestamp(isoString: string): string {
  try {
    const date = new Date(isoString)
    return date.toISOString().slice(11, 19)
  } catch {
    return "--:--:--"
  }
}

export function WirePanel({ events, selectedEventId, onSelect }: WirePanelProps) {
  const hasEvents = events && events.length > 0

  return (
    <div className="flex flex-col h-full border-l border-emerald-900/50 bg-black/40">
      <div className="flex items-center gap-2 px-3 py-2 border-b border-emerald-900/50">
        <Activity className="h-4 w-4 text-emerald-500" />
        <span className="text-xs font-mono uppercase tracking-wider text-emerald-400">
          Live Wire Feed
        </span>
        {hasEvents && (
          <span className="ml-auto text-[10px] font-mono text-emerald-700">
            {events.length} SIGNALS
          </span>
        )}
      </div>

      <div className="flex-1 overflow-y-auto">
        {!hasEvents ? (
          <div className="flex items-center justify-center h-full px-4">
            <p className="text-xs font-mono text-emerald-800 text-center">
              No live cross-category events yet.
            </p>
          </div>
        ) : (
          <div className="divide-y divide-emerald-900/30">
            {events.map((event) => (
              <button
                key={event.id}
                type="button"
                onClick={() => onSelect?.(event.id)}
                className={`block w-full px-3 py-2 text-left transition-colors cursor-pointer ${
                  selectedEventId === event.id
                    ? "bg-emerald-950/35"
                    : "hover:bg-emerald-950/30"
                }`}
              >
                <div className="flex items-center gap-2 mb-1">
                  <span
                    className={`px-1.5 py-0.5 text-[9px] font-mono uppercase tracking-wider border ${
                      SEVERITY_COLORS[event.severity] || SEVERITY_COLORS.low
                    }`}
                  >
                    {event.severity}
                  </span>
                  <span className="text-[10px] font-mono text-emerald-700">
                    {event.category}
                  </span>
                  <span className="ml-auto text-[10px] font-mono text-emerald-800">
                    {formatTimestamp(event.timestamp)}
                  </span>
                </div>
                <p className="text-xs font-mono text-emerald-400 leading-tight">
                  {event.headline}
                </p>
                {event.company && (
                  <p className="text-[10px] font-mono text-emerald-600 mt-1">
                    {event.company}
                  </p>
                )}
              </button>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
