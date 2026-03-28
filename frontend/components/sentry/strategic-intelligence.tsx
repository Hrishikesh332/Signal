"use client"

import { AlertTriangle, FileText, LocateFixed, Radar } from "lucide-react"
import type { DashboardEvent, StrategicInsight } from "@/lib/dashboard-types"

interface StrategicIntelligencePanelProps {
  insights: StrategicInsight[] | undefined
  selectedEvent: DashboardEvent | null
  events: DashboardEvent[] | undefined
}

const SEVERITY_STYLES = {
  critical: "border-red-500/40 bg-red-500/10 text-red-400",
  high: "border-amber-500/40 bg-amber-500/10 text-amber-400",
  medium: "border-lime-500/40 bg-lime-500/10 text-lime-400",
  low: "border-cyan-500/40 bg-cyan-500/10 text-cyan-400",
} satisfies Record<DashboardEvent["severity"], string>

function formatTimestamp(isoString: string | undefined) {
  if (!isoString) return null

  try {
    return new Date(isoString).toISOString().slice(0, 19).replace("T", " ")
  } catch {
    return null
  }
}

export function StrategicIntelligencePanel({
  insights,
  selectedEvent,
  events,
}: StrategicIntelligencePanelProps) {
  const insightItems = insights ?? []
  const eventItems = events ?? []
  const hasInsights = insightItems.length > 0

  return (
    <section className="flex h-full min-h-0 flex-col bg-black/30">
      <div className="flex shrink-0 items-center gap-2 border-b border-emerald-900/30 px-3 py-2">
        <Radar className="h-4 w-4 text-emerald-500" />
        <span className="text-[11px] font-mono uppercase tracking-[0.22em] text-emerald-400">
          Strategic Intelligence
        </span>
        <span className="ml-auto text-[10px] font-mono text-emerald-700">
          {hasInsights ? `${insightItems.length} NARRATIVES` : "NO BRIEFS"}
        </span>
      </div>

      <div className="grid min-h-0 flex-1 grid-cols-1 gap-px bg-emerald-950/30 xl:grid-cols-[minmax(0,1.15fr)_minmax(0,0.85fr)]">
        <div className="flex min-h-0 flex-col bg-black/20">
          <div className="flex items-center justify-between border-b border-emerald-900/20 px-3 py-2">
            <span className="text-[10px] font-mono uppercase tracking-[0.22em] text-emerald-700">
              Narrative Feed
            </span>
            {eventItems.length > 0 && (
              <span className="text-[10px] font-mono text-emerald-800">
                {eventItems.length} LINKED SIGNALS
              </span>
            )}
          </div>

          {!hasInsights ? (
            <div className="flex flex-1 items-center justify-center px-6">
              <div className="max-w-sm border border-emerald-900/30 bg-black/25 px-4 py-5 text-center">
                <FileText className="mx-auto h-4 w-4 text-emerald-700" />
                <p className="mt-3 text-[11px] font-mono leading-5 text-emerald-700">
                  No strategic narratives available from the backend feed yet.
                </p>
              </div>
            </div>
          ) : (
            <div className="min-h-0 flex-1 overflow-y-auto px-3 py-2.5 pr-2 [scrollbar-gutter:stable]">
              <div className="grid gap-2.5">
                {insightItems.map((insight) => {
                  const linkedEvent = eventItems.find(
                    (event) => event.category.toLowerCase() === insight.category.toLowerCase()
                  )

                  return (
                    <article
                      key={insight.id}
                      className="border border-emerald-900/30 bg-black/25 px-3 py-2.5"
                    >
                      <div className="flex flex-wrap items-center gap-2">
                        <span className="border border-emerald-700/40 bg-emerald-950/30 px-2 py-0.5 text-[10px] font-mono uppercase tracking-[0.2em] text-emerald-500">
                          {insight.category}
                        </span>
                        {linkedEvent && (
                          <span
                            className={`border px-2 py-0.5 text-[10px] font-mono uppercase tracking-[0.2em] ${
                              SEVERITY_STYLES[linkedEvent.severity]
                            }`}
                          >
                            {linkedEvent.severity}
                          </span>
                        )}
                        {linkedEvent && formatTimestamp(linkedEvent.timestamp) && (
                          <span className="ml-auto text-[10px] font-mono text-emerald-800">
                            {formatTimestamp(linkedEvent.timestamp)}
                          </span>
                        )}
                      </div>
                      <h3 className="mt-2 text-[13px] font-mono leading-5 text-emerald-300">
                        {insight.title}
                      </h3>
                      <p className="mt-1.5 text-[11px] font-mono leading-5 text-emerald-600">
                        {insight.description}
                      </p>
                    </article>
                  )
                })}
              </div>
            </div>
          )}
        </div>

        <div className="flex min-h-0 flex-col bg-black/25">
          <div className="flex items-center justify-between border-b border-emerald-900/20 px-3 py-2">
            <span className="text-[10px] font-mono uppercase tracking-[0.22em] text-emerald-700">
              Selected Signal Context
            </span>
            {selectedEvent ? (
              <span
                className={`border px-2 py-0.5 text-[10px] font-mono uppercase tracking-[0.2em] ${
                  SEVERITY_STYLES[selectedEvent.severity]
                }`}
              >
                {selectedEvent.severity}
              </span>
            ) : null}
          </div>

          {!selectedEvent ? (
            <div className="flex flex-1 items-center justify-center px-6">
              <div className="max-w-xs border border-emerald-900/30 bg-black/25 px-4 py-5 text-center">
                <LocateFixed className="mx-auto h-4 w-4 text-emerald-700" />
                <p className="mt-3 text-[11px] font-mono leading-5 text-emerald-700">
                  Select a live signal to inspect operational context.
                </p>
              </div>
            </div>
          ) : (
            <div className="min-h-0 flex-1 overflow-y-auto px-3 py-2.5 pr-2 [scrollbar-gutter:stable]">
              <div className="border border-emerald-900/30 bg-black/25 px-3 py-3">
                <div className="flex flex-wrap items-center gap-2">
                  <span className="border border-emerald-700/40 bg-emerald-950/30 px-2 py-0.5 text-[10px] font-mono uppercase tracking-[0.18em] text-emerald-500">
                    {selectedEvent.category}
                  </span>
                  {formatTimestamp(selectedEvent.timestamp) && (
                    <span className="ml-auto text-[10px] font-mono text-emerald-800">
                      {formatTimestamp(selectedEvent.timestamp)}
                    </span>
                  )}
                </div>

                <h3 className="mt-2 text-[13px] font-mono leading-5 text-emerald-300">
                  {selectedEvent.headline}
                </h3>

                {selectedEvent.company && (
                  <p className="mt-1.5 text-[11px] font-mono uppercase tracking-[0.16em] text-emerald-600">
                    {selectedEvent.company}
                  </p>
                )}

                {selectedEvent.explanation ? (
                  <p className="mt-2 text-[11px] font-mono leading-5 text-emerald-600">
                    {selectedEvent.explanation}
                  </p>
                ) : (
                  <div className="mt-2 flex items-start gap-2 border border-emerald-900/20 bg-black/25 px-3 py-2.5">
                    <AlertTriangle className="mt-0.5 h-3.5 w-3.5 text-emerald-700" />
                    <p className="text-[10px] font-mono leading-5 text-emerald-800">
                      No detailed explanation was supplied by the backend for this signal yet.
                    </p>
                  </div>
                )}

                <div className="mt-3 grid grid-cols-1 gap-2">
                  {selectedEvent.location_label && (
                    <div className="border border-emerald-900/20 bg-black/20 px-3 py-2">
                      <p className="text-[9px] font-mono uppercase tracking-[0.18em] text-emerald-800">
                        Origin
                      </p>
                      <p className="mt-1 text-[11px] font-mono text-emerald-400">
                        {selectedEvent.location_label}
                      </p>
                    </div>
                  )}
                  {(typeof selectedEvent.latitude === "number" ||
                    typeof selectedEvent.longitude === "number") && (
                    <div className="border border-emerald-900/20 bg-black/20 px-3 py-2">
                      <p className="text-[9px] font-mono uppercase tracking-[0.18em] text-emerald-800">
                        Coordinates
                      </p>
                      <p className="mt-1 text-[11px] font-mono text-emerald-400">
                        {(selectedEvent.latitude ?? 0).toFixed(2)},{" "}
                        {(selectedEvent.longitude ?? 0).toFixed(2)}
                      </p>
                    </div>
                  )}
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </section>
  )
}
