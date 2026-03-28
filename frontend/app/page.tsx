"use client"

import { useMemo, useState } from "react"
import type { MapPoint, DashboardEvent } from "@/lib/dashboard-types"
import { useDashboard } from "@/hooks/use-dashboard"
import { CommandBar } from "@/components/sentry/command-bar"
import { KPIStrip } from "@/components/sentry/kpi-strip"
import { IntelMap } from "@/components/sentry/intel-map"
import { WirePanel } from "@/components/sentry/wire-panel"
import { AlertRail } from "@/components/sentry/alert-rail"
import { GrowthPanel } from "@/components/sentry/growth-panel"
import { StrategicIntelligencePanel } from "@/components/sentry/strategic-intelligence"
import { SourceHealthPanel } from "@/components/sentry/source-health"
import { eventToMapPoint } from "@/lib/location-resolver"
import { Loader2 } from "lucide-react"

export default function DashboardPage() {
  const { data, error, isLoading, refresh } = useDashboard()
  const [isRefreshing, setIsRefreshing] = useState(false)
  const [selectedEventId, setSelectedEventId] = useState<string | null>(null)

  const events = useMemo(() => data?.dashboard?.events ?? [], [data?.dashboard?.events])
  const selectedEvent = useMemo(() => {
    if (events.length === 0) return null
    return events.find((event) => event.id === selectedEventId) ?? events[0]
  }, [events, selectedEventId])

  const combinedMapPoints = useMemo(() => {
    const mapPoints: MapPoint[] = data?.dashboard?.map?.points ?? []
    const eventPoints: MapPoint[] = (data?.dashboard?.events ?? [])
      .map((e: DashboardEvent) => eventToMapPoint(e))
      .filter((point): point is MapPoint => point !== null)

    const combined = [...mapPoints, ...eventPoints]
    const deduped = new Map<string, MapPoint>()

    combined.forEach((point, index) => {
      const fallbackKey = [
          point.entity_name,
          point.category ?? "",
          point.timestamp ?? "",
          point.latitude.toFixed(4),
          point.longitude.toFixed(4),
          point.severity,
        ].join("|")
      const key = point.id ?? (fallbackKey || `point-${index}`)

      if (!deduped.has(key)) {
        deduped.set(key, point)
      }
    })

    return Array.from(deduped.values())
  }, [data?.dashboard?.events, data?.dashboard?.map?.points])

  const handleRefresh = async () => {
    setIsRefreshing(true)
    await refresh()
    setIsRefreshing(false)
  }

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-screen bg-[#030a06]">
        <div className="flex flex-col items-center gap-4">
          <Loader2 className="h-8 w-8 text-emerald-500 animate-spin" />
          <p className="text-sm font-mono text-emerald-600">
            INITIALIZING COMMAND CENTER...
          </p>
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-screen bg-[#030a06]">
        <div className="flex flex-col items-center gap-4 text-center px-4">
          <div className="w-12 h-12 border-2 border-red-500 flex items-center justify-center">
            <span className="text-red-500 text-2xl font-mono">!</span>
          </div>
          <p className="text-sm font-mono text-red-500">
            CONNECTION FAILED
          </p>
          <p className="text-xs font-mono text-emerald-700 max-w-md">
            Unable to establish connection to data sources. Check network status
            and try again.
          </p>
          <button
            onClick={handleRefresh}
            className="px-4 py-2 text-xs font-mono uppercase tracking-wider bg-emerald-950 text-emerald-400 border border-emerald-700 hover:bg-emerald-900"
          >
            Retry Connection
          </button>
        </div>
      </div>
    )
  }

  return (
    <div className="flex flex-col h-screen bg-[#030a06] overflow-hidden">
      {/* Command Bar */}
      <CommandBar
        meta={data?.meta ?? null}
        onRefresh={handleRefresh}
        isRefreshing={isRefreshing}
      />

      {/* KPI Strips */}
      <KPIStrip kpis={data?.dashboard?.kpis} label="PRIMARY" />
      <KPIStrip kpis={data?.dashboard?.growth_intelligence?.kpis} label="GROWTH" />

      {/* Alert Rail (only shows if alerts exist) */}
      <AlertRail alerts={data?.dashboard?.alerts} />

      {/* Main Content */}
      <div className="flex flex-1 min-h-0">
        {/* Map Area */}
        <div className="flex-1 flex flex-col min-w-0">
          <div className="flex-1 min-h-0">
            <IntelMap points={combinedMapPoints} />
          </div>

          {/* Bottom Intelligence Panels */}
          <div className="min-h-0 flex-shrink-0 overflow-hidden border-t border-emerald-900/50 bg-black/25">
            <div className="grid min-h-0 h-[clamp(320px,38vh,460px)] grid-cols-1 gap-px bg-emerald-950/30 xl:grid-cols-[minmax(0,1.3fr)_minmax(0,0.92fr)_minmax(0,1fr)]">
              <div className="min-h-0 overflow-hidden bg-black/20">
                <StrategicIntelligencePanel
                  insights={data?.dashboard?.growth_intelligence?.strategic_insights}
                  selectedEvent={selectedEvent}
                  events={events}
                />
              </div>
              <div className="min-h-0 overflow-hidden bg-black/20">
                <GrowthPanel data={data?.dashboard?.growth_intelligence} />
              </div>
              <div className="min-h-0 overflow-hidden bg-black/20">
                <SourceHealthPanel sources={data?.dashboard?.source_health} />
              </div>
            </div>
          </div>
        </div>

        {/* Wire Panel */}
        <div className="hidden w-80 flex-shrink-0 md:block lg:w-96 xl:w-[25rem]">
          <WirePanel
            events={events}
            selectedEventId={selectedEvent?.id ?? null}
            onSelect={setSelectedEventId}
          />
        </div>
      </div>

      {/* Status Bar */}
      <div className="flex items-center justify-between px-4 py-1 border-t border-emerald-900/50 bg-black/60 text-[10px] font-mono">
        <div className="flex items-center gap-4">
          <span className="flex items-center gap-1.5">
            <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse" />
            <span className="text-emerald-600">SYSTEM ONLINE</span>
          </span>
          <span className="text-emerald-800">|</span>
          <span className="text-emerald-700">
            SOURCES: {data?.meta?.source_count ?? 0}
          </span>
          <span className="text-emerald-800">|</span>
          <span className="text-emerald-700">
            SNAPSHOTS: {data?.meta?.snapshot_count ?? 0}
          </span>
        </div>
        <span className="text-emerald-700">
          AI MARKET SENTRY COMMAND CENTER v1.0
        </span>
      </div>
    </div>
  )
}
