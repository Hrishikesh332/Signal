"use client"

import { AlertTriangle } from "lucide-react"
import type { Alert } from "@/lib/dashboard-types"

interface AlertRailProps {
  alerts: Alert[] | undefined
}

const SEVERITY_STYLES = {
  critical: "border-red-500/50 bg-red-950/30 text-red-400",
  high: "border-amber-500/50 bg-amber-950/30 text-amber-400",
  medium: "border-lime-500/50 bg-lime-950/30 text-lime-400",
  low: "border-cyan-500/50 bg-cyan-950/30 text-cyan-400",
}

export function AlertRail({ alerts }: AlertRailProps) {
  // Only render if there are alerts
  if (!alerts || alerts.length === 0) {
    return null
  }

  // Filter for critical and high priority alerts
  const priorityAlerts = alerts.filter(
    (alert) => alert.severity === "critical" || alert.severity === "high"
  )

  if (priorityAlerts.length === 0) {
    return null
  }

  return (
    <div className="flex items-center gap-2 px-4 py-1.5 border-b border-red-900/30 bg-red-950/10 overflow-x-auto">
      <AlertTriangle className="h-4 w-4 text-red-500 shrink-0" />
      <span className="text-[10px] font-mono uppercase tracking-wider text-red-500 shrink-0">
        ALERTS
      </span>
      <div className="flex items-center gap-2">
        {priorityAlerts.map((alert) => (
          <div
            key={alert.id}
            className={`px-2 py-0.5 text-xs font-mono border shrink-0 ${
              SEVERITY_STYLES[alert.severity]
            }`}
          >
            {alert.message}
          </div>
        ))}
      </div>
    </div>
  )
}
