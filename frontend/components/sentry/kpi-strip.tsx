import type { KPI } from "@/lib/dashboard-types"

interface KPIStripProps {
  kpis: KPI[] | undefined
  label?: string
}

export function KPIStrip({ kpis, label }: KPIStripProps) {
  if (!kpis || kpis.length === 0) {
    return (
      <div className="flex items-center gap-2 px-4 py-2 border-b border-emerald-900/30 bg-black/20">
        {label && (
          <span className="text-[10px] font-mono uppercase tracking-wider text-emerald-700 mr-2">
            {label}
          </span>
        )}
        <span className="text-xs font-mono text-emerald-800">
          No KPIs available
        </span>
      </div>
    )
  }

  return (
    <div className="flex items-center gap-4 px-4 py-2 border-b border-emerald-900/30 bg-black/20 overflow-x-auto">
      {label && (
        <span className="text-[10px] font-mono uppercase tracking-wider text-emerald-700 mr-2 shrink-0">
          {label}
        </span>
      )}
      {kpis.map((kpi) => (
        <div
          key={kpi.id}
          className="flex items-center gap-2 shrink-0 px-3 py-1 border border-emerald-900/50 bg-emerald-950/30"
        >
          <span className="text-[10px] font-mono uppercase tracking-wider text-emerald-600">
            {kpi.label}
          </span>
          <span className="text-sm font-mono font-bold text-emerald-400">
            {kpi.value}
          </span>
        </div>
      ))}
    </div>
  )
}
