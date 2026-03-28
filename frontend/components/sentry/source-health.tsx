"use client"

import { AlertCircle, CheckCircle2, Database, ShieldAlert, XCircle } from "lucide-react"
import type { SourceHealth } from "@/lib/dashboard-types"

interface SourceHealthPanelProps {
  sources: SourceHealth[] | undefined
}

const STATUS_ICONS = {
  healthy: CheckCircle2,
  degraded: AlertCircle,
  failed: XCircle,
}

const STATUS_STYLES = {
  healthy: "border-lime-500/35 bg-lime-500/8 text-lime-400",
  degraded: "border-amber-500/35 bg-amber-500/8 text-amber-400",
  failed: "border-red-500/35 bg-red-500/10 text-red-400",
} satisfies Record<SourceHealth["status"], string>

function formatTimestamp(isoString: string) {
  try {
    return new Date(isoString).toISOString().slice(0, 19).replace("T", " ")
  } catch {
    return "--"
  }
}

export function SourceHealthPanel({ sources }: SourceHealthPanelProps) {
  const items = sources ?? []
  const hasSources = items.length > 0

  return (
    <section className="flex h-full min-h-0 flex-col bg-black/30">
      <div className="flex shrink-0 items-center gap-2 border-b border-emerald-900/30 px-3 py-2">
        <ShieldAlert className="h-4 w-4 text-emerald-500" />
        <span className="text-[11px] font-mono uppercase tracking-[0.22em] text-emerald-400">
          Source Health / Trust Layer
        </span>
        {hasSources && (
          <span className="ml-auto text-[10px] font-mono text-emerald-700">
            {items.length} PROVIDERS
          </span>
        )}
      </div>

      {!hasSources ? (
        <div className="flex flex-1 items-center justify-center px-6">
          <div className="border border-emerald-900/30 bg-black/25 px-4 py-5 text-center">
            <Database className="mx-auto h-4 w-4 text-emerald-700" />
            <p className="mt-3 text-xs font-mono text-emerald-700">
              No backend source-health payload available yet.
            </p>
          </div>
        </div>
      ) : (
        <div className="min-h-0 flex-1 overflow-y-auto px-3 py-2.5 pr-2 [scrollbar-gutter:stable]">
          <div className="grid gap-2.5">
            {items.map((source, index) => {
              const StatusIcon = STATUS_ICONS[source.status]

              return (
                <article
                  key={`${source.provider}-${index}`}
                  className={`border px-3 py-2.5 ${
                    source.status === "failed"
                      ? "border-red-900/40 bg-red-950/10"
                      : "border-emerald-900/30 bg-black/25"
                  }`}
                >
                  <div className="flex items-start gap-2">
                    <div>
                      <h3 className="text-[13px] font-mono text-emerald-300">
                        {source.provider}
                      </h3>
                      <p className="mt-1 text-[10px] font-mono text-emerald-800">
                        LAST RUN {formatTimestamp(source.last_run_at)}
                      </p>
                    </div>

                    <div className="ml-auto flex items-center gap-1.5">
                      <StatusIcon className={`h-3.5 w-3.5 ${STATUS_STYLES[source.status].split(" ").at(-1)}`} />
                      <span
                        className={`border px-2 py-0.5 text-[10px] font-mono uppercase tracking-[0.2em] ${STATUS_STYLES[source.status]}`}
                      >
                        {source.status}
                      </span>
                    </div>
                  </div>

                  <div className="mt-3 grid grid-cols-2 gap-2 xl:grid-cols-4">
                    <div className="border border-emerald-900/20 bg-black/20 px-3 py-2">
                      <p className="text-[9px] font-mono uppercase tracking-[0.18em] text-emerald-800">
                        Success
                      </p>
                      <p className="mt-1 text-[13px] font-mono text-emerald-300">
                        {(source.success_rate * 100).toFixed(1)}%
                      </p>
                    </div>
                    <div className="border border-emerald-900/20 bg-black/20 px-3 py-2">
                      <p className="text-[9px] font-mono uppercase tracking-[0.18em] text-emerald-800">
                        Runtime
                      </p>
                      <p className="mt-1 text-[13px] font-mono text-emerald-300">
                        {source.avg_runtime_ms}ms
                      </p>
                    </div>
                    <div className="border border-emerald-900/20 bg-black/20 px-3 py-2">
                      <p className="text-[9px] font-mono uppercase tracking-[0.18em] text-emerald-800">
                        Snapshots
                      </p>
                      <p className="mt-1 text-[13px] font-mono text-emerald-300">
                        {source.snapshots_total}
                      </p>
                    </div>
                    <div className="border border-emerald-900/20 bg-black/20 px-3 py-2">
                      <p className="text-[9px] font-mono uppercase tracking-[0.18em] text-emerald-800">
                        Trust
                      </p>
                      <p className="mt-1 text-[13px] font-mono text-emerald-300">
                        {source.status === "failed"
                          ? "BREACHED"
                          : source.status === "degraded"
                            ? "WATCH"
                            : "GREEN"}
                      </p>
                    </div>
                  </div>

                  {source.last_error && (
                    <div className="mt-3 border border-red-900/50 bg-red-950/20 px-3 py-3">
                      <p className="text-[10px] font-mono uppercase tracking-[0.18em] text-red-400">
                        {source.last_error.code}
                      </p>
                      <p className="mt-2 text-[10px] font-mono leading-5 text-red-500/80">
                        {source.last_error.message}
                      </p>
                    </div>
                  )}
                </article>
              )
            })}
          </div>
        </div>
      )}
    </section>
  )
}
