"use client"

import { AlertTriangle, Radio, Activity, Clock, MapPin } from "lucide-react"

interface WireEvent {
  id: string
  location: string
  country: string
  severity: "CRITICAL" | "ELEVATED" | "ACTIVE"
  date: string
  timeOfDay: string
  description: string
  timeAgo: string
}

const wireEvents: WireEvent[] = [
  {
    id: "1",
    location: "PRINCE SULTAN AIR BASE",
    country: "Saudi Arabia",
    severity: "CRITICAL",
    date: "Mar 28",
    timeOfDay: "morning",
    description: "Reports indicate 14 additional people of unspecified nationality were injured in the Iranian strike on Prince Sultan Air Base.",
    timeAgo: "~1h ago"
  },
  {
    id: "2",
    location: "IRAN (NATIONWIDE, FOCUSING ON NUCLEAR SITES)",
    country: "Iran",
    severity: "CRITICAL",
    date: "Mar 28",
    timeOfDay: "early morning",
    description: "The Israeli military announces it is conducting strikes on targets across Tehran and other parts of Iran.",
    timeAgo: "~1h ago"
  },
  {
    id: "3",
    location: "IRANIAN REGIME GOVERNMENTAL CENTERS",
    country: "Iran",
    severity: "CRITICAL",
    date: "Mar 28",
    timeOfDay: "night",
    description: "The Israeli military announced it is conducting strikes on targets across Tehran and other parts of Iran.",
    timeAgo: "~1h ago"
  },
  {
    id: "4",
    location: "TEHRAN, ISFAHAN, SHIRAZ, DEZFUL",
    country: "Iran",
    severity: "CRITICAL",
    date: "Mar 28",
    timeOfDay: "night",
    description: "Strikes were reported across Iran in the cities of Tehran, Isfahan, Shiraz, and Dezful, as Israeli and U.S. strikes targeted Iranian regime, IRGC, and other infrastructure throughout the nation.",
    timeAgo: "~1h ago"
  },
  {
    id: "5",
    location: "ISFAHAN AIR FORCE BASE",
    country: "Iran",
    severity: "CRITICAL",
    date: "Mar 28",
    timeOfDay: "night",
    description: "Strikes are reported across Iran, including in Isfahan, targeting Iranian regime, IRGC, and other infrastructure.",
    timeAgo: "~1h ago"
  },
  {
    id: "6",
    location: "SHIRAZ",
    country: "Iran",
    severity: "ELEVATED",
    date: "Mar 28",
    timeOfDay: "morning",
    description: "Strikes reported in Shiraz as part of a broader campaign targeting Iranian regime and IRGC infrastructure.",
    timeAgo: "~1h ago"
  },
  {
    id: "7",
    location: "DEZFUL",
    country: "Iran",
    severity: "ELEVATED",
    date: "Mar 28",
    timeOfDay: "morning",
    description: "Military activity reported in Dezful region as part of ongoing operations.",
    timeAgo: "~1h ago"
  },
  {
    id: "8",
    location: "STRAIT OF HORMUZ",
    country: "International Waters",
    severity: "ELEVATED",
    date: "Mar 28",
    timeOfDay: "afternoon",
    description: "Naval activity detected in the Strait of Hormuz with multiple vessels repositioning.",
    timeAgo: "~2h ago"
  },
  {
    id: "9",
    location: "DAMASCUS INTERNATIONAL AIRPORT",
    country: "Syria",
    severity: "ACTIVE",
    date: "Mar 28",
    timeOfDay: "evening",
    description: "Flights suspended following reports of military activity in the region.",
    timeAgo: "~3h ago"
  },
]

const getSeverityConfig = (severity: WireEvent["severity"]) => {
  switch (severity) {
    case "CRITICAL":
      return {
        color: "#ef4444",
        bgColor: "rgba(239, 68, 68, 0.08)",
        borderColor: "rgba(239, 68, 68, 0.2)",
        icon: AlertTriangle,
        label: "CRITICAL"
      }
    case "ELEVATED":
      return {
        color: "#f59e0b",
        bgColor: "rgba(245, 158, 11, 0.08)",
        borderColor: "rgba(245, 158, 11, 0.2)",
        icon: Radio,
        label: "ELEVATED"
      }
    case "ACTIVE":
      return {
        color: "#22c55e",
        bgColor: "rgba(34, 197, 94, 0.08)",
        borderColor: "rgba(34, 197, 94, 0.2)",
        icon: Activity,
        label: "ACTIVE"
      }
  }
}

export function WireView() {
  const criticalCount = wireEvents.filter(e => e.severity === "CRITICAL").length
  const elevatedCount = wireEvents.filter(e => e.severity === "ELEVATED").length
  const activeCount = wireEvents.filter(e => e.severity === "ACTIVE").length

  return (
    <div className="absolute inset-0 pt-14 pb-0 overflow-hidden bg-[#09090b]">
      {/* Subtle background pattern */}
      <div 
        className="absolute inset-0 opacity-[0.02]"
        style={{
          backgroundImage: `radial-gradient(circle at 1px 1px, white 1px, transparent 0)`,
          backgroundSize: '32px 32px'
        }}
      />
      
      <div className="relative h-full overflow-y-auto">
        {/* Sticky header */}
        <div className="sticky top-0 z-20 bg-[#09090b]/95 backdrop-blur-md border-b border-[#18181b]">
          <div className="max-w-3xl mx-auto px-6 py-5">
            <div className="flex items-center justify-between">
              {/* Title */}
              <div className="flex items-center gap-3">
                <div className="w-2 h-2 rounded-full bg-[#22c55e] animate-pulse" />
                <h1 className="text-white text-sm font-semibold tracking-wide">LIVE FEED</h1>
              </div>
              
              {/* Stats */}
              <div className="flex items-center gap-6">
                <div className="flex items-center gap-2">
                  <div className="w-2 h-2 rounded-full bg-[#ef4444]" />
                  <span className="text-[11px] text-[#71717a] tracking-wide">
                    <span className="text-[#ef4444] font-semibold">{criticalCount}</span> Critical
                  </span>
                </div>
                <div className="flex items-center gap-2">
                  <div className="w-2 h-2 rounded-full bg-[#f59e0b]" />
                  <span className="text-[11px] text-[#71717a] tracking-wide">
                    <span className="text-[#f59e0b] font-semibold">{elevatedCount}</span> Elevated
                  </span>
                </div>
                <div className="flex items-center gap-2">
                  <div className="w-2 h-2 rounded-full bg-[#22c55e]" />
                  <span className="text-[11px] text-[#71717a] tracking-wide">
                    <span className="text-[#22c55e] font-semibold">{activeCount}</span> Active
                  </span>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Events list */}
        <div className="max-w-3xl mx-auto px-6 py-6">
          <div className="space-y-3">
            {wireEvents.map((event, index) => {
              const config = getSeverityConfig(event.severity)
              const Icon = config.icon
              
              return (
                <article
                  key={event.id}
                  className="group relative overflow-hidden rounded-2xl border border-[#18181b] bg-[#0c0c0e]/92 transition-all duration-300 cursor-pointer hover:-translate-y-0.5 hover:border-[#27272a] hover:shadow-[0_18px_50px_rgba(0,0,0,0.35)]"
                  style={{
                    boxShadow: `inset 0 0 0 1px rgba(255,255,255,0.02), inset 0 0 30px ${config.bgColor}`,
                  }}
                >
                  <div
                    className="absolute inset-0 opacity-80"
                    style={{
                      background: `linear-gradient(135deg, ${config.bgColor} 0%, rgba(12,12,14,0) 42%)`,
                    }}
                  />
                  <div
                    className="absolute left-0 top-0 bottom-0 w-px"
                    style={{ background: `linear-gradient(180deg, ${config.color} 0%, transparent 100%)` }}
                  />
                  <div className="relative p-5">
                    {/* Top row */}
                    <div className="mb-4 flex items-start justify-between gap-4">
                      <div className="flex items-center gap-2.5">
                        <div
                          className="flex h-8 w-8 flex-shrink-0 items-center justify-center rounded-xl border"
                          style={{ color: config.color }}
                        >
                          <Icon className="h-3.5 w-3.5" />
                        </div>
                        <span
                          className="rounded-full px-2.5 py-1 text-[10px] font-bold tracking-[0.22em]"
                          style={{
                            color: config.color,
                            background: config.bgColor,
                            border: `1px solid ${config.borderColor}`,
                          }}
                        >
                          {config.label}
                        </span>
                        <span className="rounded-full border border-[#27272a] bg-[#09090b]/80 px-2.5 py-1 text-[10px] text-[#71717a]">
                          {event.country}
                        </span>
                      </div>

                      <div className="flex items-center gap-1.5 rounded-full border border-[#27272a] bg-[#09090b]/80 px-2.5 py-1 text-[10px] text-[#71717a]">
                        <Clock className="w-3 h-3" />
                        <span className="font-mono">{event.timeAgo}</span>
                      </div>
                    </div>

                    {/* Location */}
                    <div className="mb-3 flex items-start gap-3">
                      <div className="mt-0.5 flex h-7 w-7 flex-shrink-0 items-center justify-center rounded-lg border border-[#27272a] bg-[#09090b]/80">
                        <MapPin className="h-3.5 w-3.5 text-[#71717a]" />
                      </div>
                      <h3 className="text-[13px] font-semibold tracking-[0.12em] text-white">
                        {event.location}
                      </h3>
                    </div>

                    {/* Time context */}
                    <div className="mb-3 flex flex-wrap items-center gap-2 pl-10">
                      <span className="rounded-full border border-[#27272a] bg-[#09090b]/70 px-2 py-1 text-[10px] text-[#71717a] tracking-wide">
                        {event.date}
                      </span>
                      <span className="rounded-full border border-[#27272a] bg-[#09090b]/70 px-2 py-1 text-[10px] text-[#71717a] tracking-wide">
                        {event.timeOfDay}
                      </span>
                    </div>

                    {/* Description */}
                    <p className="pl-10 text-[12px] leading-6 text-[#d4d4d8]">
                      {event.description}
                    </p>
                  </div>

                  {/* Hover indicator */}
                  <div
                    className="absolute right-5 top-5 opacity-0 transition-all duration-300 group-hover:translate-x-0 group-hover:opacity-100"
                    style={{ color: config.color }}
                  >
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                      <path d="M9 18l6-6-6-6" />
                    </svg>
                  </div>
                </article>
              )
            })}
          </div>
        </div>
        
        {/* Bottom spacing for news ticker */}
        <div className="h-8" />
      </div>
      
      {/* Top fade */}
      <div className="absolute top-14 left-0 right-0 h-16 bg-gradient-to-b from-[#09090b] to-transparent pointer-events-none z-10" />
      
      {/* Bottom fade */}
      <div className="absolute bottom-14 left-0 right-0 h-16 bg-gradient-to-t from-[#09090b] to-transparent pointer-events-none" />
    </div>
  )
}
