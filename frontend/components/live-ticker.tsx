"use client"

import { useEffect, useState, useRef } from "react"

const headlines = [
  "UNDER NEW SECURITY POWERS TO ENSURE SUPPLY",
  "BREONNA TAYLOR SHOOTING: CHARGES DISMISSED AGAINST EX-POLICE OFFICERS FOR FALSIFYING WARRANT",
  "CONNECTICUT OFFICER FIRED AFTER SHOOTING MAN IN MENTAL HEALTH CRISIS",
  "GLOBAL MARKETS RALLY AS FED SIGNALS RATE PAUSE",
  "EARTHQUAKE DETECTED OFF COAST OF JAPAN, NO TSUNAMI WARNING ISSUED",
  "MAJOR CYBERATTACK TARGETS EUROPEAN FINANCIAL INSTITUTIONS",
  "DIPLOMATIC TALKS RESUME IN MIDDLE EAST PEACE NEGOTIATIONS",
  "SEVERE WEATHER WARNING ISSUED FOR MIDWEST UNITED STATES",
]

export function LiveTicker() {
  const [offset, setOffset] = useState(0)
  const containerRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const interval = setInterval(() => {
      setOffset((prev) => prev - 1)
    }, 30)

    return () => clearInterval(interval)
  }, [])

  const combinedText = headlines.join("  •  ")

  return (
    <div className="flex h-8 items-center border-t border-[#1a3a2a] bg-[#0a1612]">
      <div className="flex shrink-0 items-center gap-2 border-r border-[#1a3a2a] px-3">
        <span className="relative flex h-2 w-2">
          <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-red-400 opacity-75"></span>
          <span className="relative inline-flex h-2 w-2 rounded-full bg-red-500"></span>
        </span>
        <span className="font-mono text-xs font-bold text-red-500">LIVE</span>
      </div>
      <div
        ref={containerRef}
        className="flex-1 overflow-hidden"
      >
        <div
          className="flex whitespace-nowrap font-mono text-xs text-[#22c55e]/80"
          style={{
            transform: `translateX(${offset % (combinedText.length * 8)}px)`,
          }}
        >
          <span className="px-4">{combinedText}</span>
          <span className="px-4">{combinedText}</span>
        </div>
      </div>
      <div className="shrink-0 border-l border-[#1a3a2a] px-3 font-mono text-[10px] text-[#22c55e]/60">
        Leaflet | OpenStreetMap contributors | CARTO | adsb.lol
      </div>
    </div>
  )
}
