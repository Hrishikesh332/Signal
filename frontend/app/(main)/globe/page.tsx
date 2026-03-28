"use client"

import dynamic from "next/dynamic"

const GlobeScene = dynamic(
  () => import("@/components/market-signal/globe-scene").then((mod) => mod.GlobeCanvas),
  {
    ssr: false,
    loading: () => (
      <div className="absolute inset-0 pt-14 pb-0 bg-[#09090b] flex items-center justify-center">
        <div className="text-[#22c55e] animate-pulse text-[11px] tracking-wide">Initializing globe...</div>
      </div>
    ),
  }
)

export default function GlobePage() {
  return <GlobeScene />
}
