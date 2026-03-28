"use client"

import type L from "leaflet"
import dynamic from "next/dynamic"
import { useRef } from "react"
import { SearchBar } from "@/components/market-signal/search-bar"

const WorldMap = dynamic(
  () => import("@/components/market-signal/world-map").then((mod) => mod.WorldMap),
  {
    ssr: false,
    loading: () => (
      <div className="w-full h-full bg-[#09090b] flex items-center justify-center">
        <div className="text-white animate-pulse text-[11px] tracking-wide">Initializing map...</div>
      </div>
    ),
  }
)

export default function DashboardPage() {
  const mapRef = useRef<L.Map | null>(null)

  return (
    <>
      <SearchBar mapRef={mapRef} />
      <div className="absolute top-14 left-0 right-0 bottom-0 z-0">
        <WorldMap mapRef={mapRef} />
      </div>
    </>
  )
}
