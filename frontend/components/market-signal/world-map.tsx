"use client"

import { useEffect, useRef, useState } from "react"
import L from "leaflet"
import "leaflet/dist/leaflet.css"

type Severity = "low" | "medium" | "high" | "critical"

type DashboardMapPoint = {
  latitude: number
  longitude: number
  entity_name: string
  signal_type: string
  severity: Severity | string
  timestamp: string
  explanation: string
}

function normalizeSeverity(value: string): Severity {
  const v = value.toLowerCase()
  if (v === "critical" || v === "high" || v === "medium" || v === "low") return v
  return "low"
}

const getSignalColor = (severity: Severity) => {
  switch (severity) {
    case "low":
      return "#52525b"
    case "medium":
      return "#a1a1aa"
    case "high":
      return "#d4d4d8"
    case "critical":
      return "#ffffff"
    default:
      return "#52525b"
  }
}

const getSignalSize = (severity: Severity) => {
  switch (severity) {
    case "critical":
      return 60
    case "high":
      return 52
    case "medium":
      return 40
    case "low":
      return 28
    default:
      return 28
  }
}

function escapeHtml(text: string): string {
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
}

async function fetchDashboardMapPoints(): Promise<DashboardMapPoint[]> {
  const response = await fetch("/api/dashboard", { cache: "no-store" })
  const payload = (await response.json()) as { map?: { points?: DashboardMapPoint[] }; error?: { message?: string } }
  if (!response.ok) {
    const message = "error" in payload ? payload.error?.message : "Unable to load dashboard data."
    throw new Error(message || "Unable to load dashboard data.")
  }
  return payload.map?.points ?? []
}

interface WorldMapProps {
  onCoordinateChange?: (lat: number, lng: number) => void
  mapRef?: React.MutableRefObject<L.Map | null>
}

export function WorldMap({ onCoordinateChange, mapRef }: WorldMapProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const internalMapRef = useRef<L.Map | null>(null)
  const markersLayerRef = useRef<L.LayerGroup | null>(null)
  const isUnmountedRef = useRef(false)
  const [isClient, setIsClient] = useState(false)
  const [mapPoints, setMapPoints] = useState<DashboardMapPoint[]>([])
  const [mapReady, setMapReady] = useState(false)
  const [pointsError, setPointsError] = useState<string | null>(null)

  useEffect(() => {
    setIsClient(true)
    return () => {
      isUnmountedRef.current = true
    }
  }, [])

  useEffect(() => {
    let active = true
    setPointsError(null)
    fetchDashboardMapPoints()
      .then((points) => {
        if (!active) return
        setMapPoints(points)
      })
      .catch((err: unknown) => {
        if (!active) return
        setMapPoints([])
        setPointsError(err instanceof Error ? err.message : "Unable to load map signals.")
      })
    return () => {
      active = false
    }
  }, [])

  useEffect(() => {
    if (!isClient || !containerRef.current || internalMapRef.current) return

    isUnmountedRef.current = false

    const map = L.map(containerRef.current, {
      center: [20, 0],
      zoom: 2,
      minZoom: 2,
      maxZoom: 18,
      zoomControl: false,
      attributionControl: false,
    })

    L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png", {
      maxZoom: 19,
    }).addTo(map)

    const markersLayer = L.layerGroup().addTo(map)
    markersLayerRef.current = markersLayer

    const handleMouseMove = (e: L.LeafletMouseEvent) => {
      if (!isUnmountedRef.current && e.latlng) {
        onCoordinateChange?.(e.latlng.lat, e.latlng.lng)
      }
    }
    map.on("mousemove", handleMouseMove)

    internalMapRef.current = map
    if (mapRef) {
      mapRef.current = map
    }
    setMapReady(true)

    return () => {
      isUnmountedRef.current = true
      markersLayerRef.current = null

      if (mapRef) {
        mapRef.current = null
      }
      internalMapRef.current = null

      setTimeout(() => {
        try {
          const originalWarn = console.warn
          const originalError = console.error
          console.warn = () => {}
          console.error = () => {}

          map.off()
          map.remove()

          console.warn = originalWarn
          console.error = originalError
        } catch {
          // Ignore cleanup errors during unmount
        }
      }, 0)
    }
  }, [isClient, onCoordinateChange, mapRef])

  useEffect(() => {
    if (!mapReady || !markersLayerRef.current) return
    const layer = markersLayerRef.current
    layer.clearLayers()

    for (const point of mapPoints) {
      const severity = normalizeSeverity(String(point.severity))
      const size = getSignalSize(severity)
      const color = getSignalColor(severity)
      const label = escapeHtml(point.entity_name.trim().slice(0, 2) || "·")

      const icon = L.divIcon({
        className: "signal-marker",
        html: `
          <div style="
            width: ${size}px;
            height: ${size}px;
            background-color: ${color};
            border: 2px solid ${color}44;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-family: 'Space Mono', monospace;
            font-size: ${size > 44 ? 14 : size > 32 ? 12 : 11}px;
            font-weight: bold;
            color: #09090b;
            box-shadow: 0 0 ${size / 3}px ${color}88, inset 0 0 ${size / 4}px ${color}22;
            cursor: pointer;
            transition: transform 0.2s, box-shadow 0.2s;
          " onmouseover="this.style.transform='scale(1.1)'; this.style.boxShadow='0 0 ${size / 2}px ${color}bb, inset 0 0 ${size / 3}px ${color}44';" onmouseout="this.style.transform='scale(1)'; this.style.boxShadow='0 0 ${size / 3}px ${color}88, inset 0 0 ${size / 4}px ${color}22';">
            ${label}
          </div>
        `,
        iconSize: [size, size],
        iconAnchor: [size / 2, size / 2],
      })

      const marker = L.marker([point.latitude, point.longitude], { icon }).addTo(layer)
      marker.bindPopup(
        `<div style="font-family: system-ui,sans-serif; font-size:12px; max-width:240px;">
          <div style="font-weight:600;margin-bottom:4px;">${escapeHtml(point.entity_name)}</div>
          <div style="opacity:0.85;margin-bottom:6px;">${escapeHtml(point.explanation)}</div>
          <div style="font-size:10px;text-transform:uppercase;letter-spacing:0.06em;opacity:0.7;">${escapeHtml(point.signal_type)} · ${escapeHtml(String(point.severity))}</div>
        </div>`
      )
    }
  }, [mapPoints, mapReady])

  if (!isClient) {
    return (
      <div className="w-full h-full bg-[#09090b] flex items-center justify-center">
        <div className="text-white animate-pulse text-[11px] tracking-wide">Initializing map...</div>
      </div>
    )
  }

  return (
    <div className="relative h-full w-full">
      {pointsError ? (
        <div className="pointer-events-none absolute bottom-3 left-3 right-3 z-[1000] rounded border border-zinc-700 bg-zinc-950/90 px-3 py-2 text-[11px] text-zinc-300">
          {pointsError}
        </div>
      ) : null}
      <div ref={containerRef} className="h-full w-full" style={{ background: "#09090b" }} />
    </div>
  )
}
