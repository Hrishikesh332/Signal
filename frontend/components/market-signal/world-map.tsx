"use client"

import { useEffect, useRef, useState } from "react"
import L from "leaflet"
import "leaflet/dist/leaflet.css"

interface Signal {
  id: string
  lat: number
  lng: number
  count: number
  intensity: "low" | "medium" | "high" | "critical"
}

// Market signals data across the globe
const signals: Signal[] = [
  { id: "1", lat: 51.5074, lng: -0.1278, count: 85, intensity: "high" },
  { id: "2", lat: 48.8566, lng: 2.3522, count: 107, intensity: "high" },
  { id: "3", lat: 40.7128, lng: -74.006, count: 135, intensity: "critical" },
  { id: "4", lat: 35.6762, lng: 139.6503, count: 8, intensity: "low" },
  { id: "5", lat: 22.3193, lng: 114.1694, count: 50, intensity: "medium" },
  { id: "6", lat: 1.3521, lng: 103.8198, count: 19, intensity: "low" },
  { id: "7", lat: -33.8688, lng: 151.2093, count: 9, intensity: "low" },
  { id: "8", lat: 55.7558, lng: 37.6173, count: 4, intensity: "low" },
  { id: "9", lat: 19.076, lng: 72.8777, count: 104, intensity: "critical" },
  { id: "10", lat: 31.2304, lng: 121.4737, count: 3, intensity: "low" },
  { id: "11", lat: 52.52, lng: 13.405, count: 14, intensity: "low" },
  { id: "12", lat: -23.5505, lng: -46.6333, count: 59, intensity: "medium" },
  { id: "13", lat: 25.2048, lng: 55.2708, count: 25, intensity: "medium" },
  { id: "14", lat: 41.9028, lng: 12.4964, count: 4, intensity: "low" },
  { id: "15", lat: 37.5665, lng: 126.978, count: 3, intensity: "low" },
  { id: "16", lat: -34.6037, lng: -58.3816, count: 14, intensity: "low" },
  { id: "17", lat: 50.8503, lng: 4.3517, count: 8, intensity: "low" },
  { id: "18", lat: 59.3293, lng: 18.0686, count: 4, intensity: "low" },
  { id: "19", lat: 39.9042, lng: 116.4074, count: 4, intensity: "low" },
  { id: "20", lat: -1.2921, lng: 36.8219, count: 12, intensity: "low" },
  { id: "21", lat: 6.5244, lng: 3.3792, count: 17, intensity: "low" },
  { id: "22", lat: -26.2041, lng: 28.0473, count: 7, intensity: "low" },
  { id: "23", lat: 30.0444, lng: 31.2357, count: 6, intensity: "low" },
  { id: "24", lat: 33.8938, lng: 35.5018, count: 25, intensity: "medium" },
  { id: "25", lat: 64.1466, lng: -21.9426, count: 5, intensity: "low" },
  { id: "26", lat: 45.4215, lng: -75.6972, count: 39, intensity: "medium" },
  { id: "27", lat: 19.4326, lng: -99.1332, count: 11, intensity: "low" },
  { id: "28", lat: -12.0464, lng: -77.0428, count: 13, intensity: "low" },
  { id: "29", lat: 4.711, lng: -74.0721, count: 17, intensity: "low" },
  { id: "30", lat: -33.4489, lng: -70.6693, count: 14, intensity: "low" },
  { id: "31", lat: 47.4979, lng: 19.0402, count: 4, intensity: "low" },
  { id: "32", lat: 38.7223, lng: -9.1393, count: 2, intensity: "low" },
  { id: "33", lat: 60.1699, lng: 24.9384, count: 3, intensity: "low" },
  { id: "34", lat: -6.2088, lng: 106.8456, count: 10, intensity: "low" },
  { id: "35", lat: 13.7563, lng: 100.5018, count: 4, intensity: "low" },
  { id: "36", lat: 14.5995, lng: 120.9842, count: 2, intensity: "low" },
  { id: "37", lat: 3.139, lng: 101.6869, count: 9, intensity: "low" },
  { id: "38", lat: 21.0278, lng: 105.8342, count: 2, intensity: "low" },
]

const getSignalColor = (intensity: Signal["intensity"]) => {
  switch (intensity) {
    case "low":
      return "#52525b" // dark gray
    case "medium":
      return "#a1a1aa" // medium gray
    case "high":
      return "#d4d4d8" // light gray
    case "critical":
      return "#ffffff" // white
    default:
      return "#52525b"
  }
}

const getSignalSize = (count: number) => {
  if (count < 10) return 28
  if (count < 30) return 36
  if (count < 60) return 44
  if (count < 100) return 52
  return 60
}

interface WorldMapProps {
  onCoordinateChange?: (lat: number, lng: number) => void
  mapRef?: React.MutableRefObject<L.Map | null>
}

export function WorldMap({ onCoordinateChange, mapRef }: WorldMapProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const internalMapRef = useRef<L.Map | null>(null)
  const isUnmountedRef = useRef(false)
  const [isClient, setIsClient] = useState(false)

  useEffect(() => {
    setIsClient(true)
    return () => {
      isUnmountedRef.current = true
    }
  }, [])

  useEffect(() => {
    if (!isClient || !containerRef.current || internalMapRef.current) return

    // Reset unmounted flag
    isUnmountedRef.current = false

    // Create map with dark theme
    const map = L.map(containerRef.current, {
      center: [20, 0],
      zoom: 2,
      minZoom: 2,
      maxZoom: 18,
      zoomControl: false,
      attributionControl: false,
    })

    // Dark theme map tiles
    L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png", {
      maxZoom: 19,
    }).addTo(map)

    // Add signal markers
    signals.forEach((signal) => {
      const size = getSignalSize(signal.count)
      const color = getSignalColor(signal.intensity)

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
            ${signal.count}
          </div>
        `,
        iconSize: [size, size],
        iconAnchor: [size / 2, size / 2],
      })

      L.marker([signal.lat, signal.lng], { icon }).addTo(map)
    })

    // Track mouse position with guard
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

    return () => {
      // Set unmount flag first
      isUnmountedRef.current = true
      
      // Clear refs immediately
      if (mapRef) {
        mapRef.current = null
      }
      internalMapRef.current = null
      
      // Defer cleanup to avoid DOM access during unmount
      setTimeout(() => {
        try {
          // Suppress Leaflet internal errors during cleanup
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

  if (!isClient) {
    return (
      <div className="w-full h-full bg-[#09090b] flex items-center justify-center">
        <div className="text-white animate-pulse text-[11px] tracking-wide">Initializing map...</div>
      </div>
    )
  }

  return (
    <div 
      ref={containerRef} 
      className="w-full h-full"
      style={{ background: "#09090b" }}
    />
  )
}
