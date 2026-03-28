"use client"

interface CoordinateDisplayProps {
  lat: number | null
  lng: number | null
}

function formatDMS(decimal: number, isLat: boolean): string {
  const absolute = Math.abs(decimal)
  const degrees = Math.floor(absolute)
  const minutesDecimal = (absolute - degrees) * 60
  const minutes = minutesDecimal.toFixed(2)
  const direction = isLat
    ? decimal >= 0 ? "N" : "S"
    : decimal >= 0 ? "E" : "W"
  
  return `${degrees}° ${minutes} ${direction}`
}

export function CoordinateDisplay({ lat, lng }: CoordinateDisplayProps) {
  if (lat === null || lng === null) {
    return null
  }

  return (
    <div className="rounded border border-[#1a3a2a] bg-[#0c1a14]/90 px-4 py-3 font-mono text-xs backdrop-blur-sm">
      <div className="flex flex-col items-end gap-1">
        <div className="text-[#22c55e]">{formatDMS(lat, true)}</div>
        <div className="text-[#22c55e]">{formatDMS(lng, false)}</div>
        <div className="text-[#22c55e]/60">
          ({lat.toFixed(6)}, {lng.toFixed(6)})
        </div>
      </div>
    </div>
  )
}
