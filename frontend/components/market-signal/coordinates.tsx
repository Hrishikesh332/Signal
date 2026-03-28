"use client"

interface CoordinatesProps {
  lat: number
  lng: number
}

export function Coordinates({ lat, lng }: CoordinatesProps) {
  const formatDMS = (decimal: number, isLat: boolean) => {
    const absolute = Math.abs(decimal)
    const degrees = Math.floor(absolute)
    const minutes = ((absolute - degrees) * 60).toFixed(2)
    const direction = isLat 
      ? (decimal >= 0 ? "N" : "S")
      : (decimal >= 0 ? "E" : "W")
    return `${degrees}° ${minutes} ${direction}`
  }

  return (
    <div className="absolute right-40 bottom-4 z-40 bg-[#09090b]/95 backdrop-blur-sm border border-[#27272a] rounded-lg px-4 py-3">
      <div className="text-right space-y-0.5">
        <div className="text-white font-mono text-[11px] tracking-tight">
          {formatDMS(lat, true)}
        </div>
        <div className="text-white font-mono text-[11px] tracking-tight">
          {formatDMS(lng, false)}
        </div>
        <div className="text-[#71717a] text-[10px] mt-1.5 font-mono">
          ({lat.toFixed(6)}, {lng.toFixed(6)})
        </div>
      </div>
    </div>
  )
}
