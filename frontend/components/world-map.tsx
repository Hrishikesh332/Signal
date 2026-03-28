"use client"

import { useCallback, useEffect, useRef, useState } from "react"

interface ClusterMarker {
  id: string
  lat: number
  lng: number
  count: number
  color: "green" | "yellow" | "orange" | "cyan" | "red"
}

const clusterMarkers: ClusterMarker[] = [
  { id: "1", lat: 38, lng: -122, count: 21, color: "green" },
  { id: "2", lat: 40, lng: -100, count: 28, color: "yellow" },
  { id: "3", lat: 35, lng: -85, count: 51, color: "yellow" },
  { id: "4", lat: 25, lng: -80, count: 4, color: "cyan" },
  { id: "5", lat: -5, lng: -70, count: 7, color: "green" },
  { id: "6", lat: -10, lng: -55, count: 2, color: "cyan" },
  { id: "7", lat: -20, lng: -45, count: 4, color: "green" },
  { id: "8", lat: -30, lng: -60, count: 4, color: "green" },
  { id: "9", lat: 50, lng: 10, count: 19, color: "yellow" },
  { id: "10", lat: 55, lng: 25, count: 15, color: "yellow" },
  { id: "11", lat: 52, lng: 40, count: 10, color: "yellow" },
  { id: "12", lat: 45, lng: 0, count: 4, color: "cyan" },
  { id: "13", lat: 25, lng: 30, count: 45, color: "orange" },
  { id: "14", lat: 5, lng: 35, count: 62, color: "cyan" },
  { id: "15", lat: 10, lng: 50, count: 7, color: "orange" },
  { id: "16", lat: -5, lng: 25, count: 2, color: "yellow" },
  { id: "17", lat: -25, lng: 25, count: 3, color: "green" },
  { id: "18", lat: -30, lng: 30, count: 2, color: "cyan" },
  { id: "19", lat: 20, lng: 80, count: 16, color: "cyan" },
  { id: "20", lat: 35, lng: 105, count: 2, color: "green" },
  { id: "21", lat: 40, lng: 120, count: 6, color: "green" },
  { id: "22", lat: 25, lng: 125, count: 4, color: "orange" },
  { id: "23", lat: 5, lng: 110, count: 3, color: "green" },
  { id: "24", lat: 65, lng: 170, count: 1, color: "cyan" },
  { id: "25", lat: 70, lng: -160, count: 1, color: "red" },
  { id: "26", lat: -25, lng: 135, count: 3, color: "green" },
  { id: "27", lat: -35, lng: 145, count: 5, color: "green" },
]

const colorMap = {
  green: { fill: "#22c55e", glow: "rgba(34, 197, 94, 0.4)" },
  yellow: { fill: "#eab308", glow: "rgba(234, 179, 8, 0.4)" },
  orange: { fill: "#f97316", glow: "rgba(249, 115, 22, 0.4)" },
  cyan: { fill: "#06b6d4", glow: "rgba(6, 182, 212, 0.4)" },
  red: { fill: "#ef4444", glow: "rgba(239, 68, 68, 0.4)" },
}

const STATIC_ZOOM = 1
const STATIC_OFFSET = { x: 0, y: 0 }

interface WorldMapProps {
  onCoordinateChange?: (coords: { lat: number; lng: number } | null) => void
}

export function WorldMap({ onCoordinateChange }: WorldMapProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const [dimensions, setDimensions] = useState({ width: 0, height: 0 })
  const zoom = STATIC_ZOOM
  const offset = STATIC_OFFSET
  const animationRef = useRef<number>(0)

  useEffect(() => {
    const updateDimensions = () => {
      if (canvasRef.current?.parentElement) {
        const rect = canvasRef.current.parentElement.getBoundingClientRect()
        setDimensions({ width: rect.width, height: rect.height })
      }
    }

    updateDimensions()
    window.addEventListener("resize", updateDimensions)
    return () => window.removeEventListener("resize", updateDimensions)
  }, [])

  const latToY = useCallback((lat: number) => {
    return ((90 - lat) / 180) * dimensions.height * zoom + offset.y
  }, [dimensions.height, zoom, offset.y])

  const lngToX = useCallback((lng: number) => {
    return ((lng + 180) / 360) * dimensions.width * zoom + offset.x
  }, [dimensions.width, zoom, offset.x])

  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas || dimensions.width === 0) return

    const ctx = canvas.getContext("2d")
    if (!ctx) return

    const dpr = window.devicePixelRatio || 1
    canvas.width = dimensions.width * dpr
    canvas.height = dimensions.height * dpr
    ctx.scale(dpr, dpr)

    const drawGrid = () => {
      ctx.strokeStyle = "rgba(34, 197, 94, 0.06)"
      ctx.lineWidth = 0.5

      for (let lat = -80; lat <= 80; lat += 20) {
        ctx.beginPath()
        ctx.moveTo(0, latToY(lat))
        ctx.lineTo(dimensions.width, latToY(lat))
        ctx.stroke()
      }

      for (let lng = -180; lng <= 180; lng += 30) {
        ctx.beginPath()
        ctx.moveTo(lngToX(lng), 0)
        ctx.lineTo(lngToX(lng), dimensions.height)
        ctx.stroke()
      }
    }

    const drawRegionLabels = () => {
      ctx.font = "bold 24px system-ui"
      ctx.fillStyle = "rgba(34, 197, 94, 0.12)"
      ctx.textAlign = "center"

      const labels = [
        { text: "NORTH", lat: 55, lng: -100 },
        { text: "AMERICA", lat: 45, lng: -100 },
        { text: "AMERICA", lat: -5, lng: -60 },
        { text: "DO SUL/AMERICA", lat: -15, lng: -55 },
        { text: "DEL SUL", lat: -25, lng: -55 },
        { text: "EUROPE", lat: 50, lng: 25 },
        { text: "AFRIKA /", lat: 15, lng: 20 },
        { text: "أفريقيا", lat: 5, lng: 20 },
        { text: "亚洲", lat: 45, lng: 100 },
        { text: "AUSTRALIA", lat: -30, lng: 135 },
        { text: "OCEA", lat: -20, lng: 165 },
      ]

      labels.forEach(({ text, lat, lng }) => {
        ctx.fillText(text, lngToX(lng), latToY(lat))
      })
    }

    const drawContinents = () => {
      ctx.fillStyle = "rgba(34, 197, 94, 0.15)"
      ctx.strokeStyle = "rgba(34, 197, 94, 0.25)"
      ctx.lineWidth = 1

      // North America
      ctx.beginPath()
      ctx.moveTo(lngToX(-170), latToY(65))
      ctx.lineTo(lngToX(-140), latToY(72))
      ctx.lineTo(lngToX(-95), latToY(72))
      ctx.lineTo(lngToX(-75), latToY(75))
      ctx.lineTo(lngToX(-60), latToY(60))
      ctx.lineTo(lngToX(-55), latToY(48))
      ctx.lineTo(lngToX(-70), latToY(45))
      ctx.lineTo(lngToX(-75), latToY(35))
      ctx.lineTo(lngToX(-80), latToY(25))
      ctx.lineTo(lngToX(-90), latToY(18))
      ctx.lineTo(lngToX(-105), latToY(20))
      ctx.lineTo(lngToX(-115), latToY(28))
      ctx.lineTo(lngToX(-125), latToY(48))
      ctx.lineTo(lngToX(-140), latToY(60))
      ctx.lineTo(lngToX(-165), latToY(60))
      ctx.closePath()
      ctx.fill()
      ctx.stroke()

      // Greenland
      ctx.beginPath()
      ctx.moveTo(lngToX(-45), latToY(60))
      ctx.lineTo(lngToX(-20), latToY(65))
      ctx.lineTo(lngToX(-20), latToY(80))
      ctx.lineTo(lngToX(-55), latToY(80))
      ctx.lineTo(lngToX(-55), latToY(70))
      ctx.closePath()
      ctx.fill()
      ctx.stroke()

      // South America
      ctx.beginPath()
      ctx.moveTo(lngToX(-80), latToY(12))
      ctx.lineTo(lngToX(-60), latToY(10))
      ctx.lineTo(lngToX(-35), latToY(-5))
      ctx.lineTo(lngToX(-35), latToY(-15))
      ctx.lineTo(lngToX(-40), latToY(-22))
      ctx.lineTo(lngToX(-48), latToY(-28))
      ctx.lineTo(lngToX(-55), latToY(-35))
      ctx.lineTo(lngToX(-68), latToY(-55))
      ctx.lineTo(lngToX(-75), latToY(-50))
      ctx.lineTo(lngToX(-70), latToY(-18))
      ctx.lineTo(lngToX(-78), latToY(-5))
      ctx.lineTo(lngToX(-80), latToY(0))
      ctx.closePath()
      ctx.fill()
      ctx.stroke()

      // Europe
      ctx.beginPath()
      ctx.moveTo(lngToX(-10), latToY(58))
      ctx.lineTo(lngToX(-5), latToY(50))
      ctx.lineTo(lngToX(0), latToY(43))
      ctx.lineTo(lngToX(5), latToY(45))
      ctx.lineTo(lngToX(15), latToY(38))
      ctx.lineTo(lngToX(25), latToY(38))
      ctx.lineTo(lngToX(30), latToY(42))
      ctx.lineTo(lngToX(40), latToY(42))
      ctx.lineTo(lngToX(30), latToY(55))
      ctx.lineTo(lngToX(25), latToY(58))
      ctx.lineTo(lngToX(30), latToY(65))
      ctx.lineTo(lngToX(25), latToY(72))
      ctx.lineTo(lngToX(5), latToY(62))
      ctx.lineTo(lngToX(-5), latToY(60))
      ctx.closePath()
      ctx.fill()
      ctx.stroke()

      // Africa
      ctx.beginPath()
      ctx.moveTo(lngToX(-17), latToY(35))
      ctx.lineTo(lngToX(-5), latToY(36))
      ctx.lineTo(lngToX(10), latToY(37))
      ctx.lineTo(lngToX(12), latToY(33))
      ctx.lineTo(lngToX(25), latToY(32))
      ctx.lineTo(lngToX(35), latToY(30))
      ctx.lineTo(lngToX(43), latToY(12))
      ctx.lineTo(lngToX(52), latToY(12))
      ctx.lineTo(lngToX(42), latToY(-2))
      ctx.lineTo(lngToX(40), latToY(-12))
      ctx.lineTo(lngToX(35), latToY(-25))
      ctx.lineTo(lngToX(28), latToY(-33))
      ctx.lineTo(lngToX(18), latToY(-35))
      ctx.lineTo(lngToX(12), latToY(-25))
      ctx.lineTo(lngToX(10), latToY(-5))
      ctx.lineTo(lngToX(5), latToY(5))
      ctx.lineTo(lngToX(-5), latToY(8))
      ctx.lineTo(lngToX(-17), latToY(15))
      ctx.closePath()
      ctx.fill()
      ctx.stroke()

      // Asia / Russia
      ctx.beginPath()
      ctx.moveTo(lngToX(30), latToY(42))
      ctx.lineTo(lngToX(60), latToY(40))
      ctx.lineTo(lngToX(75), latToY(38))
      ctx.lineTo(lngToX(90), latToY(30))
      ctx.lineTo(lngToX(100), latToY(22))
      ctx.lineTo(lngToX(105), latToY(10))
      ctx.lineTo(lngToX(120), latToY(5))
      ctx.lineTo(lngToX(125), latToY(15))
      ctx.lineTo(lngToX(135), latToY(35))
      ctx.lineTo(lngToX(145), latToY(45))
      ctx.lineTo(lngToX(160), latToY(60))
      ctx.lineTo(lngToX(180), latToY(65))
      ctx.lineTo(lngToX(180), latToY(75))
      ctx.lineTo(lngToX(100), latToY(78))
      ctx.lineTo(lngToX(60), latToY(75))
      ctx.lineTo(lngToX(30), latToY(72))
      ctx.lineTo(lngToX(30), latToY(55))
      ctx.closePath()
      ctx.fill()
      ctx.stroke()

      // India
      ctx.beginPath()
      ctx.moveTo(lngToX(68), latToY(35))
      ctx.lineTo(lngToX(78), latToY(35))
      ctx.lineTo(lngToX(90), latToY(28))
      ctx.lineTo(lngToX(92), latToY(22))
      ctx.lineTo(lngToX(88), latToY(22))
      ctx.lineTo(lngToX(80), latToY(8))
      ctx.lineTo(lngToX(72), latToY(18))
      ctx.lineTo(lngToX(68), latToY(24))
      ctx.closePath()
      ctx.fill()
      ctx.stroke()

      // Australia
      ctx.beginPath()
      ctx.moveTo(lngToX(115), latToY(-14))
      ctx.lineTo(lngToX(130), latToY(-12))
      ctx.lineTo(lngToX(145), latToY(-15))
      ctx.lineTo(lngToX(153), latToY(-25))
      ctx.lineTo(lngToX(150), latToY(-38))
      ctx.lineTo(lngToX(140), latToY(-38))
      ctx.lineTo(lngToX(130), latToY(-34))
      ctx.lineTo(lngToX(115), latToY(-34))
      ctx.lineTo(lngToX(115), latToY(-22))
      ctx.closePath()
      ctx.fill()
      ctx.stroke()

      // Japan
      ctx.beginPath()
      ctx.moveTo(lngToX(130), latToY(45))
      ctx.lineTo(lngToX(145), latToY(45))
      ctx.lineTo(lngToX(140), latToY(35))
      ctx.lineTo(lngToX(132), latToY(33))
      ctx.closePath()
      ctx.fill()
      ctx.stroke()

      // Indonesia/Philippines
      ctx.beginPath()
      ctx.moveTo(lngToX(95), latToY(5))
      ctx.lineTo(lngToX(105), latToY(6))
      ctx.lineTo(lngToX(115), latToY(-2))
      ctx.lineTo(lngToX(125), latToY(-5))
      ctx.lineTo(lngToX(140), latToY(-5))
      ctx.lineTo(lngToX(135), latToY(-10))
      ctx.lineTo(lngToX(115), latToY(-8))
      ctx.lineTo(lngToX(100), latToY(-5))
      ctx.closePath()
      ctx.fill()
      ctx.stroke()

      // New Zealand
      ctx.beginPath()
      ctx.moveTo(lngToX(165), latToY(-35))
      ctx.lineTo(lngToX(178), latToY(-38))
      ctx.lineTo(lngToX(175), latToY(-45))
      ctx.lineTo(lngToX(168), latToY(-45))
      ctx.closePath()
      ctx.fill()
      ctx.stroke()
    }

    const drawClusterMarkers = (time: number) => {
      clusterMarkers.forEach((marker) => {
        const x = lngToX(marker.lng)
        const y = latToY(marker.lat)
        const colors = colorMap[marker.color]
        
        const baseRadius = Math.max(18, 14 + Math.log(marker.count + 1) * 6)
        const pulseScale = 1 + Math.sin(time * 0.002 + parseInt(marker.id) * 0.7) * 0.08

        // Outer glow ring
        ctx.strokeStyle = colors.glow
        ctx.lineWidth = 3
        ctx.beginPath()
        ctx.arc(x, y, (baseRadius + 8) * pulseScale, 0, Math.PI * 2)
        ctx.stroke()

        // Inner glow
        const gradient = ctx.createRadialGradient(x, y, 0, x, y, baseRadius * 1.5)
        gradient.addColorStop(0, colors.fill + "40")
        gradient.addColorStop(1, "transparent")
        ctx.fillStyle = gradient
        ctx.beginPath()
        ctx.arc(x, y, baseRadius * 1.5, 0, Math.PI * 2)
        ctx.fill()

        // Main circle
        ctx.fillStyle = colors.fill
        ctx.beginPath()
        ctx.arc(x, y, baseRadius, 0, Math.PI * 2)
        ctx.fill()

        // Number text
        ctx.fillStyle = marker.color === "yellow" || marker.color === "cyan" ? "#000" : "#fff"
        ctx.font = `bold ${Math.min(16, baseRadius * 0.7)}px system-ui`
        ctx.textAlign = "center"
        ctx.textBaseline = "middle"
        ctx.fillText(marker.count.toString(), x, y)
      })
    }

    const animate = (time: number) => {
      ctx.clearRect(0, 0, dimensions.width, dimensions.height)

      // Background
      ctx.fillStyle = "#0a1612"
      ctx.fillRect(0, 0, dimensions.width, dimensions.height)

      drawGrid()
      drawContinents()
      drawRegionLabels()
      drawClusterMarkers(time)

      animationRef.current = requestAnimationFrame(animate)
    }

    animate(0)

    return () => {
      cancelAnimationFrame(animationRef.current)
    }
  }, [dimensions, zoom, offset, latToY, lngToX])

  const handleMouseMove = (e: React.MouseEvent<HTMLCanvasElement>) => {
    if (!canvasRef.current || !onCoordinateChange) return

    const rect = canvasRef.current.getBoundingClientRect()
    const x = e.clientX - rect.left
    const y = e.clientY - rect.top

    const lng = ((x - offset.x) / (dimensions.width * zoom)) * 360 - 180
    const lat = 90 - ((y - offset.y) / (dimensions.height * zoom)) * 180

    onCoordinateChange({ lat, lng })
  }

  const handleMouseLeave = () => {
    onCoordinateChange?.(null)
  }

  return (
    <div className="relative h-full w-full overflow-hidden">
      <canvas
        ref={canvasRef}
        className="absolute inset-0 h-full w-full cursor-crosshair"
        style={{ width: dimensions.width, height: dimensions.height }}
        onMouseMove={handleMouseMove}
        onMouseLeave={handleMouseLeave}
      />
    </div>
  )
}

export { type ClusterMarker }
