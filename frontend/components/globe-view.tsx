"use client"

import { useEffect, useRef, useState } from "react"

export function GlobeView() {
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const [dimensions, setDimensions] = useState({ width: 0, height: 0 })
  const animationRef = useRef<number>(0)
  const rotationRef = useRef<number>(0)

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

  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas || dimensions.width === 0) return

    const ctx = canvas.getContext("2d")
    if (!ctx) return

    const dpr = window.devicePixelRatio || 1
    canvas.width = dimensions.width * dpr
    canvas.height = dimensions.height * dpr
    ctx.scale(dpr, dpr)

    const centerX = dimensions.width / 2
    const centerY = dimensions.height / 2
    const radius = Math.min(dimensions.width, dimensions.height) * 0.35

    const points: { lat: number; lng: number; color: string }[] = []

    // Generate landmass approximation points
    for (let i = 0; i < 2000; i++) {
      const lat = (Math.random() - 0.5) * 160
      const lng = (Math.random() - 0.5) * 360

      // Simple landmass detection (very approximate)
      const isLand =
        // North America
        (lat > 25 && lat < 70 && lng > -130 && lng < -60) ||
        // South America
        (lat > -55 && lat < 10 && lng > -80 && lng < -35) ||
        // Europe
        (lat > 35 && lat < 70 && lng > -10 && lng < 60) ||
        // Africa
        (lat > -35 && lat < 35 && lng > -20 && lng < 50) ||
        // Asia
        (lat > 10 && lat < 70 && lng > 60 && lng < 150) ||
        // Australia
        (lat > -40 && lat < -10 && lng > 115 && lng < 155)

      if (isLand && Math.random() > 0.3) {
        points.push({
          lat,
          lng,
          color: `rgba(34, 197, 94, ${0.3 + Math.random() * 0.4})`,
        })
      }
    }

    // Event locations with World Monitor colors
    const events = [
      { lat: 48.8566, lng: 2.3522, color: "#eab308" },
      { lat: 35.6762, lng: 139.6503, color: "#22c55e" },
      { lat: 51.5074, lng: -0.1278, color: "#06b6d4" },
      { lat: 40.7128, lng: -74.006, color: "#eab308" },
      { lat: 55.7558, lng: 37.6173, color: "#f97316" },
      { lat: 39.9042, lng: 116.4074, color: "#22c55e" },
    ]

    const projectPoint = (lat: number, lng: number, rotation: number) => {
      const phi = (90 - lat) * (Math.PI / 180)
      const theta = (lng + rotation) * (Math.PI / 180)

      const x = radius * Math.sin(phi) * Math.cos(theta)
      const y = radius * Math.cos(phi)
      const z = radius * Math.sin(phi) * Math.sin(theta)

      // Only show front-facing points
      if (z < 0) return null

      return {
        x: centerX + x,
        y: centerY - y,
        z,
        scale: (z + radius) / (2 * radius),
      }
    }

    const animate = () => {
      rotationRef.current += 0.15
      ctx.clearRect(0, 0, dimensions.width, dimensions.height)

      // Background
      ctx.fillStyle = "#0a1612"
      ctx.fillRect(0, 0, dimensions.width, dimensions.height)

      // Globe outline
      ctx.strokeStyle = "rgba(34, 197, 94, 0.2)"
      ctx.lineWidth = 2
      ctx.beginPath()
      ctx.arc(centerX, centerY, radius, 0, Math.PI * 2)
      ctx.stroke()

      // Globe glow
      const glowGradient = ctx.createRadialGradient(
        centerX,
        centerY,
        radius * 0.8,
        centerX,
        centerY,
        radius * 1.2
      )
      glowGradient.addColorStop(0, "rgba(34, 197, 94, 0.05)")
      glowGradient.addColorStop(1, "transparent")
      ctx.fillStyle = glowGradient
      ctx.fillRect(0, 0, dimensions.width, dimensions.height)

      // Latitude lines
      ctx.strokeStyle = "rgba(34, 197, 94, 0.08)"
      ctx.lineWidth = 0.5
      for (let lat = -60; lat <= 60; lat += 30) {
        ctx.beginPath()
        let started = false
        for (let lng = -180; lng <= 180; lng += 5) {
          const point = projectPoint(lat, lng, rotationRef.current)
          if (point) {
            if (!started) {
              ctx.moveTo(point.x, point.y)
              started = true
            } else {
              ctx.lineTo(point.x, point.y)
            }
          }
        }
        ctx.stroke()
      }

      // Longitude lines
      for (let lng = -180; lng < 180; lng += 30) {
        ctx.beginPath()
        let started = false
        for (let lat = -80; lat <= 80; lat += 5) {
          const point = projectPoint(lat, lng, rotationRef.current)
          if (point) {
            if (!started) {
              ctx.moveTo(point.x, point.y)
              started = true
            } else {
              ctx.lineTo(point.x, point.y)
            }
          }
        }
        ctx.stroke()
      }

      // Land points
      points.forEach((p) => {
        const point = projectPoint(p.lat, p.lng, rotationRef.current)
        if (point) {
          ctx.fillStyle = p.color
          ctx.beginPath()
          ctx.arc(point.x, point.y, 1.5 * point.scale, 0, Math.PI * 2)
          ctx.fill()
        }
      })

      // Event markers
      events.forEach((event) => {
        const point = projectPoint(event.lat, event.lng, rotationRef.current)
        if (point) {
          // Glow
          const gradient = ctx.createRadialGradient(
            point.x,
            point.y,
            0,
            point.x,
            point.y,
            12 * point.scale
          )
          gradient.addColorStop(0, event.color + "80")
          gradient.addColorStop(0.5, event.color + "30")
          gradient.addColorStop(1, "transparent")
          ctx.fillStyle = gradient
          ctx.beginPath()
          ctx.arc(point.x, point.y, 12 * point.scale, 0, Math.PI * 2)
          ctx.fill()

          // Center dot
          ctx.fillStyle = event.color
          ctx.beginPath()
          ctx.arc(point.x, point.y, 3 * point.scale, 0, Math.PI * 2)
          ctx.fill()
        }
      })

      animationRef.current = requestAnimationFrame(animate)
    }

    animate()

    return () => {
      cancelAnimationFrame(animationRef.current)
    }
  }, [dimensions])

  return (
    <div className="relative h-full w-full overflow-hidden bg-[#0a1612]">
      <canvas
        ref={canvasRef}
        className="absolute inset-0 h-full w-full"
        style={{ width: dimensions.width, height: dimensions.height }}
      />
      
      {/* Info overlay */}
      <div className="absolute bottom-4 left-4 right-4 flex items-center justify-between">
        <div className="rounded border border-[#1a3a2a] bg-[#0c1a14]/80 px-3 py-2 font-mono text-xs backdrop-blur-sm">
          <span className="text-[#22c55e]/60">Rotation: </span>
          <span className="text-[#22c55e]">AUTO</span>
        </div>
        <div className="rounded border border-[#1a3a2a] bg-[#0c1a14]/80 px-3 py-2 font-mono text-xs backdrop-blur-sm">
          <span className="text-[#22c55e]/60">Events: </span>
          <span className="text-[#22c55e]">6 ACTIVE</span>
        </div>
      </div>
    </div>
  )
}
