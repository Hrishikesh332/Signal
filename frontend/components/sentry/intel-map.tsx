"use client"

import {
  startTransition,
  useDeferredValue,
  useEffect,
  useRef,
  useState,
} from "react"
import { geoGraticule10, geoNaturalEarth1, geoPath } from "d3-geo"
import { Filter, LocateFixed, Minus, Plus, Radar } from "lucide-react"
import atlas from "world-atlas/countries-110m.json"
import { feature, mesh } from "topojson-client"
import type { MapPoint } from "@/lib/dashboard-types"

interface IntelMapProps {
  points: MapPoint[] | undefined
}

const SEVERITY_COLORS = {
  critical: "#ef4444",
  high: "#f59e0b",
  medium: "#84cc16",
  low: "#06b6d4",
} satisfies Record<MapPoint["severity"], string>

const FILTER_SEVERITIES = [
  "all",
  "critical",
  "high",
  "medium",
  "low",
] as const

const REGION_LABELS = [
  { label: "NORTH AMERICA", coordinates: [-104, 40] as [number, number] },
  { label: "SOUTH AMERICA", coordinates: [-60, -18] as [number, number] },
  { label: "EUROPE", coordinates: [15, 52] as [number, number] },
  { label: "AFRICA", coordinates: [21, 9] as [number, number] },
  { label: "ASIA PACIFIC", coordinates: [118, 32] as [number, number] },
  { label: "AUSTRALIA", coordinates: [137, -26] as [number, number] },
] as const

type SeverityFilter = (typeof FILTER_SEVERITIES)[number]

interface MapView {
  scale: number
  x: number
  y: number
}

interface CursorState {
  x: number
  y: number
  latitude: number
  longitude: number
}

const atlasTopology = atlas as {
  objects: {
    countries: object
  }
}

const COUNTRY_FEATURES = (
  feature(atlasTopology as never, atlasTopology.objects.countries as never) as unknown as {
    features: unknown[]
  }
).features

const COUNTRY_BORDERS = mesh(
  atlasTopology as never,
  atlasTopology.objects.countries as never,
  (a: unknown, b: unknown) => a !== b
)

const GRATICULE = geoGraticule10()

function clamp(value: number, min: number, max: number) {
  return Math.min(max, Math.max(min, value))
}

function getPointId(point: MapPoint, index: number): string {
  return (
    point.id ??
    `${point.entity_name}-${point.category ?? "signal"}-${point.timestamp ?? "na"}-${point.longitude}-${point.latitude}-${index}`
  )
}

function buildHomeView(
  points: MapPoint[],
  projection: ReturnType<typeof geoNaturalEarth1>,
  width: number,
  height: number
): MapView {
  if (!width || !height || points.length === 0) {
    return { scale: 1, x: 0, y: 0 }
  }

  const projected = points
    .map((point) => projection([point.longitude, point.latitude]))
    .filter((value): value is [number, number] => Array.isArray(value))

  if (projected.length === 0) {
    return { scale: 1, x: 0, y: 0 }
  }

  if (projected.length === 1) {
    const [x, y] = projected[0]
    const scale = 1.9
    return {
      scale,
      x: width / 2 - x * scale,
      y: height / 2 - y * scale,
    }
  }

  const xs = projected.map(([x]) => x)
  const ys = projected.map(([, y]) => y)
  const minX = Math.min(...xs)
  const maxX = Math.max(...xs)
  const minY = Math.min(...ys)
  const maxY = Math.max(...ys)
  const boundsWidth = Math.max(maxX - minX, 1)
  const boundsHeight = Math.max(maxY - minY, 1)
  const padding = 84
  const scale = clamp(
    Math.min((width - padding * 2) / boundsWidth, (height - padding * 2) / boundsHeight),
    1,
    4
  )

  return {
    scale,
    x: width / 2 - ((minX + maxX) / 2) * scale,
    y: height / 2 - ((minY + maxY) / 2) * scale,
  }
}

function getHomeCoordinate(points: MapPoint[]): [number, number] {
  if (points.length === 0) return [15, 20]

  const totals = points.reduce(
    (accumulator, point) => ({
      latitude: accumulator.latitude + point.latitude,
      longitude: accumulator.longitude + point.longitude,
    }),
    { latitude: 0, longitude: 0 }
  )

  return [totals.longitude / points.length, totals.latitude / points.length]
}

function formatCoordinate(value: number, positive: string, negative: string) {
  const suffix = value >= 0 ? positive : negative
  return `${Math.abs(value).toFixed(2)} ${suffix}`
}

export function IntelMap({ points }: IntelMapProps) {
  const deferredPoints = useDeferredValue(points ?? [])
  const containerRef = useRef<HTMLDivElement>(null)
  const dragRef = useRef<{
    pointerId: number
    startX: number
    startY: number
    originX: number
    originY: number
  } | null>(null)
  const userHasMovedRef = useRef(false)
  const homeViewRef = useRef<MapView>({ scale: 1, x: 0, y: 0 })
  const criticalFingerprintRef = useRef("")

  const [dimensions, setDimensions] = useState({ width: 0, height: 0 })
  const [hoveredPoint, setHoveredPoint] = useState<MapPoint | null>(null)
  const [mousePos, setMousePos] = useState({ x: 0, y: 0 })
  const [cursorState, setCursorState] = useState<CursorState | null>(null)
  const [selectedSeverity, setSelectedSeverity] = useState<SeverityFilter>("all")
  const [selectedCategory, setSelectedCategory] = useState("all")
  const [view, setView] = useState<MapView>({ scale: 1, x: 0, y: 0 })

  useEffect(() => {
    if (!containerRef.current) return

    const observer = new ResizeObserver(([entry]) => {
      const { width, height } = entry.contentRect
      setDimensions({ width, height })
    })

    observer.observe(containerRef.current)
    return () => observer.disconnect()
  }, [])

  const projection = geoNaturalEarth1().fitExtent(
    [
      [18, 18],
      [Math.max(dimensions.width - 18, 18), Math.max(dimensions.height - 18, 18)],
    ],
    { type: "Sphere" }
  )

  const path = geoPath(projection)

  const categories = Array.from(
    new Set(
      deferredPoints
        .map((point) => point.category?.trim())
        .filter((value): value is string => Boolean(value))
    )
  ).sort((left, right) => left.localeCompare(right))

  const effectiveCategory =
    selectedCategory !== "all" && !categories.includes(selectedCategory)
      ? "all"
      : selectedCategory

  const filteredPoints = deferredPoints.filter((point) => {
    const severityMatches =
      selectedSeverity === "all" || point.severity === selectedSeverity
    const categoryMatches =
      effectiveCategory === "all" || (point.category ?? "") === effectiveCategory

    return severityMatches && categoryMatches
  })

  const criticalPoints = filteredPoints.filter((point) => point.severity === "critical")
  const criticalSignature = criticalPoints
    .map((point, index) => getPointId(point, index))
    .sort()
    .join("|")
  const activeCoordinate = cursorState
    ? [cursorState.longitude, cursorState.latitude]
    : getHomeCoordinate(filteredPoints)
  const hasPoints = filteredPoints.length > 0

  useEffect(() => {
    const homeView = buildHomeView(
      filteredPoints,
      projection,
      dimensions.width,
      dimensions.height
    )
    homeViewRef.current = homeView

    if (!dimensions.width || !dimensions.height) {
      return
    }

    const focusPoints =
      !userHasMovedRef.current &&
      criticalPoints.length > 0 &&
      criticalSignature !== criticalFingerprintRef.current
        ? criticalPoints
        : filteredPoints

    criticalFingerprintRef.current = criticalSignature

    if (!userHasMovedRef.current) {
      // The view should track backend-driven signal changes until the user takes control.
      // eslint-disable-next-line react-hooks/set-state-in-effect
      setView(buildHomeView(focusPoints, projection, dimensions.width, dimensions.height))
    }
  }, [
    criticalPoints,
    criticalSignature,
    dimensions.height,
    dimensions.width,
    filteredPoints,
    projection,
  ])

  const setCursorFromEvent = (clientX: number, clientY: number) => {
    if (!containerRef.current) return

    const rect = containerRef.current.getBoundingClientRect()
    const x = clientX - rect.left
    const y = clientY - rect.top
    const mapX = (x - view.x) / view.scale
    const mapY = (y - view.y) / view.scale
    const inverted = projection.invert?.([mapX, mapY])

    if (!inverted) {
      setCursorState(null)
      return
    }

    setCursorState({
      x,
      y,
      longitude: inverted[0],
      latitude: inverted[1],
    })
  }

  const applyZoom = (factor: number, anchorX: number, anchorY: number) => {
    userHasMovedRef.current = true
    setView((current) => {
      const nextScale = clamp(current.scale * factor, 1, 6)
      const ratio = nextScale / current.scale

      return {
        scale: nextScale,
        x: anchorX - (anchorX - current.x) * ratio,
        y: anchorY - (anchorY - current.y) * ratio,
      }
    })
  }

  const handleWheel = (event: React.WheelEvent<SVGSVGElement>) => {
    event.preventDefault()
    const rect = event.currentTarget.getBoundingClientRect()
    const anchorX = event.clientX - rect.left
    const anchorY = event.clientY - rect.top
    applyZoom(event.deltaY < 0 ? 1.16 : 1 / 1.16, anchorX, anchorY)
    setCursorFromEvent(event.clientX, event.clientY)
  }

  const handlePointerDown = (event: React.PointerEvent<SVGSVGElement>) => {
    dragRef.current = {
      pointerId: event.pointerId,
      startX: event.clientX,
      startY: event.clientY,
      originX: view.x,
      originY: view.y,
    }
    event.currentTarget.setPointerCapture(event.pointerId)
  }

  const handlePointerMove = (event: React.PointerEvent<SVGSVGElement>) => {
    setCursorFromEvent(event.clientX, event.clientY)

    if (!dragRef.current || dragRef.current.pointerId !== event.pointerId) {
      return
    }

    userHasMovedRef.current = true
    setView((current) => ({
      scale: current.scale,
      x: dragRef.current?.originX ?? current.x + (event.clientX - event.clientX),
      y: dragRef.current?.originY ?? current.y + (event.clientY - event.clientY),
    }))

    setView((current) => ({
      scale: current.scale,
      x: (dragRef.current?.originX ?? current.x) + (event.clientX - (dragRef.current?.startX ?? event.clientX)),
      y: (dragRef.current?.originY ?? current.y) + (event.clientY - (dragRef.current?.startY ?? event.clientY)),
    }))
  }

  const handlePointerUp = (event: React.PointerEvent<SVGSVGElement>) => {
    if (dragRef.current?.pointerId === event.pointerId) {
      dragRef.current = null
      event.currentTarget.releasePointerCapture(event.pointerId)
    }
  }

  const handlePointerLeave = () => {
    dragRef.current = null
    setCursorState(null)
    setHoveredPoint(null)
  }

  const handleZoomIn = () => {
    applyZoom(1.18, dimensions.width / 2, dimensions.height / 2)
  }

  const handleZoomOut = () => {
    applyZoom(1 / 1.18, dimensions.width / 2, dimensions.height / 2)
  }

  const handleReset = () => {
    userHasMovedRef.current = false
    setView(homeViewRef.current)
  }

  return (
    <div
      ref={containerRef}
      className="relative h-full min-h-[300px] w-full overflow-hidden bg-[#030712]"
    >
      <svg
        className="absolute inset-0 z-0 h-full w-full"
        viewBox={`0 0 ${Math.max(dimensions.width, 1)} ${Math.max(dimensions.height, 1)}`}
        preserveAspectRatio="xMidYMid meet"
        onWheel={handleWheel}
        onPointerDown={handlePointerDown}
        onPointerMove={handlePointerMove}
        onPointerUp={handlePointerUp}
        onPointerCancel={handlePointerUp}
        onPointerLeave={handlePointerLeave}
      >
        <g transform={`translate(${view.x} ${view.y}) scale(${view.scale})`}>
          <path
            d={path({ type: "Sphere" }) ?? ""}
            fill="rgba(3, 7, 18, 0.98)"
            stroke="rgba(16, 185, 129, 0.08)"
            strokeWidth={1}
            vectorEffect="non-scaling-stroke"
          />
          <path
            d={path(GRATICULE as never) ?? ""}
            fill="none"
            stroke="rgba(16, 185, 129, 0.08)"
            strokeWidth={0.85}
            vectorEffect="non-scaling-stroke"
          />
          {COUNTRY_FEATURES.map((country, index) => (
            <path
              key={index}
              d={path(country as never) ?? ""}
              fill="rgba(203, 213, 225, 0.10)"
              stroke="rgba(203, 213, 225, 0.12)"
              strokeWidth={0.45}
              vectorEffect="non-scaling-stroke"
            />
          ))}
          <path
            d={path(COUNTRY_BORDERS as never) ?? ""}
            fill="none"
            stroke="rgba(148, 163, 156, 0.16)"
            strokeWidth={0.55}
            vectorEffect="non-scaling-stroke"
          />

          {REGION_LABELS.map((region) => {
            const projected = projection(region.coordinates)
            if (!projected) return null

            return (
              <text
                key={region.label}
                x={projected[0]}
                y={projected[1]}
                fill="rgba(148, 163, 156, 0.18)"
                fontSize="11"
                fontFamily="Cascadia Mono, SFMono-Regular, Consolas, monospace"
                textAnchor="middle"
                letterSpacing="0.22em"
              >
                {region.label}
              </text>
            )
          })}

          {filteredPoints.map((point, index) => {
            const projected = projection([point.longitude, point.latitude])
            if (!projected) return null

            return (
              <g
                key={getPointId(point, index)}
                transform={`translate(${projected[0]} ${projected[1]})`}
                onMouseEnter={(event) => {
                  const rect = containerRef.current?.getBoundingClientRect()
                  if (!rect) return
                  setMousePos({ x: event.clientX - rect.left, y: event.clientY - rect.top })
                  setHoveredPoint(point)
                }}
                onMouseMove={(event) => {
                  const rect = containerRef.current?.getBoundingClientRect()
                  if (!rect) return
                  setMousePos({ x: event.clientX - rect.left, y: event.clientY - rect.top })
                }}
                onMouseLeave={() => setHoveredPoint(null)}
              >
                <circle r="18" fill={SEVERITY_COLORS[point.severity]} opacity="0.16" />
                <circle
                  r="8"
                  fill={SEVERITY_COLORS[point.severity]}
                  opacity="0.92"
                  stroke="rgba(248,250,252,0.92)"
                  strokeWidth="1.1"
                  vectorEffect="non-scaling-stroke"
                />
                <circle r="2" fill="#f8fafc" opacity="0.95" />
              </g>
            )
          })}
        </g>
      </svg>

      <div className="pointer-events-none absolute inset-0 z-20 bg-[radial-gradient(circle_at_52%_42%,rgba(16,185,129,0.08),transparent_42%)]" />
      <div className="pointer-events-none absolute inset-0 z-20 bg-[linear-gradient(180deg,rgba(148,163,184,0.05),transparent_16%,transparent_84%,rgba(15,23,42,0.34))]" />
      <div className="pointer-events-none absolute inset-0 z-20 shadow-[inset_0_0_160px_rgba(2,6,23,0.94)]" />

      {cursorState && (
        <>
          <div
            className="pointer-events-none absolute bottom-0 top-0 z-20 w-px bg-emerald-500/8"
            style={{ left: cursorState.x }}
          />
          <div
            className="pointer-events-none absolute left-0 right-0 z-20 h-px bg-emerald-500/8"
            style={{ top: cursorState.y }}
          />
        </>
      )}

      <div className="absolute left-3 top-3 z-30 flex max-w-[480px] flex-col gap-2">
        <div className="flex items-center gap-2 border border-emerald-900/50 bg-black/72 px-3 py-2">
          <Radar className="h-3.5 w-3.5 text-emerald-400" />
          <span className="text-[10px] font-mono uppercase tracking-[0.26em] text-emerald-500">
            Active Signals
          </span>
          <span className="ml-auto text-sm font-mono text-emerald-300">
            {filteredPoints.length}
          </span>
        </div>

        {deferredPoints.length > 0 && (
          <div className="border border-emerald-900/50 bg-black/72 px-3 py-2">
            <div className="flex items-center gap-2 text-[10px] font-mono uppercase tracking-[0.22em] text-emerald-600">
              <Filter className="h-3.5 w-3.5" />
              Filter View
            </div>

            <div className="mt-2 flex flex-wrap gap-1.5">
              {FILTER_SEVERITIES.map((severity) => {
                const isActive = selectedSeverity === severity
                const color =
                  severity === "all"
                    ? "#10b981"
                    : SEVERITY_COLORS[severity as MapPoint["severity"]]

                return (
                  <button
                    key={severity}
                    type="button"
                    onClick={() => {
                      userHasMovedRef.current = false
                      startTransition(() => setSelectedSeverity(severity))
                    }}
                    className="rounded-none border px-2 py-1 text-[10px] font-mono uppercase tracking-[0.18em] transition-colors"
                    style={{
                      borderColor: isActive ? `${color}99` : "rgba(6, 78, 59, 0.7)",
                      backgroundColor: isActive ? `${color}18` : "rgba(0, 0, 0, 0.24)",
                      color: isActive ? color : "#4ade80",
                    }}
                  >
                    {severity}
                  </button>
                )
              })}
            </div>

            {categories.length > 0 && (
              <div className="mt-2 flex flex-wrap gap-1.5">
                {["all", ...categories].map((category) => {
                  const isActive = effectiveCategory === category

                  return (
                    <button
                      key={category}
                      type="button"
                      onClick={() => {
                        userHasMovedRef.current = false
                        startTransition(() => setSelectedCategory(category))
                      }}
                      className="rounded-none border px-2 py-1 text-[10px] font-mono uppercase tracking-[0.15em] transition-colors"
                      style={{
                        borderColor: isActive
                          ? "rgba(16, 185, 129, 0.62)"
                          : "rgba(6, 78, 59, 0.7)",
                        backgroundColor: isActive
                          ? "rgba(16, 185, 129, 0.14)"
                          : "rgba(0, 0, 0, 0.24)",
                        color: isActive ? "#6ee7b7" : "#0f766e",
                      }}
                    >
                      {category}
                    </button>
                  )
                })}
              </div>
            )}
          </div>
        )}
      </div>

      <div className="absolute right-3 top-3 z-30 flex flex-col gap-1.5">
        <button
          type="button"
          title="Zoom in"
          onClick={handleZoomIn}
          className="flex h-9 w-9 items-center justify-center border border-emerald-800 bg-black/72 text-emerald-400 transition-colors hover:bg-emerald-950/40"
        >
          <Plus className="h-4 w-4" />
        </button>
        <button
          type="button"
          title="Zoom out"
          onClick={handleZoomOut}
          className="flex h-9 w-9 items-center justify-center border border-emerald-800 bg-black/72 text-emerald-400 transition-colors hover:bg-emerald-950/40"
        >
          <Minus className="h-4 w-4" />
        </button>
        <button
          type="button"
          title="Reset view"
          onClick={handleReset}
          className="flex h-9 w-9 items-center justify-center border border-emerald-800 bg-black/72 text-emerald-400 transition-colors hover:bg-emerald-950/40"
        >
          <LocateFixed className="h-4 w-4" />
        </button>
      </div>

      {!hasPoints && (
        <div className="absolute inset-0 z-30 flex items-center justify-center">
          <div className="border border-emerald-900/50 bg-black/60 px-4 py-6 text-center">
            <p className="text-sm font-mono text-emerald-600">
              No geolocated signals available yet.
            </p>
          </div>
        </div>
      )}

      {hoveredPoint && (
        <div
          className="pointer-events-none absolute z-40 max-w-[280px] border border-emerald-700 bg-black/95 px-3 py-2 text-xs font-mono"
          style={{ left: mousePos.x + 12, top: mousePos.y + 12 }}
        >
          <div className="mb-1 flex items-center gap-2">
            <span
              className="h-2 w-2 rounded-full"
              style={{ backgroundColor: SEVERITY_COLORS[hoveredPoint.severity] }}
            />
            <span className="font-bold uppercase text-emerald-400">
              {hoveredPoint.entity_name}
            </span>
          </div>
          <p className="text-emerald-600">{hoveredPoint.explanation}</p>
          <p className="mt-1 text-emerald-800">
            {hoveredPoint.latitude.toFixed(2)}, {hoveredPoint.longitude.toFixed(2)}
          </p>
        </div>
      )}

      <div className="absolute bottom-2 left-2 z-30 flex items-center gap-3 border border-emerald-900/50 bg-black/60 px-2 py-1 text-[10px] font-mono">
        {Object.entries(SEVERITY_COLORS).map(([severity, color]) => (
          <div key={severity} className="flex items-center gap-1">
            <span className="h-2 w-2 rounded-full" style={{ backgroundColor: color }} />
            <span className="text-emerald-600 uppercase">{severity}</span>
          </div>
        ))}
      </div>

      <div className="absolute bottom-2 right-2 z-30 min-w-[180px] border border-emerald-900/50 bg-black/68 px-3 py-2 text-[10px] font-mono">
        <div className="uppercase tracking-[0.22em] text-emerald-700">
          {cursorState ? "Tracking" : "Home View"}
        </div>
        <div className="mt-1 text-sm text-emerald-400">
          {formatCoordinate(activeCoordinate[1], "N", "S")}{" "}
          {formatCoordinate(activeCoordinate[0], "E", "W")}
        </div>
        <div className="mt-1 text-emerald-800">
          ({activeCoordinate[1].toFixed(4)}, {activeCoordinate[0].toFixed(4)})
        </div>
      </div>
    </div>
  )
}
