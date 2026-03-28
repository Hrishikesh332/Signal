"use client"

import type L from "leaflet"
import { Loader2, Search } from "lucide-react"
import { useEffect, useRef, useState } from "react"

import { fetchAppApi } from "@/lib/fetch-app"

export type GeocodeHit = {
  lat: string
  lon: string
  display_name: string
  class?: string
  type?: string
  boundingbox?: [string, string, string, string]
}

interface SearchBarProps {
  mapRef: React.RefObject<L.Map | null>
}

function zoomFromPlaceType(type: string | undefined, cls: string | undefined): number {
  const t = (type ?? "").toLowerCase()
  if (t === "country") return 5
  if (t === "state" || t === "region" || t === "province") return 6
  if (t === "county") return 8
  if (t === "city" || t === "administrative") return 9
  if (t === "town" || t === "municipality") return 11
  if (t === "village" || t === "hamlet" || t === "suburb" || t === "neighbourhood") return 13
  if (cls === "boundary" && t === "administrative") return 6
  return 10
}

function goToPlace(map: L.Map, hit: GeocodeHit) {
  const bbox = hit.boundingbox
  if (bbox?.length === 4) {
    const [south, north, west, east] = bbox.map(Number)
    map.fitBounds(
      [
        [south, west],
        [north, east],
      ],
      { padding: [40, 40], maxZoom: 14, animate: true }
    )
    return
  }
  const lat = parseFloat(hit.lat)
  const lon = parseFloat(hit.lon)
  if (Number.isNaN(lat) || Number.isNaN(lon)) return
  const z = zoomFromPlaceType(hit.type, hit.class)
  map.flyTo([lat, lon], z, { duration: 1.2 })
}

async function fetchPlaces(q: string): Promise<GeocodeHit[]> {
  const res = await fetchAppApi(`/api/geocode?q=${encodeURIComponent(q)}`)
  if (!res.ok) return []
  const data = (await res.json()) as { results?: GeocodeHit[] }
  return data.results ?? []
}

export function SearchBar({ mapRef }: SearchBarProps) {
  const [query, setQuery] = useState("")
  const [suggestions, setSuggestions] = useState<GeocodeHit[]>([])
  const [loading, setLoading] = useState(false)
  const [open, setOpen] = useState(false)
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const rootRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const onDoc = (e: MouseEvent) => {
      if (!rootRef.current?.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener("mousedown", onDoc)
    return () => document.removeEventListener("mousedown", onDoc)
  }, [])

  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current)
    const t = query.trim()
    if (t.length < 2) {
      setSuggestions([])
      setOpen(false)
      return
    }
    debounceRef.current = setTimeout(async () => {
      setLoading(true)
      try {
        const hits = await fetchPlaces(t)
        setSuggestions(hits)
        setOpen(hits.length > 0)
      } finally {
        setLoading(false)
      }
    }, 400)
    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current)
    }
  }, [query])

  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    const t = query.trim()
    if (t.length < 2) return
    setLoading(true)
    try {
      const hits = await fetchPlaces(t)
      setSuggestions(hits)
      setOpen(hits.length > 0)
      const map = mapRef.current
      if (map && hits[0]) goToPlace(map, hits[0])
    } finally {
      setLoading(false)
    }
  }

  const onPick = (hit: GeocodeHit) => {
    const map = mapRef.current
    if (map) goToPlace(map, hit)
    setQuery(hit.display_name.split(",").slice(0, 2).join(",").trim())
    setOpen(false)
  }

  return (
    <div ref={rootRef} className="absolute top-20 left-1/2 -translate-x-1/2 z-40 w-[min(100vw-2rem,22rem)]">
      <form onSubmit={onSubmit}>
        <div className="relative flex items-center gap-2 bg-[#09090b]/95 backdrop-blur-sm border border-[#27272a] rounded-lg px-3 py-2">
          {loading ? (
            <Loader2 className="w-4 h-4 text-[#52525b] shrink-0 animate-spin" />
          ) : (
            <Search className="w-4 h-4 text-[#52525b] shrink-0" />
          )}
          <input
            type="search"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onFocus={() => suggestions.length > 0 && setOpen(true)}
            placeholder="Search country or city…"
            autoComplete="off"
            className="flex-1 min-w-0 bg-transparent text-[11px] text-white outline-none placeholder:text-[#52525b]"
          />
        </div>
      </form>
      {open && suggestions.length > 0 && (
        <ul className="absolute left-0 right-0 top-full mt-1 max-h-56 overflow-y-auto rounded-lg border border-[#27272a] bg-[#0c0c0e] py-1 shadow-xl">
          {suggestions.map((hit, i) => (
            <li key={`${hit.lat}-${hit.lon}-${i}`}>
              <button
                type="button"
                onClick={() => onPick(hit)}
                className="w-full px-3 py-2 text-left text-[11px] text-[#a1a1aa] hover:bg-[#18181b] hover:text-white transition-colors"
              >
                {hit.display_name}
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}
