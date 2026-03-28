import type { DashboardEvent, MapPoint } from "@/lib/dashboard-types"

interface LatLng {
  latitude: number
  longitude: number
}

const LOCATION_CENTROIDS: Record<string, LatLng> = {
  argentina: { latitude: -38.4161, longitude: -63.6167 },
  australia: { latitude: -25.2744, longitude: 133.7751 },
  belgium: { latitude: 50.5039, longitude: 4.4699 },
  brazil: { latitude: -14.235, longitude: -51.9253 },
  brussels: { latitude: 50.8503, longitude: 4.3517 },
  canada: { latitude: 56.1304, longitude: -106.3468 },
  china: { latitude: 35.8617, longitude: 104.1954 },
  egypt: { latitude: 26.8206, longitude: 30.8025 },
  "eastern europe": { latitude: 50.4501, longitude: 30.5234 },
  europe: { latitude: 54.526, longitude: 15.2551 },
  france: { latitude: 46.2276, longitude: 2.2137 },
  germany: { latitude: 51.1657, longitude: 10.4515 },
  global: { latitude: 20, longitude: 0 },
  india: { latitude: 20.5937, longitude: 78.9629 },
  indonesia: { latitude: -0.7893, longitude: 113.9213 },
  italy: { latitude: 41.8719, longitude: 12.5674 },
  japan: { latitude: 36.2048, longitude: 138.2529 },
  london: { latitude: 51.5072, longitude: -0.1276 },
  mexico: { latitude: 23.6345, longitude: -102.5528 },
  philippines: { latitude: 12.8797, longitude: 121.774 },
  rome: { latitude: 41.9028, longitude: 12.4964 },
  singapore: { latitude: 1.3521, longitude: 103.8198 },
  "south africa": { latitude: -30.5595, longitude: 22.9375 },
  "southeast asia": { latitude: 10.5, longitude: 106 },
  spain: { latitude: 40.4637, longitude: -3.7492 },
  tokyo: { latitude: 35.6762, longitude: 139.6503 },
  "united kingdom": { latitude: 55.3781, longitude: -3.436 },
  uk: { latitude: 55.3781, longitude: -3.436 },
  ukraine: { latitude: 48.3794, longitude: 31.1656 },
  "united states": { latitude: 39.8283, longitude: -98.5795 },
  usa: { latitude: 39.8283, longitude: -98.5795 },
  "washington dc": { latitude: 38.9072, longitude: -77.0369 },
  "washington d.c.": { latitude: 38.9072, longitude: -77.0369 },
}

function normalizeLocationPart(value: string): string {
  return value
    .toLowerCase()
    .replace(/[().]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
}

function candidateKeys(value: string): string[] {
  const normalized = normalizeLocationPart(value)
  const candidates = new Set<string>()
  candidates.add(normalized)

  normalized
    .split(/[|/]/)
    .map((part) => normalizeLocationPart(part))
    .filter(Boolean)
    .forEach((part) => candidates.add(part))

  normalized
    .split(",")
    .map((part) => normalizeLocationPart(part))
    .filter(Boolean)
    .forEach((part) => candidates.add(part))

  return Array.from(candidates)
}

export function resolveLocationLabel(value: string | null | undefined): LatLng | null {
  if (!value) return null

  for (const key of candidateKeys(value)) {
    const direct = LOCATION_CENTROIDS[key]
    if (direct) return direct
  }

  return null
}

export function deriveEventCoordinates(
  event: Pick<
    DashboardEvent,
    "latitude" | "longitude" | "location_label" | "locations"
  >
): LatLng | null {
  if (typeof event.latitude === "number" && typeof event.longitude === "number") {
    return {
      latitude: event.latitude,
      longitude: event.longitude,
    }
  }

  const primary = resolveLocationLabel(event.location_label)
  if (primary) return primary

  for (const location of event.locations ?? []) {
    const resolved = resolveLocationLabel(location)
    if (resolved) return resolved
  }

  return null
}

export function eventToMapPoint(event: DashboardEvent): MapPoint | null {
  const coords = deriveEventCoordinates(event)
  if (!coords) return null

  return {
    id: event.id,
    category: event.category,
    company: event.company,
    timestamp: event.timestamp,
    location_label: event.location_label,
    latitude: coords.latitude,
    longitude: coords.longitude,
    severity: event.severity,
    entity_name: event.headline || event.company || "Signal",
    explanation: event.explanation || event.headline || "",
  }
}
