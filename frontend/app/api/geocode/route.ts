import { NextRequest, NextResponse } from "next/server"

/** Proxy for OpenStreetMap Nominatim (browser must not call directly without policy compliance). */
export async function GET(req: NextRequest) {
  const q = req.nextUrl.searchParams.get("q")?.trim()
  if (!q) {
    return NextResponse.json({ results: [] })
  }

  const params = new URLSearchParams({
    q,
    format: "json",
    limit: "10",
    addressdetails: "1",
  })

  const res = await fetch(`https://nominatim.openstreetmap.org/search?${params}`, {
    headers: {
      Accept: "application/json",
      "User-Agent": "MarketSignal/1.0 (dashboard map search)",
    },
    next: { revalidate: 0 },
  })

  if (!res.ok) {
    return NextResponse.json({ error: "Geocoding failed" }, { status: 502 })
  }

  const data = await res.json()
  return NextResponse.json({ results: data })
}
