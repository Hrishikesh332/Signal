import { NextRequest, NextResponse } from "next/server"

import { buildBackendUrl, runBackendPayloadBridge } from "@/lib/backend-bridge"

export const runtime = "nodejs"

export async function GET(request: NextRequest) {
  try {
    const target = buildBackendUrl("/api/v1/competitor-intelligence", request)
    const upstream = await fetch(target, {
      cache: "no-store",
      headers: {
        Accept: "application/json",
      },
    })
    const body = await upstream.text()
    return new NextResponse(body, {
      status: upstream.status,
      headers: {
        "content-type": upstream.headers.get("content-type") || "application/json",
      },
    })
  } catch {
    try {
      return await runBackendPayloadBridge("competitor-intelligence", request)
    } catch {
      return NextResponse.json(
        {
          error: {
            code: "competitor_intelligence_proxy_unavailable",
            message: "Unable to load competitor intelligence from the backend API.",
          },
        },
        { status: 502 }
      )
    }
  }
}
