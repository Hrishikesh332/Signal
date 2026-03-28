import { NextRequest, NextResponse } from "next/server"
import { buildBackendUrl, runBackendPayloadBridge } from "@/lib/backend-bridge"

export const runtime = "nodejs"

export async function GET(request: NextRequest) {
  try {
    const target = buildBackendUrl("/api/v1/dashboard", request)
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
      return await runBackendPayloadBridge("dashboard", request)
    } catch {
      return NextResponse.json(
        {
          error: {
            code: "dashboard_proxy_unavailable",
            message: "Unable to load dashboard data from the backend API.",
          },
        },
        { status: 502 }
      )
    }
  }
}
