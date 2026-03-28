import { NextResponse } from "next/server"

const DEFAULT_BACKEND_ORIGIN = "http://127.0.0.1:5000"

export async function POST(request: Request) {
  const backendOrigin = (process.env.MARKET_SIGNAL_API_BASE_URL || DEFAULT_BACKEND_ORIGIN).replace(/\/$/, "")
  const upstreamUrl = `${backendOrigin}/api/v1/product-viability`

  try {
    const formData = await request.formData()
    const upstreamResponse = await fetch(upstreamUrl, {
      method: "POST",
      body: formData,
      cache: "no-store",
    })

    const responseText = await upstreamResponse.text()
    const contentType = upstreamResponse.headers.get("content-type") || "application/json"

    return new NextResponse(responseText, {
      status: upstreamResponse.status,
      headers: {
        "Content-Type": contentType,
      },
    })
  } catch {
    return NextResponse.json(
      {
        error: {
          code: "product_viability_proxy_unavailable",
          message: "Unable to reach the product viability backend.",
        },
      },
      { status: 502 },
    )
  }
}
