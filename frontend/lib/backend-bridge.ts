import { execFile } from "node:child_process"
import { existsSync } from "node:fs"
import path from "node:path"
import { promisify } from "node:util"

import { NextRequest, NextResponse } from "next/server"

const execFileAsync = promisify(execFile)

type BridgeEndpoint = "dashboard" | "market-signals"

function resolveProjectRoot() {
  const configuredRoot = process.env.MARKET_MONITOR_PROJECT_ROOT
  const candidates = [
    configuredRoot,
    process.cwd(),
    path.resolve(process.cwd(), ".."),
    path.resolve(process.cwd(), "../.."),
  ].filter((candidate): candidate is string => Boolean(candidate))

  const projectRoot = candidates.find((candidate) =>
    existsSync(path.join(candidate, "backend", "scripts", "get_frontend_payload.py"))
  )

  if (!projectRoot) {
    throw new Error("Unable to resolve the Market Monitor project root.")
  }

  return projectRoot
}

export function buildBackendUrl(pathname: string, request: NextRequest) {
  const configuredBase =
    process.env.MARKET_MONITOR_BACKEND_URL ||
    process.env.NEXT_PUBLIC_MARKET_MONITOR_BACKEND_URL ||
    "http://127.0.0.1:5000"
  const normalizedBase = configuredBase.endsWith("/") ? configuredBase.slice(0, -1) : configuredBase
  const target = new URL(`${normalizedBase}${pathname}`)
  for (const [key, value] of request.nextUrl.searchParams.entries()) {
    target.searchParams.set(key, value)
  }
  return target
}

export async function runBackendPayloadBridge(endpoint: BridgeEndpoint, request: NextRequest) {
  const projectRoot = resolveProjectRoot()
  const scriptPath = path.join(projectRoot, "backend", "scripts", "get_frontend_payload.py")
  const queryEntries = Object.fromEntries(request.nextUrl.searchParams.entries())
  const { stdout } = await execFileAsync("python3", [scriptPath, endpoint, JSON.stringify(queryEntries)], {
    cwd: projectRoot,
  })
  const parsed = JSON.parse(stdout) as { status_code: number; payload: unknown }
  return NextResponse.json(parsed.payload, { status: parsed.status_code })
}
