/**
 * Same-origin fetch to this app's /api routes. Adds headers so requests succeed
 * through ngrok's free tier (avoids the browser-warning HTML on API calls).
 */
export function fetchAppApi(input: string | URL | Request, init?: RequestInit): Promise<Response> {
  const headers = new Headers(init?.headers)
  headers.set("ngrok-skip-browser-warning", "69420")
  return fetch(input, { ...init, headers })
}
