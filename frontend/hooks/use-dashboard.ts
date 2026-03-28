import useSWR from "swr"
import type { DashboardResponse } from "@/lib/dashboard-types"

const fetcher = (url: string) => fetch(url).then((res) => res.json())

export function useDashboard() {
  const { data, error, isLoading, mutate } = useSWR<DashboardResponse>(
    "/api/v1/dashboard",
    fetcher,
    {
      refreshInterval: 0, // Manual refresh only
      revalidateOnFocus: false,
    }
  )

  const refresh = async () => {
    await mutate(
      fetch("/api/v1/dashboard?refresh=true").then((res) => res.json()),
      { revalidate: false }
    )
  }

  return {
    data,
    error,
    isLoading,
    refresh,
  }
}
