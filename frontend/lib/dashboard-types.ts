export interface DashboardMeta {
  generated_at: string
  latest_snapshot_at: string
  source_count: number
  snapshot_count: number
  integrations: string[]
}

export interface KPI {
  id: string
  label: string
  value: string | number
}

export interface MapPoint {
  id?: string
  category?: string
  company?: string
  timestamp?: string
  location_label?: string
  latitude: number
  longitude: number
  severity: "critical" | "high" | "medium" | "low"
  entity_name: string
  explanation: string
}

export interface DashboardEvent {
  id: string
  severity: "critical" | "high" | "medium" | "low"
  category: string
  company: string
  timestamp: string
  headline: string
  explanation?: string
  location_label?: string
  locations?: string[]
  latitude?: number
  longitude?: number
}

export interface Alert {
  id: string
  severity: "critical" | "high" | "medium" | "low"
  message: string
  timestamp: string
}

export interface StrategicInsight {
  id: string
  title: string
  description: string
  category: string
}

export interface CompanyRollup {
  company: string
  signals: number
  trend: "up" | "down" | "stable"
  severity: "critical" | "high" | "medium" | "low"
}

export interface TrendDataPoint {
  date: string
  value: number
}

export interface TrendSeries {
  jobs: TrendDataPoint[]
  products: TrendDataPoint[]
  funding: TrendDataPoint[]
  markets: TrendDataPoint[]
  role_clusters: TrendDataPoint[]
}

export interface GrowthIntelligence {
  kpis: KPI[]
  strategic_insights: StrategicInsight[]
  company_rollups: CompanyRollup[]
  trend_series: TrendSeries
}

export interface SourceHealth {
  provider: string
  status: "healthy" | "degraded" | "failed"
  last_run_at: string
  success_rate: number
  avg_runtime_ms: number
  snapshots_total: number
  last_error?: {
    code: string
    message: string
  }
}

export interface DashboardData {
  kpis: KPI[]
  map: {
    points: MapPoint[]
  }
  events: DashboardEvent[]
  alerts: Alert[]
  growth_intelligence: GrowthIntelligence
  source_health: SourceHealth[]
}

export interface DashboardResponse {
  meta: DashboardMeta
  dashboard: DashboardData
}
