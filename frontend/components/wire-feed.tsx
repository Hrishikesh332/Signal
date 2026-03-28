"use client"

import { useEffect, useState, useRef } from "react"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Badge } from "@/components/ui/badge"
import {
  AlertTriangle,
  Globe,
  TrendingUp,
  Shield,
  Zap,
  Radio,
  Cloud,
  Cpu,
} from "lucide-react"

interface WireItem {
  id: string
  timestamp: Date
  category: "conflict" | "economic" | "political" | "cyber" | "disaster" | "health" | "infrastructure"
  title: string
  location: string
  severity: "low" | "medium" | "high" | "critical"
  isBreaking?: boolean
}

const categoryIcons = {
  conflict: AlertTriangle,
  economic: TrendingUp,
  political: Globe,
  cyber: Cpu,
  disaster: Cloud,
  health: Shield,
  infrastructure: Zap,
}

const categoryColors = {
  conflict: "text-red-500",
  economic: "text-green-500",
  political: "text-blue-500",
  cyber: "text-purple-500",
  disaster: "text-orange-500",
  health: "text-teal-500",
  infrastructure: "text-yellow-500",
}

const severityColors = {
  low: "bg-green-500/20 text-green-400 border-green-500/30",
  medium: "bg-yellow-500/20 text-yellow-400 border-yellow-500/30",
  high: "bg-orange-500/20 text-orange-400 border-orange-500/30",
  critical: "bg-red-500/20 text-red-400 border-red-500/30",
}

const sampleWireItems: Omit<WireItem, "id" | "timestamp">[] = [
  {
    category: "economic",
    title: "Asian markets open higher amid renewed trade optimism",
    location: "Tokyo, Japan",
    severity: "medium",
  },
  {
    category: "political",
    title: "EU leaders convene for emergency summit on energy policy",
    location: "Brussels, Belgium",
    severity: "high",
  },
  {
    category: "cyber",
    title: "Critical infrastructure alert: Telecommunications sector targeted",
    location: "Multiple Regions",
    severity: "critical",
    isBreaking: true,
  },
  {
    category: "disaster",
    title: "Seismic activity detected in Pacific Ring region",
    location: "Philippines",
    severity: "medium",
  },
  {
    category: "conflict",
    title: "UN peacekeeping mission reports escalation in border zone",
    location: "Eastern Europe",
    severity: "high",
  },
  {
    category: "health",
    title: "WHO monitoring new respiratory illness clusters",
    location: "Southeast Asia",
    severity: "medium",
  },
  {
    category: "economic",
    title: "Federal Reserve signals potential rate adjustment",
    location: "Washington D.C., USA",
    severity: "high",
  },
  {
    category: "infrastructure",
    title: "Major shipping lane disruption reported in Suez Canal",
    location: "Egypt",
    severity: "high",
  },
  {
    category: "political",
    title: "G7 finance ministers reach agreement on digital currency framework",
    location: "Rome, Italy",
    severity: "medium",
  },
  {
    category: "cyber",
    title: "Financial sector reports coordinated DDoS attacks",
    location: "Global",
    severity: "high",
  },
]

function formatTimeAgo(date: Date): string {
  const seconds = Math.floor((Date.now() - date.getTime()) / 1000)
  if (seconds < 60) return `${seconds}s ago`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.floor(minutes / 60)
  return `${hours}h ago`
}

function WireItemCard({ item }: { item: WireItem }) {
  const Icon = categoryIcons[item.category]
  const [timeAgo, setTimeAgo] = useState(formatTimeAgo(item.timestamp))

  useEffect(() => {
    const interval = setInterval(() => {
      setTimeAgo(formatTimeAgo(item.timestamp))
    }, 10000)
    return () => clearInterval(interval)
  }, [item.timestamp])

  return (
    <div
      className={`group relative border-b border-border p-4 transition-colors hover:bg-secondary/30 ${
        item.isBreaking ? "bg-red-500/5" : ""
      }`}
    >
      {item.isBreaking && (
        <div className="mb-2 flex items-center gap-2">
          <span className="relative flex h-2 w-2">
            <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-red-400 opacity-75"></span>
            <span className="relative inline-flex h-2 w-2 rounded-full bg-red-500"></span>
          </span>
          <span className="text-xs font-bold uppercase tracking-wider text-red-500">
            Breaking
          </span>
        </div>
      )}

      <div className="flex items-start gap-3">
        <div className={`mt-0.5 ${categoryColors[item.category]}`}>
          <Icon className="size-4" />
        </div>

        <div className="flex-1 min-w-0">
          <h3 className="text-sm font-medium leading-tight text-foreground group-hover:text-primary transition-colors">
            {item.title}
          </h3>

          <div className="mt-2 flex flex-wrap items-center gap-2 text-xs">
            <Badge
              variant="outline"
              className={`${severityColors[item.severity]} border text-[10px] px-1.5 py-0`}
            >
              {item.severity.toUpperCase()}
            </Badge>
            <span className="text-muted-foreground">{item.location}</span>
            <span className="text-muted-foreground/60">•</span>
            <span className="text-muted-foreground/60">{timeAgo}</span>
          </div>
        </div>
      </div>
    </div>
  )
}

export function WireFeed() {
  const [items, setItems] = useState<WireItem[]>([])
  const [isConnected, setIsConnected] = useState(false)
  const scrollRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    // Simulate connection
    const connectTimeout = setTimeout(() => {
      setIsConnected(true)
    }, 1500)

    // Initialize with sample items
    const initTimeout = setTimeout(() => {
      const initialItems = sampleWireItems.slice(0, 5).map((item, index) => ({
        ...item,
        id: `initial-${index}`,
        timestamp: new Date(Date.now() - index * 120000),
      }))
      setItems(initialItems)
    }, 2000)

    // Add new items periodically
    const interval = setInterval(() => {
      setItems((prev) => {
        const randomItem = sampleWireItems[Math.floor(Math.random() * sampleWireItems.length)]
        const newItem: WireItem = {
          ...randomItem,
          id: `item-${Date.now()}`,
          timestamp: new Date(),
          isBreaking: Math.random() > 0.9,
        }
        return [newItem, ...prev].slice(0, 50)
      })
    }, 8000)

    return () => {
      clearTimeout(connectTimeout)
      clearTimeout(initTimeout)
      clearInterval(interval)
    }
  }, [])

  return (
    <div className="flex h-full flex-col">
      {/* Header */}
      <div className="flex items-center justify-between border-b border-border px-4 py-3">
        <div className="flex items-center gap-2">
          <Radio className="size-4 text-primary" />
          <span className="text-sm font-semibold tracking-wide">THE WIRE</span>
        </div>
        <div className="flex items-center gap-2">
          <span
            className={`relative flex h-2 w-2 ${
              isConnected ? "" : "opacity-50"
            }`}
          >
            {isConnected && (
              <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-green-400 opacity-75"></span>
            )}
            <span
              className={`relative inline-flex h-2 w-2 rounded-full ${
                isConnected ? "bg-green-500" : "bg-muted"
              }`}
            ></span>
          </span>
          <span className="text-xs text-muted-foreground">
            {isConnected ? `${items.length} ACTIVE` : "CONNECTING..."}
          </span>
        </div>
      </div>

      {/* Feed */}
      <ScrollArea className="flex-1">
        <div ref={scrollRef}>
          {!isConnected ? (
            <div className="flex items-center justify-center p-8 text-muted-foreground">
              <div className="flex items-center gap-2">
                <div className="size-4 animate-spin rounded-full border-2 border-primary border-t-transparent" />
                <span className="text-sm">LOADING THE WIRE</span>
              </div>
            </div>
          ) : items.length === 0 ? (
            <div className="flex items-center justify-center p-8 text-muted-foreground">
              <span className="text-sm">Waiting for updates...</span>
            </div>
          ) : (
            items.map((item) => <WireItemCard key={item.id} item={item} />)
          )}
        </div>
      </ScrollArea>
    </div>
  )
}
