"use client"

import {
  Radio,
  TrendingUp,
  Tv,
  BarChart3,
  Video,
  ShieldAlert,
  Bug,
  MessageCircle,
} from "lucide-react"
import { cn } from "@/lib/utils"

interface NavItem {
  id: string
  label: string
  icon: React.ElementType
  badge?: string
}

const navItems: NavItem[] = [
  { id: "wire", label: "WIRE", icon: Radio },
  { id: "chat", label: "CHAT", icon: MessageCircle, badge: "118 in chat" },
  { id: "stocks", label: "STOCKS", icon: TrendingUp },
  { id: "tv", label: "TV", icon: Tv },
  { id: "markets", label: "MARKETS", icon: BarChart3 },
  { id: "cameras", label: "CAMERAS", icon: Video },
  { id: "defcon", label: "DEFCON", icon: ShieldAlert },
  { id: "outbreaks", label: "OUTBREAKS", icon: Bug },
]

interface SidebarNavProps {
  onSelect?: (id: string) => void
  selected?: string
}

export function SidebarNav({ onSelect, selected = "wire" }: SidebarNavProps) {
  return (
    <div className="flex h-full flex-col border-r border-[#1a3a2a] bg-[#0c1a14]/90 py-4 backdrop-blur-sm">
      {navItems.map((item) => {
        const Icon = item.icon
        const isActive = selected === item.id

        return (
          <button
            key={item.id}
            onClick={() => onSelect?.(item.id)}
            className={cn(
              "relative flex items-center gap-3 px-4 py-3 font-mono text-xs transition-all",
              isActive
                ? "text-[#22c55e]"
                : "text-[#22c55e]/60 hover:text-[#22c55e]"
            )}
          >
            {/* Active indicator */}
            {isActive && (
              <div className="absolute left-0 top-1/2 h-4 w-0.5 -translate-y-1/2 bg-[#22c55e]" />
            )}

            <Icon className="size-4" />
            <span className="font-medium tracking-wide">{item.label}</span>
            
            {/* Badge */}
            {item.badge && (
              <span className="ml-auto rounded bg-[#ef4444] px-1.5 py-0.5 text-[10px] font-medium text-white">
                {item.badge}
              </span>
            )}
          </button>
        )
      })}
    </div>
  )
}
