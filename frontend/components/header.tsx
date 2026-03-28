"use client"

import { useState, useEffect } from "react"
import { Globe, User } from "lucide-react"

interface HeaderProps {
  activeView: "map" | "wire" | "globe"
  onViewChange: (view: "map" | "wire" | "globe") => void
}

export function Header({ activeView, onViewChange }: HeaderProps) {
  const [currentTime, setCurrentTime] = useState("")
  const [currentDate, setCurrentDate] = useState("")

  useEffect(() => {
    const updateTime = () => {
      const now = new Date()
      setCurrentDate(
        now.toLocaleDateString("en-US", {
          month: "2-digit",
          day: "2-digit",
          year: "numeric",
        }).replace(/\//g, "/")
      )
      setCurrentTime(
        now.toLocaleTimeString("en-US", {
          hour: "2-digit",
          minute: "2-digit",
          second: "2-digit",
          hour12: false,
          timeZone: "UTC",
        }) + " UTC"
      )
    }
    updateTime()
    const interval = setInterval(updateTime, 1000)
    return () => clearInterval(interval)
  }, [])

  const views = [
    { id: "map" as const, label: "THE MAP" },
    { id: "wire" as const, label: "THE WIRE" },
    { id: "globe" as const, label: "THE GLOBE" },
  ]

  return (
    <header className="flex h-12 items-center justify-between border-b border-[#1a3a2a] bg-[#0a1612] px-4">
      {/* Logo */}
      <div className="flex items-center gap-4">
        <div className="flex items-center gap-2">
          <Globe className="size-5 text-[#22c55e]" />
          <span className="font-mono text-sm font-bold tracking-wide text-[#22c55e]">
            WORLD MONITOR
          </span>
          <span className="rounded border border-[#22c55e]/30 bg-[#22c55e]/10 px-1.5 py-0.5 font-mono text-[10px] text-[#22c55e]">
            BETA
          </span>
        </div>
        <button className="rounded border border-[#22c55e]/30 px-3 py-1 font-mono text-xs text-[#22c55e] transition-colors hover:bg-[#22c55e]/10">
          HOW IT WORKS
        </button>
      </div>

      {/* View Tabs */}
      <nav className="flex items-center gap-1">
        {views.map((view) => (
          <button
            key={view.id}
            onClick={() => onViewChange(view.id)}
            className={`px-4 py-1.5 font-mono text-xs transition-all ${
              activeView === view.id
                ? "border border-[#22c55e] bg-[#22c55e]/10 text-[#22c55e]"
                : "border border-transparent text-[#22c55e]/60 hover:text-[#22c55e]"
            }`}
          >
            {view.label}
          </button>
        ))}
      </nav>

      {/* Right Section */}
      <div className="flex items-center gap-3">
        <button className="flex items-center gap-2 rounded border border-[#22c55e]/30 px-3 py-1 font-mono text-xs text-[#22c55e] transition-colors hover:bg-[#22c55e]/10">
          <User className="size-3" />
          SIGN IN
        </button>
        <button className="rounded border border-[#22c55e]/30 px-3 py-1 font-mono text-xs text-[#22c55e] transition-colors hover:bg-[#22c55e]/10">
          FOLLOW WORLD MONITOR ON{" "}
          <span className="font-bold">X</span>
        </button>
        <span className="font-mono text-xs text-[#22c55e]/80">
          {currentDate} {currentTime}
        </span>
      </div>
    </header>
  )
}
