"use client"

import {
  Filter,
  Plus,
  Minus,
  Maximize2,
  User,
  Settings,
} from "lucide-react"

interface RightControlsProps {
  onZoomIn?: () => void
  onZoomOut?: () => void
  onReset?: () => void
}

export function RightControls({ onZoomIn, onZoomOut, onReset }: RightControlsProps) {
  const controls = [
    { id: "filter", label: "FILTER", icon: Filter, action: undefined },
    { id: "zoom-in", label: "ZOOM IN", icon: Plus, action: onZoomIn },
    { id: "zoom-out", label: "ZOOM OUT", icon: Minus, action: onZoomOut },
    { id: "reset", label: "RESET", icon: Maximize2, action: onReset },
    { id: "sign-in", label: "SIGN IN", icon: User, action: undefined },
    { id: "settings", label: "SETTINGS", icon: Settings, action: undefined },
  ]

  return (
    <div className="flex flex-col gap-1 rounded-l border-l border-t border-b border-[#1a3a2a] bg-[#0c1a14]/90 py-2 backdrop-blur-sm">
      {controls.map((control) => {
        const Icon = control.icon
        return (
          <button
            key={control.id}
            onClick={control.action}
            className="flex items-center gap-2 px-4 py-2 font-mono text-xs text-[#22c55e]/80 transition-colors hover:bg-[#22c55e]/10 hover:text-[#22c55e]"
          >
            <Icon className="size-4" />
            <span className="tracking-wide">{control.label}</span>
          </button>
        )
      })}
    </div>
  )
}
