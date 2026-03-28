"use client"

import { Search } from "lucide-react"
import { useState } from "react"

export function LocateSearch() {
  const [query, setQuery] = useState("")

  return (
    <div className="flex items-center gap-2 rounded border border-[#1a3a2a] bg-[#0c1a14]/90 px-4 py-2 backdrop-blur-sm">
      <Search className="size-4 text-[#22c55e]/60" />
      <span className="font-mono text-xs text-[#22c55e]">LOCATE</span>
      <span className="text-[#22c55e]/40">/</span>
      <input
        type="text"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        placeholder=""
        className="w-32 bg-transparent font-mono text-xs text-[#22c55e] placeholder-[#22c55e]/40 outline-none"
      />
    </div>
  )
}
