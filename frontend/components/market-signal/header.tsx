"use client"

import Link from "next/link"
import { usePathname } from "next/navigation"
import { Globe2, Radio, LayoutDashboard } from "lucide-react"

export function Header() {
  const pathname = usePathname()
  const isDashboard = pathname === "/dashboard"
  const isLatest = pathname === "/latest" || pathname === "/"

  return (
    <header className="absolute top-0 left-0 right-0 z-50 flex items-center justify-between px-6 h-14 bg-[#09090b]/80 backdrop-blur-md border-b border-[#27272a]/50">
      <Link href="/latest" className="flex items-center gap-2">
        <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-emerald-500 to-emerald-600 flex items-center justify-center">
          <Globe2 className="w-4 h-4 text-white" />
        </div>
        <span className="text-white font-semibold tracking-tight text-sm">Market Signal</span>
      </Link>

      <nav className="flex items-center gap-1 bg-[#18181b]/80 p-1 rounded-full border border-[#27272a]/50">
        <Link
          href="/dashboard"
          className={`flex items-center gap-2 px-4 py-1.5 text-xs font-medium tracking-wide transition-all duration-200 rounded-full ${
            isDashboard
              ? "bg-white text-black shadow-lg"
              : "text-[#a1a1aa] hover:text-white"
          }`}
        >
          <LayoutDashboard className="w-3.5 h-3.5" />
          Dashboard
        </Link>
        <Link
          href="/latest"
          className={`flex items-center gap-2 px-4 py-1.5 text-xs font-medium tracking-wide transition-all duration-200 rounded-full ${
            isLatest
              ? "bg-white text-black shadow-lg"
              : "text-[#a1a1aa] hover:text-white"
          }`}
        >
          <Radio className="w-3.5 h-3.5" />
          Latest
        </Link>
      </nav>
    </header>
  )
}
