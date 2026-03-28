"use client"

import { Header } from "@/components/market-signal/header"

export default function MainLayout({ children }: { children: React.ReactNode }) {
  return (
    <main className="relative w-full h-screen overflow-hidden bg-[#09090b]">
      <Header />
      {children}
    </main>
  )
}
