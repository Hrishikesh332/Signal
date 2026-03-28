"use client"

import { useState } from "react"

import {
  ProductViabilityForm,
  ProductViabilitySubmission,
} from "@/components/market-signal/product-viability-form"
import {
  ProductViabilityResponse,
  ProductViabilityResult,
} from "@/components/market-signal/product-viability-result"

interface ProductViabilityErrorPayload {
  error?: {
    code?: string
    message?: string
  }
}

export default function ProductViabilityPage() {
  const [isSubmitting, setIsSubmitting] = useState(false)
  const [result, setResult] = useState<ProductViabilityResponse | null>(null)
  const [errorMessage, setErrorMessage] = useState<string | null>(null)

  const handleSubmit = async (submission: ProductViabilitySubmission) => {
    setIsSubmitting(true)
    setErrorMessage(null)
    setResult(null)

    const formData = new FormData()
    appendIfPresent(formData, "query", submission.query)
    appendIfPresent(formData, "product_name", submission.productName)
    appendIfPresent(formData, "category", submission.category)
    appendIfPresent(formData, "price_point", submission.pricePoint)
    appendIfPresent(formData, "target_customer", submission.targetCustomer)
    appendIfPresent(formData, "market_context", submission.marketContext)
    formData.append("research_depth", submission.researchDepth)
    submission.images.forEach((image) => {
      formData.append("images", image)
    })

    try {
      const response = await fetch("/api/product-viability", {
        method: "POST",
        body: formData,
      })
      const payload = (await response.json()) as ProductViabilityResponse | ProductViabilityErrorPayload

      if (!response.ok) {
        setErrorMessage(payload && "error" in payload ? payload.error?.message || "Unable to analyze product viability." : "Unable to analyze product viability.")
        return
      }

      setResult(payload as ProductViabilityResponse)
    } catch {
      setErrorMessage("Unable to reach the product viability service.")
    } finally {
      setIsSubmitting(false)
    }
  }

  return (
    <div className="absolute inset-0 overflow-y-auto bg-[#09090b] pt-14">
      <div
        className="absolute inset-0 opacity-[0.035]"
        style={{
          backgroundImage: "radial-gradient(circle at 1px 1px, white 1px, transparent 0)",
          backgroundSize: "30px 30px",
        }}
      />
      <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_left,rgba(34,197,94,0.08),transparent_28%),radial-gradient(circle_at_top_right,rgba(255,255,255,0.03),transparent_22%)]" />

      <div className="relative mx-auto flex min-h-full w-full max-w-[1560px] flex-col px-4 py-6 sm:px-6 lg:px-8">
        <div className="mb-6 flex flex-col gap-3 border-b border-[#18181b] pb-6">
          <p className="text-[10px] tracking-[0.32em] text-[#52525b]">TACTICAL PRODUCT RESEARCH</p>
          <div className="flex flex-col gap-3 xl:flex-row xl:items-end xl:justify-between">
            <div className="max-w-3xl">
              <h1 className="text-2xl font-semibold tracking-[0.06em] text-white sm:text-3xl">
                Product Viability Command Surface
              </h1>
              <p className="mt-3 text-sm leading-7 text-[#a1a1aa] sm:text-base">
                Submit a concept in plain language, attach reference imagery if helpful, and inspect the live market readout
                without leaving the operating surface.
              </p>
            </div>
            <div className="grid grid-cols-2 gap-3 text-xs tracking-[0.22em] text-[#71717a] sm:w-fit">
              <StatusChip label="Input" value="Natural language" />
              <StatusChip label="Engine" value="TinyFish" />
              <StatusChip label="Mode" value="Standard / Deep" />
              <StatusChip label="Output" value="Actionable brief" />
            </div>
          </div>
        </div>

        <div className="grid flex-1 gap-6 xl:grid-cols-[minmax(0,30rem)_minmax(0,1fr)]">
          <div className="xl:sticky xl:top-20 xl:self-start">
            <ProductViabilityForm
              isSubmitting={isSubmitting}
              errorMessage={errorMessage}
              onSubmit={handleSubmit}
            />
          </div>
          <ProductViabilityResult
            result={result}
            isSubmitting={isSubmitting}
            errorMessage={errorMessage}
          />
        </div>
      </div>
    </div>
  )
}

function appendIfPresent(formData: FormData, name: string, value: string) {
  if (value.trim()) {
    formData.append(name, value.trim())
  }
}

function StatusChip({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-2xl border border-[#202024] bg-[#0d0e10]/80 px-3 py-3 shadow-[inset_0_1px_0_rgba(255,255,255,0.03)]">
      <div className="text-[10px] tracking-[0.22em] text-[#52525b]">{label}</div>
      <div className="mt-1 text-[11px] text-[#d4d4d8]">{value}</div>
    </div>
  )
}
