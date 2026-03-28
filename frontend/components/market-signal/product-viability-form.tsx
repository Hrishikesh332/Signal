"use client"

import { ChangeEvent, FormEvent, useState } from "react"
import { ChevronDown, ChevronUp, ImagePlus, Loader2, WandSparkles } from "lucide-react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Textarea } from "@/components/ui/textarea"
import { cn } from "@/lib/utils"

export type ResearchDepth = "standard" | "deep"

export interface ProductViabilitySubmission {
  query: string
  productName: string
  category: string
  pricePoint: string
  targetCustomer: string
  marketContext: string
  researchDepth: ResearchDepth
  images: File[]
}

interface ProductViabilityFormProps {
  isSubmitting: boolean
  errorMessage?: string | null
  onSubmit: (submission: ProductViabilitySubmission) => Promise<void> | void
}

const DEFAULT_FORM_STATE: ProductViabilitySubmission = {
  query: "",
  productName: "",
  category: "",
  pricePoint: "",
  targetCustomer: "",
  marketContext: "",
  researchDepth: "standard",
  images: [],
}

export function ProductViabilityForm({
  isSubmitting,
  errorMessage,
  onSubmit,
}: ProductViabilityFormProps) {
  const [submission, setSubmission] = useState<ProductViabilitySubmission>(DEFAULT_FORM_STATE)
  const [advancedOpen, setAdvancedOpen] = useState(false)
  const [localError, setLocalError] = useState<string | null>(null)

  const handleFileChange = (event: ChangeEvent<HTMLInputElement>) => {
    const nextFiles = Array.from(event.target.files || [])
    setSubmission((current) => ({
      ...current,
      images: nextFiles,
    }))
  }

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    if (!submission.query.trim() && submission.images.length === 0) {
      setLocalError("Add a product description or upload at least one image to start the analysis.")
      return
    }

    setLocalError(null)
    await onSubmit(submission)
  }

  const surfaceError = localError || errorMessage

  return (
    <section className="relative overflow-hidden rounded-[28px] border border-[#202024] bg-[#0b0b0d]/92 shadow-[0_40px_120px_rgba(0,0,0,0.45)]">
      <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_left,rgba(34,197,94,0.12),transparent_35%),radial-gradient(circle_at_bottom_right,rgba(255,255,255,0.04),transparent_28%)]" />
      <div className="relative border-b border-[#202024] px-6 py-5">
        <div className="flex items-center gap-3">
          <div className="flex h-10 w-10 items-center justify-center rounded-2xl border border-[#1f2937] bg-[#111214] text-[#22c55e]">
            <WandSparkles className="h-4 w-4" />
          </div>
          <div>
            <p className="text-[10px] tracking-[0.28em] text-[#71717a]">PRODUCT VIABILITY</p>
            <h1 className="mt-1 text-lg font-semibold tracking-[0.08em] text-white">Commercial Signal Console</h1>
          </div>
        </div>
        <p className="mt-4 max-w-xl text-sm leading-6 text-[#a1a1aa]">
          Describe a concept in natural language, optionally attach product images, and let TinyFish map the live market
          before the system condenses the result into a product-facing decision brief.
        </p>
      </div>

      <form className="relative space-y-6 px-6 py-6" onSubmit={handleSubmit}>
        <div className="space-y-2">
          <Label className="text-[11px] tracking-[0.22em] text-[#d4d4d8]">Natural Language Brief</Label>
          <Textarea
            value={submission.query}
            onChange={(event) =>
              setSubmission((current) => ({
                ...current,
                query: event.target.value,
              }))
            }
            placeholder="Would a portable espresso maker for travelers and campers be commercially viable?"
            className="min-h-40 rounded-[22px] border-[#202024] bg-[#09090b]/70 px-4 py-4 text-sm leading-6 text-white placeholder:text-[#52525b] focus-visible:border-[#22c55e]/50 focus-visible:ring-[#22c55e]/20"
          />
          <p className="text-xs text-[#52525b]">
            Natural language is the primary input. Use advanced fields only if you want to anchor the research further.
          </p>
        </div>

        <div className="space-y-3">
          <div className="flex items-center justify-between">
            <Label className="text-[11px] tracking-[0.22em] text-[#d4d4d8]">Research Depth</Label>
            <span className="text-[10px] tracking-[0.22em] text-[#52525b]">TinyFish live market search</span>
          </div>
          <div className="grid grid-cols-2 gap-3">
            {(["standard", "deep"] as ResearchDepth[]).map((option) => {
              const selected = submission.researchDepth === option
              return (
                <button
                  key={option}
                  type="button"
                  onClick={() =>
                    setSubmission((current) => ({
                      ...current,
                      researchDepth: option,
                    }))
                  }
                  className={cn(
                    "rounded-2xl border px-4 py-3 text-left transition-all",
                    selected
                      ? "border-[#22c55e]/45 bg-[#0d1510] text-white shadow-[0_0_0_1px_rgba(34,197,94,0.22)]"
                      : "border-[#202024] bg-[#09090b]/70 text-[#a1a1aa] hover:border-[#2f2f35] hover:text-white",
                  )}
                >
                  <div className="text-[11px] font-semibold uppercase tracking-[0.22em]">{option}</div>
                  <div className="mt-2 text-xs leading-5 text-inherit/80">
                    {option === "standard"
                      ? "One focused TinyFish research pass for a faster market read."
                      : "Multiple TinyFish lanes for broader competitor, pricing, and demand coverage."}
                  </div>
                </button>
              )
            })}
          </div>
        </div>

        <div className="space-y-3">
          <div className="flex items-center justify-between">
            <Label className="text-[11px] tracking-[0.22em] text-[#d4d4d8]">Reference Images</Label>
            <span className="text-[10px] tracking-[0.22em] text-[#52525b]">Optional</span>
          </div>
          <label className="flex cursor-pointer flex-col items-center justify-center gap-3 rounded-[22px] border border-dashed border-[#2a2a30] bg-[#09090b]/70 px-5 py-8 text-center transition-colors hover:border-[#22c55e]/35 hover:bg-[#0c120d]">
            <div className="flex h-12 w-12 items-center justify-center rounded-2xl border border-[#202024] bg-[#111214] text-[#22c55e]">
              <ImagePlus className="h-5 w-5" />
            </div>
            <div>
              <p className="text-sm font-medium text-white">Drop concept images or click to attach</p>
              <p className="mt-1 text-xs text-[#71717a]">PNG, JPG, WEBP, and GIF are supported by the backend.</p>
            </div>
            <input
              type="file"
              multiple
              accept="image/png,image/jpeg,image/webp,image/gif"
              className="sr-only"
              onChange={handleFileChange}
            />
          </label>
          {submission.images.length > 0 ? (
            <div className="space-y-2">
              {submission.images.map((file) => (
                <div
                  key={`${file.name}-${file.size}`}
                  className="flex items-center justify-between rounded-2xl border border-[#202024] bg-[#09090b]/60 px-4 py-3"
                >
                  <div>
                    <p className="text-sm text-white">{file.name}</p>
                    <p className="text-xs text-[#71717a]">{Math.max(1, Math.round(file.size / 1024))} KB</p>
                  </div>
                </div>
              ))}
            </div>
          ) : null}
        </div>

        <Collapsible open={advancedOpen} onOpenChange={setAdvancedOpen}>
          <div className="rounded-[22px] border border-[#202024] bg-[#09090b]/65">
            <CollapsibleTrigger className="flex w-full items-center justify-between px-5 py-4 text-left">
              <div>
                <p className="text-[11px] tracking-[0.24em] text-[#71717a]">ADVANCED DETAILS</p>
                <p className="mt-1 text-sm text-white">Anchor the research with explicit market signals</p>
              </div>
              <div className="rounded-full border border-[#202024] bg-[#111214] p-2 text-[#71717a]">
                {advancedOpen ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
              </div>
            </CollapsibleTrigger>
            <CollapsibleContent className="border-t border-[#202024] px-5 py-5">
              <div className="grid gap-4 md:grid-cols-2">
                <Field
                  label="Product Name"
                  value={submission.productName}
                  onChange={(value) => setSubmission((current) => ({ ...current, productName: value }))}
                  placeholder="Pocket Brewer"
                />
                <Field
                  label="Category"
                  value={submission.category}
                  onChange={(value) => setSubmission((current) => ({ ...current, category: value }))}
                  placeholder="consumer hardware"
                />
                <Field
                  label="Price Point"
                  value={submission.pricePoint}
                  onChange={(value) => setSubmission((current) => ({ ...current, pricePoint: value }))}
                  placeholder="$79"
                />
                <Field
                  label="Target Customer"
                  value={submission.targetCustomer}
                  onChange={(value) => setSubmission((current) => ({ ...current, targetCustomer: value }))}
                  placeholder="travelers and campers"
                />
              </div>
              <div className="mt-4 space-y-2">
                <Label className="text-[11px] tracking-[0.22em] text-[#d4d4d8]">Market Context</Label>
                <Textarea
                  value={submission.marketContext}
                  onChange={(event) =>
                    setSubmission((current) => ({
                      ...current,
                      marketContext: event.target.value,
                    }))
                  }
                  placeholder="Crowded specialty coffee category with rising outdoor lifestyle demand."
                  className="min-h-24 rounded-2xl border-[#202024] bg-[#09090b]/70 text-sm text-white placeholder:text-[#52525b]"
                />
              </div>
            </CollapsibleContent>
          </div>
        </Collapsible>

        {surfaceError ? (
          <Alert className="border-[#552125] bg-[#170c0d] text-[#fecaca]">
            <AlertTitle>Request Issue</AlertTitle>
            <AlertDescription>{surfaceError}</AlertDescription>
          </Alert>
        ) : null}

        <div className="flex flex-col gap-3 border-t border-[#202024] pt-5 sm:flex-row sm:items-center sm:justify-between">
          <div className="text-xs leading-5 text-[#71717a]">
            The first version is synchronous. If TinyFish is still running, the page will surface a pending state rather than
            fabricating a final verdict.
          </div>
          <Button
            type="submit"
            disabled={isSubmitting}
            className="h-11 rounded-full bg-white px-6 text-xs font-semibold uppercase tracking-[0.24em] text-black hover:bg-white/90"
          >
            {isSubmitting ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
            Analyze Product
          </Button>
        </div>
      </form>
    </section>
  )
}

interface FieldProps {
  label: string
  value: string
  onChange: (value: string) => void
  placeholder: string
}

function Field({ label, value, onChange, placeholder }: FieldProps) {
  return (
    <div className="space-y-2">
      <Label className="text-[11px] tracking-[0.22em] text-[#d4d4d8]">{label}</Label>
      <Input
        value={value}
        onChange={(event) => onChange(event.target.value)}
        placeholder={placeholder}
        className="h-11 rounded-2xl border-[#202024] bg-[#09090b]/70 text-sm text-white placeholder:text-[#52525b]"
      />
    </div>
  )
}
