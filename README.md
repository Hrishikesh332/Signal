# Market Signal

Market Signal is a two-part application for live market monitoring and research:

- a Flask backend that orchestrates TinyFish runs, persists snapshots and source-run history, and exposes JSON APIs
- a Next.js frontend that renders a tactical monitoring UI for the dashboard, latest signal wire, competitor analysis, and product viability research

The project is built around TinyFish as the live web research engine. OpenAI is used where synthesis or ranking is helpful, especially for competitor analysis and optional image-aware product viability analysis.

## What It Does

- Tracks configured market/news sources and stores both snapshots and source-run history
- Builds a live market-signal wire across sources
- Supports ad hoc competitor analysis for a submitted company URL
- Supports product viability analysis from natural language and optional images
- Exposes watcher QA and replay endpoints for debugging the collection pipeline

## Current App Surfaces

Frontend routes:

- `/dashboard`: map-first monitoring interface
- `/latest`: live market-signal wire
- `/competitors`: competitor landscape workflow
- `/product-viability`: TinyFish-backed product viability workflow

Backend API routes:

- `GET /api/v1/dashboard`
- `GET /api/v1/market-signals`
- `GET /api/v1/market-signals/<signal_id>`
- `POST /api/v1/market-signals/<signal_id>/lifecycle`
- `GET /api/v1/commerce-intelligence`
- `GET /api/v1/commerce-intelligence/signals`
- `GET /api/v1/commerce-intelligence/history`
- `GET /api/v1/growth-intelligence`
- `GET /api/v1/growth-intelligence/events`
- `GET /api/v1/growth-intelligence/history`
- `GET /api/v1/growth-intelligence/trends`
- `GET /api/v1/competitor-intelligence`
- `POST /api/v1/product-viability`
- `GET /api/v1/watcher-qa`
- `POST /api/v1/watcher-qa/replay`

## Architecture

### Backend

The backend lives under [`backend/market_monitor_api`](/home/ckwflash/repos/Signal/backend/market_monitor_api).

Key pieces:

- [`config.py`](/home/ckwflash/repos/Signal/backend/market_monitor_api/config.py): env loading and runtime settings
- [`services/tinyfish.py`](/home/ckwflash/repos/Signal/backend/market_monitor_api/services/tinyfish.py): TinyFish request/polling logic, source loading, snapshot persistence, source-run persistence
- [`services/market_signals.py`](/home/ckwflash/repos/Signal/backend/market_monitor_api/services/market_signals.py): live wire aggregation, category status, watcher QA helpers
- [`services/competitor_intelligence.py`](/home/ckwflash/repos/Signal/backend/market_monitor_api/services/competitor_intelligence.py): ad hoc company profiling + competitor landscape assembly
- [`services/product_viability.py`](/home/ckwflash/repos/Signal/backend/market_monitor_api/services/product_viability.py): natural-language/image intake, TinyFish live research, optional OpenAI synthesis, frontend response shaping
- [`services/openai_service.py`](/home/ckwflash/repos/Signal/backend/market_monitor_api/services/openai_service.py): OpenAI Responses API integration

### Frontend

The frontend lives under [`frontend`](/home/ckwflash/repos/Signal/frontend) and uses Next.js App Router.

Patterns used in the frontend:

- app pages under [`frontend/app`](/home/ckwflash/repos/Signal/frontend/app)
- Next API proxies under [`frontend/app/api`](/home/ckwflash/repos/Signal/frontend/app/api)
- tactical UI components under [`frontend/components/market-signal`](/home/ckwflash/repos/Signal/frontend/components/market-signal)

General backend-backed pages proxy through Next route handlers such as:

- [`frontend/app/api/market-signals/route.ts`](/home/ckwflash/repos/Signal/frontend/app/api/market-signals/route.ts)
- [`frontend/app/api/competitor-intelligence/route.ts`](/home/ckwflash/repos/Signal/frontend/app/api/competitor-intelligence/route.ts)
- [`frontend/app/api/product-viability/route.ts`](/home/ckwflash/repos/Signal/frontend/app/api/product-viability/route.ts)

## Data Model And Persistence

Configured sources live in [`backend/config/sources.json`](/home/ckwflash/repos/Signal/backend/config/sources.json).

At the moment, the checked-in source catalog contains 8 `reputation_intelligence` sources for market/news monitoring. Competitor analysis and product viability also create ad hoc TinyFish runs outside the static source catalog.

The backend persists two different artifacts:

- snapshots in `backend/data/snapshots`
- source-run records in `backend/data/source_runs`

This distinction matters:

- the Latest tab is built from persisted snapshots
- source-run records are useful for run history and QA, but they do not contain the full normalized snapshot payload

If `backend/data/source_runs` has records but `backend/data/snapshots` is empty, the Latest tab will show no live items and the backend will flag a degraded `missing_snapshots` category status.

## Requirements

- Python 3.11+ recommended
- Node.js 20+ recommended
- TinyFish API access for live research/refresh
- OpenAI API access for competitor analysis and optional product-image synthesis

## Local Setup

### 1. Configure environment

Copy the root env template:

```bash
cp .env.example .env
```

Important root env vars:

- `TINYFISH_API_KEY`
- `TINYFISH_BASE_URL`
- `TINYFISH_TIMEOUT_SECONDS`
- `OPENAI_API_KEY`
- `OPENAI_BASE_URL`
- `OPENAI_MODEL`
- `OPENAI_TIMEOUT_SECONDS`
- `MARKET_MONITOR_SOURCE_CONFIG_FILE`
- `MARKET_MONITOR_SNAPSHOT_STORE_DIR`
- `MARKET_MONITOR_SOURCE_RUN_STORE_DIR`
- `MARKET_MONITOR_BACKEND_URL`

Notes:

- The frontend loads the repo-root `.env` via [`frontend/next.config.mjs`](/home/ckwflash/repos/Signal/frontend/next.config.mjs).
- The product viability proxy route also supports `MARKET_SIGNAL_API_BASE_URL`, but defaults to `http://127.0.0.1:5000`.
- If you only want TinyFish-backed text product viability, OpenAI can be left unset.
- Competitor intelligence requires both TinyFish and OpenAI.

### 2. Install backend dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r backend/requirements.txt
```

### 3. Install frontend dependencies

```bash
cd frontend
npm install
cd ..
```

### 4. Run the backend

```bash
python backend/app.py
```

The Flask API runs on `http://127.0.0.1:5000` by default.

### 5. Run the frontend

```bash
cd frontend
npm run dev
```

Open:

- `http://localhost:3000/dashboard`
- `http://localhost:3000/latest`
- `http://localhost:3000/competitors`
- `http://localhost:3000/product-viability`

## Common Workflows

### Refresh market signals

Trigger a refresh through the backend script:

```bash
python backend/scripts/run_market_signal_refresh.py
```

This calls the market-signal pipeline with `refresh=True` and prints a compact summary of:

- `generated_at`
- `schedule_interval_minutes`
- `active_count`
- `latest_snapshot_at`
- `recent_run_count`

### Smoke-test product viability

Use the built-in smoke test:

```bash
python backend/scripts/smoke_test_product_viability.py
```

Useful options:

- `--transport http`
- `--transport test-client`
- `--research-depth standard`
- `--research-depth deep`
- `--query "..."`
- `--image /absolute/path/to/file.png`

Example:

```bash
python backend/scripts/smoke_test_product_viability.py \
  --query "Would a portable espresso maker for travelers be commercially viable?" \
  --research-depth deep
```

### Hit the main APIs manually

Competitor intelligence:

```bash
curl "http://127.0.0.1:5000/api/v1/competitor-intelligence?company_url=https://openai.com&refresh=true&top_n=4"
```

Market signals:

```bash
curl "http://127.0.0.1:5000/api/v1/market-signals?limit=25&market_category=tech"
```

Product viability:

```bash
curl -X POST "http://127.0.0.1:5000/api/v1/product-viability" \
  -F 'query=Would a portable espresso maker for travelers be commercially viable?' \
  -F 'research_depth=standard'
```

## Product Viability Flow

The product viability endpoint is designed for one-shot research.

Input:

- natural-language request via `query`/`prompt`/`request`
- optional structured fields such as product name, category, price point, target customer, and market context
- optional repeated `images`
- `research_depth` of `standard` or `deep`

Behavior:

- TinyFish is the primary research engine
- `standard` runs one live research pass
- `deep` runs multiple focused TinyFish research lanes
- TinyFish output is the primary decision source
- OpenAI is optional and mainly used when image interpretation is needed

Current response shape for the frontend:

- `status`
- `summary`
- `recommendation`
- `viability_score`
- `confidence_score`
- `highlights`
- `competitors`
- `sources`
- `meta`

## Competitor Intelligence Flow

Competitor intelligence works from a submitted public company URL.

High-level pipeline:

1. TinyFish profiles the target company website
2. Market signals are collected for context
3. OpenAI proposes competitor candidates
4. TinyFish profiles competitor candidates
5. OpenAI synthesizes the competitor landscape

The competitor page is implemented in:

- [`frontend/app/(main)/competitors/page.tsx`](/home/ckwflash/repos/Signal/frontend/app/(main)/competitors/page.tsx)
- [`frontend/components/market-signal/competitor-view.tsx`](/home/ckwflash/repos/Signal/frontend/components/market-signal/competitor-view.tsx)

## Testing

Backend tests currently focus on:

- product viability request parsing and response shaping
- TinyFish polling/integration behavior

Run them with:

```bash
python backend/tests/test_product_viability_route.py
python backend/tests/test_product_viability_service.py
python backend/tests/test_tinyfish_service.py
```

Frontend sanity check:

```bash
cd frontend
./node_modules/.bin/tsc --noEmit
```

If you want a production-style frontend build:

```bash
cd frontend
npm run build
npm run start
```

## Repo Layout

```text
.
├── backend
│   ├── app.py
│   ├── config/sources.json
│   ├── data/
│   │   ├── snapshots/
│   │   └── source_runs/
│   ├── market_monitor_api/
│   │   ├── config.py
│   │   ├── routes/
│   │   └── services/
│   ├── scripts/
│   └── tests/
├── frontend
│   ├── app/
│   ├── components/
│   ├── lib/
│   └── package.json
└── .env.example
```

## Troubleshooting

### Latest tab is empty

Check:

- `backend/data/snapshots` actually contains snapshot JSON files
- TinyFish refresh completed successfully
- the backend category status is not returning `missing_snapshots`

Remember: source-run JSON files alone are not enough to populate the latest wire.

### Product viability returns a research failure

Check:

- `TINYFISH_API_KEY` is set
- TinyFish can reach the target sites
- you are using `multipart/form-data`
- long-running TinyFish jobs have enough timeout budget

### Competitor intelligence fails immediately

Check:

- both `TINYFISH_API_KEY` and `OPENAI_API_KEY` are configured
- `OPENAI_MODEL` is set
- the submitted company URL is public and valid

## Notes

- This repo currently targets local development and hackathon-style iteration speed rather than hardened production deployment.
- The frontend proxies backend requests through Next route handlers instead of calling Flask directly from the browser.
- The checked-in source catalog is market-news focused today, but the service layer is already set up for commerce, growth, competitor, and product-viability workflows.
