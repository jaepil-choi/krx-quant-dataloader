# PRD: KRX Quant Data Loader (KQDL)

## Overview

PyKRX wraps KRX internal endpoints but has architectural problems that impede trust and maintainability:

- Silent data adjustments (e.g., adjusted close, implicit option flips) and occasional fallback to alternative sources (e.g., Naver) without explicit user opt-in.
- Hard-coded endpoint specs and divergent response key handling across wrappers (`output`, `OutBlock_1`, `block1`).
- Transport fragility (no timeouts/retries, HTTP instead of HTTPS), coupled with bespoke range chunking logic embedded in the IO layer.

KRX Quant DataLoader (KQDL) is a superior alternative that:
1. **Wraps KRX API directly** (not pykrx) with 1:1 endpoint mapping to the KRX website
2. **Config-driven** endpoint specs in YAML, not hardcoded in application code
3. **Two-layer architecture:**
   - **Raw layer:** As-is API client for power users (list[dict] output, explicit endpoint calls)
   - **High-level layer:** Quant-friendly DataLoader API (field-based queries, wide-format output, universe + date range)
4. **As-is by default:** No silent transforms; explicit opt-in for adjustments
5. **Resume-safe ingestion:** Per-day persistence with protocol-driven storage
6. **Transport hygiene:** HTTPS, timeouts, retries, rate limiting

This replaces the legacy `kor-quant-dataloader` (pykrx wrapper) with a modern, transparent, and maintainable solution focused exclusively on KRX data.

## Goals

- Provide a clean, reliable, config-driven wrapper for KRX endpoints.
- Return server data “as-is” by default. Any transformation must be explicit and opt-in.
- Externalize endpoint specifications (request/response schema, throttling, range chunking, root keys) into versioned YAML.
- Keep code free of hard-coded endpoint specs; code only implements generic orchestration.
- Improve transport reliability (HTTPS, timeouts, retries, session pooling, rate limiting).
- Normalize response extraction and date-range chunking without assuming specific JSON keys.
- Support corporate-action-safe analytics by computing daily adjustment factors from MDCSTAT01602 snapshots without global back-adjustments.

## Non-Goals

- No implicit use of alternative data sources (e.g., Naver) as a fallback.
- No implicit adjusted pricing; adjusted/unadjusted is controlled solely by explicit parameters.
- No attempt to expose every pykrx API 1:1 initially; prioritize core endpoints and expand via configuration.

## Principles

- As-is data: default responses are exactly what KRX returns, no silent transforms.
- Explicit transforms: any derived calculations (e.g., adjusted close) are provided by separate, opt-in utilities or flags that are explicit and traceable.
- Configuration over code: endpoint behavior belongs in YAML; code is generic and reusable.
- Deterministic behavior: no hidden fallbacks, no heuristics. If an endpoint fails, return a meaningful error.
- Observability: structured logging, request/response correlation IDs, and metrics around retries/rate-limits.

## Stakeholders and Users

- Quant researchers requiring field-based queries (종가, PER, PBR) with wide-format output (date × symbol grid)
- Data engineers building reproducible, audit-friendly pipelines with resume-safe ingestion
- Power users needing direct 1:1 KRX API access with as-is responses
- Library maintainers who need minimal surface to adapt when KRX specs change

## User Stories

**High-level DataLoader API (quant researchers):**
- As a quant, I want to initialize a DataLoader with a fixed date range: `loader = DataLoader(db_path, start_date='2018-01-01', end_date='2018-12-31')`. On initialization, the loader ensures all required data exists in the local database:
  - **Stage 1 - Snapshot ingestion**: If snapshots are missing for the date range, automatically fetch from KRX, preprocess, and persist to Parquet DB
  - **Stage 2 - Adjustment pipeline**: Compute adjustment factors from snapshots → build ephemeral cumulative adjustment cache for the date range
  - **Stage 3 - Universe pipeline**: Compute liquidity ranks from snapshots → build pre-computed universe tables (univ100, univ200, etc.)
- As a quant, I want queries to be fast because data is pre-fetched and pre-processed during initialization, not on every query.
- As a quant, I want to query `loader.get_data('close', symbols=['005930', '000660'])` and receive a wide-format DataFrame with dates as index and symbols as columns for the full loader date range.
- As a quant, I want to use pre-computed liquidity-based universes via `universe='univ100'` (or `'univ200'`/`'univ500'`/`'univ1000'`/`'univ2000'`) that select the most liquid stocks on each trading day based on trading value (거래대금). Universe filtering is applied via simple JOIN/masking of the queried data against the universe table.
- As a quant, I want survivorship bias-free data: universes are cross-sectional per day, so delisted stocks that were liquid on historical dates are included.
- As a quant, I want adjusted prices by default (`adjusted=True` is default); raw prices available via explicit `adjusted=False`. Adjustment factors are **range-dependent**: cumulative adjustments computed based on the loader's `[start_date, end_date]` window.
- As a quant, I want to query subsets of the loader's date range (e.g., Q1 only) without re-initializing: `loader.get_data('close', query_start='2018-01-01', query_end='2018-03-31')`.
- As a quant, I understand that changing the date range requires a new loader instance because cumulative adjustments are **view-dependent**: historical adjusted prices differ based on what future corporate actions are visible in the query window.
- As a quant, I want holiday handling (empty/forward-fill) to be explicit, not automatic.

**Raw client API (power users / backend services):**
- As a power user, I need direct access to `raw_client.call('stock.daily_quotes', params={...})` for custom workflows.
- As a data engineer, I need resume-safe per-day ingestion with writer injection for incremental ETL pipelines.
- As a backend service, I need requests to be retried on transient errors with bounded backoff.

**Maintainability:**
- As a maintainer, I need to update endpoint parameters/keys by editing YAML, not application code.
- As a maintainer, I need field-to-endpoint mappings in config, not hardcoded in DataLoader.

## Functional Requirements

1. Raw passthrough by default
   - Default responses equal KRX payload (minus trivial transport envelope like HTTP headers).
   - No implicit adjusted prices; parameters like `adjStkPrc` must be passed explicitly.
   - No implicit source substitution (e.g., Naver). Alternative sources may exist as separate providers but never as an invisible fallback.

2. Config-driven endpoint registry
   - Endpoints are defined in YAML (method, host, path, `bld` if applicable, parameters, date fields, chunking hints, response root keys, merge policy).
   - Config is versioned and validated on load; the codebase consumes the registry and remains free of hard-coded endpoint specs.

3. Unified response extraction
   - Extraction uses an ordered list of possible root keys declared in config.
   - If extraction fails, raise a descriptive error; do not fallback to alternate sources.

4. Date-range chunking
   - When start/end parameters are present, chunking follows config-driven limits and merging rules (append, deduplicate, order).

5. Transport hygiene
   - HTTPS-only, session pooling, timeouts, bounded retries, and rate limiting; parameters are observable via structured logs with redaction.

6. Public API surface (two-layer)
   - Raw Interface (kqdl.client): Pure pass-through that requires full endpoint parameters and returns data as-is (list[dict]).
   - High-level Interface (kqdl.apis.DataLoader): Quant-friendly field-based API with wide-format output.
     - **Range-locked initialization with automatic pipeline**: `DataLoader(db_path, start_date, end_date)` triggers:
       1. **Stage 1 - Snapshot ingestion**: Check if snapshots exist for date range; if missing, fetch from KRX → preprocess → persist
       2. **Stage 2 - Adjustment pipeline**: Compute adjustment factors from snapshots → build ephemeral cumulative adjustment cache
       3. **Stage 3 - Universe pipeline**: Compute liquidity ranks from snapshots → build pre-computed universe tables
     - **Query execution** (simple filtering + pivoting):
       1. Query field data from snapshots table (e.g., 'close' → 'TDD_CLSPRC' column)
       2. If universe specified, query universe table for membership (boolean filter)
       3. JOIN/mask: filter queried data by universe on (date, symbol)
       4. Apply cumulative adjustments if `adjusted=True` (multiply by cached multipliers)
       5. Pivot to wide format: index=dates, columns=symbols
     - Field-based queries: `get_data(field, symbols, query_start, query_end, adjusted, universe)`
     - Wide format: dates as index, symbols as columns (Pandas DataFrame)
     - Universe support: 
       - Explicit stock list: `symbols=['005930', '000660']` (simple filtering, may have survivorship bias)
       - Pre-computed liquidity-based: `universe='univ100'/'univ200'/'univ500'/'univ1000'/'univ2000'` (survivorship bias-free)
     - Universe filtering: Applied via JOIN/masking of queried data against universe table (not a separate service layer)
     - **Adjusted prices by default**: `adjusted=True` is default; raw prices via explicit `adjusted=False`
     - **Range-dependent adjustments**: Cumulative adjustment factors computed based on loader's `[start_date, end_date]` window; changing date range requires new loader instance
     - **Ephemeral adjustment cache**: Cumulative adjustments cached in `data/temp/` on initialization; auto-regenerated per session; not persisted long-term
     - Field-to-column mapping: configured in YAML (`config/fields.yaml`), not hardcoded

7. Pre-computed liquidity universes
   - Daily cross-sectional ranking by trading value (거래대금 / ACC_TRDVAL from daily snapshots)
   - Five universe tiers: `univ100` (top 100), `univ200` (top 200), `univ500` (top 500), `univ1000` (top 1000), `univ2000` (top 2000)
   - Universe membership is date-specific: stock may be in univ100 on day T but not on day T+1
   - Includes delisted stocks that were liquid on historical dates (no survivorship bias)
   - Stored in `liquidity_ranks` Parquet table with columns: TRD_DD, ISU_SRT_CD, xs_liquidity_rank, ACC_TRDVAL

8. Error handling
   - Clear separation of validation/config errors vs transport/server errors vs extraction failures.

9. Adjustment factor computation (daily snapshots)
   - For each date D, call MDCSTAT01602 (전종목등락률) as a daily snapshot with strtDd=endDd=D and tag rows client-side with TRD_DD=D.
   - Define per-symbol adjustment factor for transition (t−1 → t): adj_factor_{t−1→t}(s) = BAS_PRC_t(s) / TDD_CLSPRC_{t−1}(s).
   - adj_factor is typically 1; deviations reflect corporate actions. Persist snapshots plus factors in an append-only, relational-friendly schema.

## Non-Functional Requirements

- Reliability: High success rate on retryable failures; graceful degradation with explicit errors.
- Performance: Efficient via connection pooling and batch/chunk orchestration.
- Security: HTTPS-only, safe defaults, no secrets in source.
- Maintainability: Endpoint updates are primarily config changes; minimal code edits.
- Testability: Live smoke tests with real KRX API calls validate actual data schemas and pipeline behavior; print sample outputs for visual inspection and debugging. Unit tests cover edge cases and pure logic. Integration tests validate storage backends with real I/O.

## Design note: daily snapshots vs back-adjustment

- MDCSTAT01602 returns cross-sectional daily snapshots that include BAS_PRC (yesterday's adjusted close) and TDD_CLSPRC (today's close). The payload omits TRD_DD; the client attaches the date used in the request.
- Computing per-day adjustment factors from snapshots enables relational storage and incremental updates without rebuilding historical back-adjusted series on each fetch, aligning with "as‑is" defaults and explicit transforms.

## Design note: range-dependent cumulative adjustments

**Problem**: Adjustment factors are "event markers" (e.g., `0.02` for a 50:1 stock split), but applying them requires cumulative multiplication across the time series. Critically, cumulative adjustments depend on what future corporate actions are visible in the query window.

**Example** (Samsung 50:1 split on 2018-05-04):

```
Query Window 1: [2018-01-01, 2018-03-31] (before split)
→ No future splits visible
→ Adjusted close on 2018-01-01: ₩2,520,000 (no adjustment)

Query Window 2: [2018-01-01, 2018-12-31] (includes split)
→ May split visible in window
→ Adjusted close on 2018-01-01: ₩50,400 (2,520,000 × 0.02)
```

**Implication**: The same historical date has **different adjusted prices** depending on the query date range. This is correct behavior: adjusted prices normalize all history to the most recent scale visible in the window.

**Solution**:
1. **Persistent storage**: Store raw prices + daily adjustment factors (event markers) in Parquet DB
2. **Ephemeral cache**: On `DataLoader` initialization, compute cumulative adjustment multipliers for the `[start_date, end_date]` window and cache in `data/temp/`
3. **Range-locked queries**: Queries within the loader's range use the cached multipliers; changing the range requires a new loader instance (cache rebuild)
4. **Forward adjustment**: Cumulative multipliers computed as product of all future factors: `cum_adj[t] = factor[t] × factor[t+1] × ... × factor[end]`

**Storage pattern**:
- `data/krx_db/adj_factors/`: Persistent event markers (daily factors, sparse)
- `data/temp/cumulative_adjustments/`: Ephemeral cache (cumulative multipliers, rebuilt per session)

**Trade-offs**:
- ✅ Correct: Historical adjusted prices always normalized to query window's latest scale
- ✅ Fast: Cache rebuilt once per loader initialization (~1-2 seconds), then queries are instant
- ✅ Simple: Users never think about adjustment mechanics; just set date range once
- ⚠️ Range-locked: Changing date range requires new loader (explicit, intentional design)

## Architecture & Design

Layered design (two-tier API):

**Tier 1: Raw Layer (1:1 KRX API wrapper)**
- Transport Layer: Session management, retries, timeouts, HTTPS, and rate limiting.
- Adapter (Config-driven): Loads YAML, validates schemas, supplies endpoint metadata.
- Orchestrator: Chunking, extraction, merging per endpoint policy.
- Raw Interface (kqdl.client): Pure, as-is client requiring full endpoint parameters; returns list[dict].

**Tier 2: High-level Layer (Quant-friendly field-based API)**
- DataLoader API: Stateful, range-locked loader that orchestrates the full data pipeline on initialization
  - **On initialization (3-stage pipeline)**:
    - Stage 1: Snapshot ingestion (fetch → preprocess → persist)
    - Stage 2: Adjustment pipeline (compute factors → build ephemeral cumulative adjustment cache)
    - Stage 3: Universe pipeline (compute liquidity ranks → build pre-computed universe tables)
  - **On query**: Simple operations (query DB → filter by universe via JOIN/mask → apply adjustments → pivot to wide format)
- Field Mapper: Maps user-facing field names (close, volume, PER, etc.) → (table, column) via config (`config/fields.yaml`)
  - **Critical role**: Roadmap for data location - when user asks for field X, tells DataLoader which table's which column to query
  - Examples: `'close'` → `('snapshots', 'TDD_CLSPRC')`, `'liquidity_rank'` → `('liquidity_ranks', 'xs_liquidity_rank')`
- Wide Format Transformer: Pivots long-format (date, symbol, value) → wide format (date × symbol grid)
- Explicit Transforms: Cumulative adjustments (opt-in via `adjusted` flag)

**Separation of concerns:**
- Raw layer: "What KRX returns, we return" (as-is guarantee, endpoint-based)
- High-level layer: "What quants need" (field-based, wide-format, survivorship bias-free)
- **No intermediate service layer**: Universe filtering is simple DataFrame masking, not a separate service

Sequence (date-ranged call):

1) Raw interface receives a request with explicit parameters.
2) Adapter returns endpoint spec (method, path, bld, date params, chunk config, response roots, merge strategy).
3) Orchestrator iterates chunks (if applicable), sends requests via Transport, extracts rows using declared root keys, and merges according to policy.
4) High-level interface may reshape/transform data explicitly and return a tidy output.

## Acceptance Criteria

- Raw interface returns exactly what the server provides (post-merge if chunked), with no silent adjustments or source substitutions.
- High-level interface requires explicit flags for any transformation (e.g., adjusted prices); defaults reflect raw server behavior.
- Endpoint or policy changes are primarily handled via YAML updates; public behavior remains stable unless declared otherwise.
- Transport settings (timeouts, retries, rate limits) are configurable and observable; failures yield actionable errors.

## Risks & Mitigations

- Drift between raw and high-level layers: mitigated by contract tests and docs clarifying guarantees of each layer.
- Hidden transforms: mitigated by “strict mode” tests and requiring explicit flags in high-level APIs.
- KRX response shape changes: mitigated by config-first design and ordered root keys.

## Glossary

- As-is data: The raw server payload without client-side transformation.
- Adapter: Loader/validator that exposes endpoint metadata from YAML.
- Root key: JSON field containing row arrays (e.g., `output`, `OutBlock_1`, `block1`).
- Chunking: Splitting a date range into sub-ranges to respect server constraints.
