# PRD: KRX Quant Data Loader (KQDL)

## Overview

PyKRX wraps KRX internal endpoints but has architectural problems that impede trust and maintainability:

- Silent data adjustments (e.g., adjusted close, implicit option flips) and occasional fallback to alternative sources (e.g., Naver) without explicit user opt-in.
- Hard-coded endpoint specs and divergent response key handling across wrappers (`output`, `OutBlock_1`, `block1`).
- Transport fragility (no timeouts/retries, HTTP instead of HTTPS), coupled with bespoke range chunking logic embedded in the IO layer.

KQDL rectifies these issues while leveraging the proven structure (website layer and endpoint wrappers) by introducing a config-driven adapter and transport hygiene. The number one principle: return the API data as-is by default, with no silent preprocessing or source substitution.

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

- Quant researchers and data engineers requiring reproducible, audit-friendly market data retrieval.
- Backend services ingesting KRX data for analytics and archival.
- Library maintainers who need a minimal surface to adapt when KRX specs change.

## User Stories

- As a quant, I need to fetch raw daily quotes for a date and market without any hidden adjustments.
- As a data engineer, I need to pull multi-year time series using automatic chunking that respects server limits, with correct row merging.
- As a maintainer, I need to update endpoint parameters/keys by editing a YAML file, not application code.
- As a platform owner, I need requests to be retried on transient errors with bounded backoff and to observe failure rates.

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
   - Raw Interface (kqdl.client): Pure pass-through that requires full endpoint parameters and returns data as-is.
   - High-level Interface (kqdl.apis): DataLoader-style APIs that compose adapter endpoints and return tidy, well-formed data; any transforms are explicit and opt-in.

7. Error handling
   - Clear separation of validation/config errors vs transport/server errors vs extraction failures.

8. Adjustment factor computation (daily snapshots)
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

- MDCSTAT01602 returns cross-sectional daily snapshots that include BAS_PRC (yesterday’s adjusted close) and TDD_CLSPRC (today’s close). The payload omits TRD_DD; the client attaches the date used in the request.
- Computing per-day adjustment factors from snapshots enables relational storage and incremental updates without rebuilding historical back-adjusted series on each fetch, aligning with “as‑is” defaults and explicit transforms.

## Architecture & Design

Layered design:

- Transport Layer: Session management, retries, timeouts, HTTPS, and rate limiting.
- Adapter (Config-driven): Loads YAML, validates schemas, supplies endpoint metadata.
- Raw Interface (kqdl.client): Pure, as-is client requiring full endpoint parameters; no transforms or fallbacks.
- High-level Interface (kqdl.apis): DataLoader-style APIs that compose adapter endpoints and return tidy, well-formed data with explicit, opt-in transforms.

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
