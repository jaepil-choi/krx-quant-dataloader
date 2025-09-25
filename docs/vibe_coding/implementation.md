# Implementation Plan: KRX Quant Data Loader (KQDL)

This document lays out a TDD-first roadmap: what to build, in what order, how modules interact, and what to test at each step. It stays high-level and avoids binding implementation details until they are needed in code.

## Guiding constraints (from PRD & Architecture)

- As‑is contract on the raw path: no silent transforms or source substitution.
- Config-driven adapter: no hard-coded endpoint specs in code.
- Two public surfaces: `kqdl.client` (raw) and `kqdl.apis` (tidy outputs via explicit, opt-in transforms).
- Clean layering: transport (IO), adapter (config), orchestration (chunk→request→extract→merge), raw client (thin façade), apis (user-facing composition).

---

## Incremental roadmap (TDD milestones)

1) ConfigFacade (Pydantic settings)

- Purpose: Single source of truth for runtime configuration (paths, timeouts, retry policy, rate limits, base URLs, headers, curl options).
- Inputs/outputs: Loads from env/YAML; exposes validated, read-only settings to other modules.
- Dependencies: none.
- Primary tests:
  - Loads defaults; allows env overrides (e.g., base_url, timeouts, curl http_version/impersonate).
  - Validates types and ranges; raises on invalid config.
  - Idempotent: multiple loads return equivalent facades.

2) AdapterRegistry (config → endpoint specs)

- Purpose: Load and validate endpoint definitions from YAML; expose immutable `EndpointSpec` objects.
- Inputs/outputs: YAML registry → `EndpointSpec` (method, path, bld?, params (required/optional/enums), date params, response root keys (ordered), merge policy, chunking hints).
- Dependencies: ConfigFacade for paths and schema.
- Primary tests:
  - Valid spec loads; invalid spec fails with diagnostic.
  - Root key preference honored (`output`, `OutBlock_1`, `block1`).
  - Date params and chunking hints exposed as declared.
  - Specs are immutable value objects.

3) Transport (curl_cffi HTTP session)

- Purpose: Perform HTTP requests with timeouts, retries, rate limiting, and standard headers using `curl_cffi.requests` for robustness (HTTP/2, TLS, impersonation).
- Inputs/outputs: `Request` → `Response` (status, headers, body).
- Dependencies: ConfigFacade (timeouts, retries, headers, curl options: `http_version`, `impersonate`, `verify`).
- Primary tests (can use real IO to KRX in off-hours; also keep a local stub for unit speed):
  - Timeouts enforced; bounded retries for retryable statuses; no retry on non-retryable.
  - HTTPS enforced; headers applied; optional `impersonate` handled (e.g., chrome101, safari15).
  - Optional HTTP/2 enabled when configured; rate limiting observed under concurrent calls.

4) Orchestrator (mechanical workflow)

- Purpose: Given `EndpointSpec` + params → build requests, chunk ranges, call Transport, extract rows by ordered root keys, merge chunks per policy. No transforms.
- Inputs/outputs: (`spec`, `params`) → rows (as-is).
- Dependencies: AdapterRegistry, Transport.
- Primary tests (Transport stubbed for loop logic; selective real IO for one endpoint):
  - Chunking: splits long ranges per `spec`; merges chunks correctly (append/deduplicate/order-by).
  - Extraction: honors ordered root keys and fails clearly if none found.
  - As‑is: does not alter values; error propagation is typed and informative.

5) RawClient (thin façade)

- Purpose: Public raw interface; resolves `endpointId`, validates/normalizes params, delegates to Orchestrator.
- Inputs/outputs: endpointId + full params → rows (as-is).
- Dependencies: AdapterRegistry, Orchestrator.
- Primary tests:
  - EndpointId routing, param validation errors surfaced.
  - Errors from Orchestrator/Transport preserved.

6) APIs (DataLoader)

- Purpose: Public, ergonomic interface; composes raw calls; returns tidy outputs. Any transforms are explicit and opt-in.
- Inputs/outputs: high-level query → tidy DataFrame/Table (wide/long), explicitly shaped.
- Dependencies: RawClient.
- Primary tests:
  - Output format (wide/long) controlled explicitly (parameter or heuristic + explicit override).
  - Transforms (e.g., adjusted close) occur only when flags are set; defaults reflect raw.
  - No hidden fallbacks to alternate sources.

7) Transforms (optional)

- Purpose: Pure utilities for explicit post-processing (e.g., adjustments, resampling); documented formulas and inputs.
- Dependencies: none or optional numeric libs.
- Primary tests:
  - Correctness against reference math; stable under edge cases (splits/dividends/gaps).

8) Observability (logging & metrics)

- Purpose: Structured logging and counters/latency histograms; minimal surface in code via a small façade.
- Dependencies: ConfigFacade (log level), standard logging/metrics lib.
- Primary tests:
  - Logging contexts include endpoint id, chunk count, retries; metrics increment as expected.

---

## Suggested starting point (TDD step 1)

Module: ConfigFacade (Pydantic settings)

- Rationale: It is foundational, pure, and enables realistic tests for all subsequent modules by supplying consistent settings (paths, timeouts, base URLs, curl options).
- Acceptance criteria (tests):
  1. Loads default values and custom YAML path; env overrides take precedence.
  2. Validates types and ranges (e.g., timeouts > 0, retry counts within allowed bounds; `http_version` in {"1.1","2"}).
  3. Exposes read-only properties; attempting mutation raises.
  4. Provides typed subsets: Transport (timeouts/retries/headers/http_version/impersonate), Adapter (registry path/schema).

- Proposed interface (non-binding sketch):
  - `class ConfigFacade:`
    - `@classmethod load(cls, *, config_path: str | None = None) -> ConfigFacade`
    - Properties (examples): `base_url`, `default_headers`, `request_timeout`, `connect_timeout`, `retry_policy`, `rate_limit`, `registry_path`, `registry_schema_path`, `http_version`, `impersonate`, `verify_tls`.

---

## Test scaffolding and fixtures

- Use real IO sparingly but early for Transport sanity (e.g., `stock.daily_quotes` on a recent date, off-hours, with low rate limits). Keep unit speed with stubs otherwise.
- Provide temporary YAML fixtures for AdapterRegistry; centralize valid/invalid samples.

---

## Progressive development checkpoints

- Milestone A: ConfigFacade green; AdapterRegistry loads and validates a minimal registry (1–2 endpoints) → smoke test for RawClient calling Orchestrator with a stub Transport.
- Milestone B: Transport via `curl_cffi.requests` with timeouts/retries/rate limits; confirm HTTP/2/impersonate options from config; Orchestrator end-to-end tests on one live endpoint.
- Milestone C: RawClient public surface stabilized; error taxonomy verified.
- Milestone D: First `kqdl.apis` method returns tidy output with explicit flags; no silent transforms.
- Milestone E: Observability baseline added; contract tests for a small set of endpoints.


---

## Live test plan and rationale (kept up to date)

Purpose: This section is the authoritative, evolving plan of what we test and why. It reflects the current TDD milestone focus and our risk priorities from PRD/Architecture.

Guiding principles (tests):

- As‑is behavior: never substitute sources or generate synthetic data on failure; errors must surface clearly.
- Hermetic and fast by default: unit tests use stubs and fixtures; live tests are opt‑in via markers.
- DRY and explicit: shared wiring in fixtures; no duplicated setup across tests.
- Layer boundaries: test each layer in isolation first; add thin, representative integration checks later.

Test layout (paths and ownership):

- `tests/fixtures/test_config.yaml` – canonical test config moved under tests (not docs) to keep tests hermetic and independent of documentation layout.
- `tests/conftest.py` – shared fixtures for config_facade, adapter_registry, fake_transport, orchestrator, raw_client; and utility helpers.
- Markers (strict): `unit`, `integration`, `live`, `slow` (documented here; registered in pytest ini options).

Milestone A focus: ConfigFacade (per “Suggested starting point” above)

- What we test (and why):
  - Load defaults from `tests/fixtures/test_config.yaml` (ensures reproducible baseline independent of docs).
  - Environment overrides precedence (prevents hidden coupling and enables tunable CI settings).
  - Validation of hygiene constraints (HTTPS‑only, positive timeouts, bounded retries, allowed HTTP versions); aligns with PRD transport hygiene.
  - Read‑only/immutability of exposed settings; prevents runtime drift.
  - Typed subviews for transport/adapter; reduces configuration leakage across layers.

Test inventory (initial files):

- `tests/test_config_facade.py` (unit)
  - Loads defaults from fixture YAML; env overrides; validation errors (HTTPS, timeouts, retries, http_version); immutability; typed views.

- `tests/test_adapter_registry.py` (unit)
  - Loads endpoint specs from the same YAML; root key precedence order; presence/absence of date param mapping; immutable `EndpointSpec`; diagnostic failure on malformed specs.

- `tests/test_orchestrator_chunk_extract_merge.py` (unit)
  - Chunking of ranges by spec; extraction with ordered roots; merging across chunks with mixed root keys; `order_by` respected; raises ExtractionError when no known roots – no fallback.

- `tests/test_raw_client_validation.py` (unit)
  - Required param validation with clear errors; unknown endpoint id; no hidden defaults beyond registry declarations.

- `tests/test_transport_policy_unit.py` (unit)
  - HTTPS‑only enforcement; headers application; retry policy applied only to configured statuses; rate‑limit hook invoked. Pure unit with a stub transport; no network.

- `tests/test_no_fallback_policy.py` (unit)
  - On extraction failure or exhausted retryable transport errors, the error surfaces; no alternate source, no synthetic data.

- `tests/test_observability_contract.py` (unit)
  - Logs include endpoint id, attempt count, and chunk_count at debug level; ensures structured, actionable context.

- `tests/test_integration_live_krx.py` (integration, live; always‑on minimal smoke)
  - Single minimal smoke path (e.g., `stock.daily_quotes` for one day); validates host/path/headers and schema against the real API. Runs by default and is kept tiny to avoid rate limits. If needed, deselect via `-m "not live"`.

Fixtures and stubs (why they exist):

- `test_config_path` fixture centralizes the canonical YAML path, decoupling tests from repo doc structure and enabling temporary, mutated copies for negative cases.
- `fake_transport` fixture provides deterministic responses (queued JSON/status) to validate retries, chunking, and extraction without flakiness or external rate limits.
- Composition fixtures (`config_facade`, `adapter_registry`, `orchestrator`, `raw_client`) ensure consistent wiring across tests, prevent copy‑paste setup, and keep tests focused on behavior, not bootstrapping.

Markers and execution hints:

- With `--strict-markers`, we register `unit`, `integration`, `live`, `slow`. All tests (including live smoke) run by default. Run fast suite: `pytest -m unit`. To skip live smoke: `pytest -m "not live"`.

Scope control and out‑of‑scope for Milestone A:

- No real network in unit tests; the only network touch is the opt‑in live smoke.
- No DataFrame shaping or transforms in raw path tests; transforms belong to later milestones.
- No silent source substitution or synthetic fallbacks anywhere; failures must be explicit.

Transition to subsequent milestones:

- Milestone B adds concrete transport behavior (timeouts/retries/rate‑limits via curl_cffi), enabling a small set of transport integration tests in addition to the existing unit guarantees.
- Milestone C stabilizes the RawClient surface and error taxonomy; tests ensure propagation semantics from transport → orchestrator → raw client remain unchanged.