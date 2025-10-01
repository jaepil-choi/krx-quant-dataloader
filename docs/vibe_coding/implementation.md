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

---

## Next milestone: DataLoader API (kqdl.apis)

Goal: Provide a high-level, ergonomic API that composes raw calls and returns tidy tabular outputs (explicit, opt-in transforms only). Keep the raw layer as-is.

Scope (initial endpoints):

- Daily quotes (single day) – wide or long format
- Individual history (date range with chunking) – long format sorted by trade date

Public surface (proposed):

- `DataLoader.get_daily_quotes(date: str, market: str = "ALL", *, tidy: str = "wide") -> Table`
- `DataLoader.get_individual_history(isin: str, start: str, end: str, *, adjusted: bool = False) -> Table`

Behavioral rules:

- Defaults reflect raw server behavior; transforms (e.g., adjusted price) are explicit and opt-in.
- Deterministic column naming; stable across releases. Document any derived fields.
- Sorting: deterministic; per endpoint (e.g., by `TRD_DD` for histories).

Dependencies:

- Depends only on `RawClient`; must not import transport/orchestration/adapter directly.

Acceptance criteria (tests):

- Unit tests (no network):
  - Correct parameterization of underlying raw calls (e.g., `adjStkPrc` set when `adjusted=True`).
  - Output schema (columns) matches the contract for both wide and long forms.
  - Sorting/order rules enforced.
  - No silent transforms when flags are False.
- Live smoke tests:
  - Small-scope calls for both endpoints return non-empty tables with expected essential columns.

Implementation notes:

- Keep output type minimal (list[dict] or optional pandas) based on optional dependency flag.
- Provide a thin conversion layer from rows to table, no value transforms by default.
- Reuse `AdapterRegistry` ids directly; DataLoader maps high-level parameters (e.g., market) to raw params (`mktId`) explicitly.

Test inventory to add:

- `tests/test_apis_dataloader_unit.py` – validate parameter mapping, schema shaping, sorting, and opt-in transforms.
- `tests/test_apis_dataloader_live_smoke.py` – live smoke for both APIs with minimal scope.

---

## Progress status (live)

- ConfigFacade: implemented, validated, env overrides supported; tests green (unit).
- AdapterRegistry: implemented with `EndpointSpec` and `ParamSpec`; normalizes `client_policy.chunking` and infers `date_params` from param roles; tests green (unit).
- Transport: implemented (requests-based), header merge, timeouts, retries; tests green (unit + live smoke).
- Orchestrator: implemented; chunking/extraction/merge ordering; tests green (unit).
- RawClient: implemented; required param validation and defaults; tests green (unit + live smoke).
- DataLoader (apis): implemented two initial APIs and factory; demo script prints outputs; suite green (live).

Open gaps against PRD/Architecture:

- Transport hardening (HTTP/2, impersonation, jittered backoff) and proper rate limiting
- Structured observability (logs/metrics) across Transport/Orchestrator
- Schema-first validation for endpoint registry (JSON Schema), versioned evolution
- Error taxonomy finalization and propagation (typed errors across layers)
- More endpoints and tidy shaping rules in `apis` (keeping transforms explicit)

---

## Next tasks (prioritized) with tests and acceptance criteria

1) Transport hardening (curl_cffi)
   - Implement `curl_cffi.requests` client option with config-driven `http_version`, `impersonate`, `verify_tls`.
   - Add jittered exponential backoff for retryable statuses.
   - Tests:
     - Unit: ensure selected options are passed; backoff callable invoked; no retry on 4xx.
     - Live smoke (kept tiny): confirm basic compatibility with KRX using HTTP/2 where available.
   - Acceptance: parity with current behavior + options honored; no regressions in suite.

2) Rate limiter (per-host)
   - Token-bucket limiter driven by `requests_per_second` per host; injected into Transport.
   - Tests:
     - Unit: under N calls, `acquire` invoked N times and enforces spacing (mock time).
     - Integration: concurrent calls limited to configured QPS (coarse assertion with timestamps).
   - Acceptance: limiter engaged without materially increasing test flakiness.

3) Observability baseline
   - Structured logging on Transport (endpoint id, status, latency, retries) and Orchestrator (chunk_count, order_by).
   - Tests: `caplog` assertions for key fields; sampling-friendly.
   - Acceptance: logs include actionable context; no PII; minimal overhead.

4) Endpoint registry schema validation (code-first)
   - Continue using Pydantic and explicit checks in `AdapterRegistry` (no separate JSON Schema yet).
   - Tests: invalid shapes (missing `bld`, bad `response.root_keys` type, malformed `client_policy.chunking`) yield precise diagnostics.
   - Acceptance: schema violations fail early with clear messages; existing YAML passes.

5) Error taxonomy finalization
   - Define `ConfigError`, `TransportError` (with status/attempts), `ExtractionError`, `ValidationError` for params; ensure propagation boundaries.
   - Tests: unit + integration asserting error types and messages at RawClient boundary; no swallowing.
   - Acceptance: consistent, typed errors across layers; documented in architecture.

6) APIs shaping and additional endpoint(s)
   - Add `search_listed(market: str = "ALL", search_text: str = "", type_no: int = 0)`.
   - Optional tidy helpers (wide/long) with deterministic columns (no transforms by default).
   - Tests: unit for parameter mapping and schema; live smoke (small scope) returning non-empty rows.
   - Acceptance: API remains thin over RawClient; no hidden transforms.

7) CLI entry point (optional)
   - Minimal CLI to call supported endpoints and print JSON/CSV.
   - Tests: smoke run via subprocess; validates output headers for CSV.
   - Acceptance: runnable without code; respects config path.

8) Documentation updates
   - Update `architecture.md`/`prd.md` references where behavior is now implemented; document logging, rate limiting, and error taxonomy.
   - Add README usage examples with `create_dataloader_from_yaml`.

---

## Daily snapshots ingestion and adjustment factor (MDCSTAT01602) – Refactored

Scope and rationale:

- Build an append-only, resume-safe daily ingestion pipeline for MDCSTAT01602-like endpoint (전종목등락률), labeling each row with `TRD_DD = D` where `strtDd=endDd=D`.
- Compute per-symbol daily adjustment factor post-hoc, after ingestion completes, across consecutive trading days. No on-the-fly adjustment during fetch.
- Separate preprocessing (type coercion, labeling) from shaping (pivot) and from adjustment (factor computation).

Module structure (refactored):

- `transforms/preprocessing.py`: `preprocess_change_rates_row(s)` – inject `TRD_DD`, coerce numeric strings (comma → int).
- `transforms/shaping.py`: `pivot_long_to_wide()` – structural pivot; no type coercion.
- `transforms/adjustment.py`: `compute_adj_factors_grouped()` – per-symbol LAG semantics; pure computation.
- `storage/protocols.py`: `SnapshotWriter` protocol (ABC) for dependency injection.
- `storage/writers.py`: `CSVSnapshotWriter` (UTF-8, no BOM, append-only), `SQLiteSnapshotWriter` (UPSERT on composite key).
- `pipelines/snapshots.py`:
  - `ingest_change_rates_day(raw_client, *, date, market, adjusted_flag, writer)` – fetch, preprocess, persist one day; returns row count.
  - `ingest_change_rates_range(raw_client, *, dates, market, adjusted_flag, writer)` – iterate days with per-day isolation (errors on one day do not halt subsequent days).
  - `compute_and_persist_adj_factors(snapshot_rows, writer)` – post-hoc batch job; computes factors and persists.

Algorithm (resume-safe ingestion, post-hoc adjustment):

1. For each requested date `D` (iterate one day at a time via `ingest_change_rates_day`):
   - Call the endpoint with `strtDd=endDd=D`, `mktId=ALL`, and explicit `adjStkPrc`.
   - If the response is a successful empty list, treat as a non-trading day and write nothing (return 0).
   - Preprocess rows (inject `TRD_DD=D`, coerce numeric-string fields like `BAS_PRC`, `TDD_CLSPRC`).
   - Persist preprocessed rows immediately via a writer (CSV/SQLite) before proceeding to the next day.
2. After the ingestion loop completes, compute per-symbol daily adjustment factors via `compute_and_persist_adj_factors`:
   - `adj_factor_{t-1→t}(s) = BAS_PRC_t(s) / TDD_CLSPRC_{t-1}(s)` using the previous trading day for that symbol (SQL LAG semantics).
   - First observation per symbol yields empty string (not `NULL` in Python, but serialized as empty).
   - Persist factor rows via writer.

Storage contract (relational-friendly, append-only):

- Table `change_rates_snapshot` with primary key `(TRD_DD, ISU_SRT_CD)` and essential fields including `BAS_PRC`, `TDD_CLSPRC` (integers) plus passthrough columns.
- Table `change_rates_adj_factor` with primary key `(TRD_DD, ISU_SRT_CD)` and `ADJ_FACTOR` (text/numeric).
- Holidays naturally produce no rows; no special handling required.

Exposure and layering:

- Ingestion is performed in pipelines and persists snapshots per day (append-only). The public API remains "as-is" by default and only returns transformed results when explicitly requested.
- Adjustment is a post job over stored snapshots (or the in-memory preprocessed set). Optional SQL/window-function re-computation can be added later for backfills and verification.
- Writer injected via dependency; test with fake writer (unit), real CSV/SQLite (integration).

Acceptance criteria:

- Injected `TRD_DD` present on all ingested rows; numeric fields correctly coerced.
- Factors computed only when previous trading day exists for a symbol; first observations yield empty string.
- Append-only semantics with composite uniqueness enforced; holidays cause no writes.
- Per-day isolation: errors on one day do not halt subsequent days.
- Writers abstract backend; pipelines decoupled from CSV vs SQLite choice.

---

## Testing strategy: Live smoke tests first

Philosophy:

- **Live smoke tests as primary validation:** Real KRX API calls validate actual data schemas, preprocessing, and pipeline behavior. Print sample outputs to terminal for visual inspection and debugging.
- **Unit tests as secondary:** Cover edge cases and pure logic after live tests confirm real-world behavior.
- **Integration tests:** Validate CSV/SQLite writers with real I/O using temporary files.

Test plan for refactored modules:

### 1. Preprocessing live smoke (`tests/test_transforms_preprocessing_live_smoke.py`)

- Fetch real KRX data for recent trading day
- Preprocess and print original vs preprocessed rows
- Validate: TRD_DD injection, numeric coercion, passthrough fields
- Visual inspection: confirm schema matches expectations

### 2. Adjustment live smoke (`tests/test_transforms_adjustment_live_smoke.py`)

- Fetch 3-4 consecutive trading days (include holiday)
- Preprocess all rows
- Compute adjustment factors
- Print sample factors per symbol for inspection
- Validate: first day empty, subsequent days computed, Decimal precision

### 3. Shaping live smoke (`tests/test_transforms_shaping_live_smoke.py`)

- Fetch 2-3 trading days
- Pivot long → wide (TRD_DD as index, ISU_SRT_CD as columns)
- Print sample wide rows
- Validate: correct structure, missing values as None

### 4. Storage writers live smoke (`tests/test_storage_writers_live_smoke.py`)

- Fetch real data, preprocess
- Write to CSV and SQLite via concrete writers
- Read back and compare
- Print samples for inspection
- Validate: UTF-8 (no BOM), UPSERT behavior, append-only

### 5. Pipelines live smoke (`tests/test_pipelines_snapshots_live_smoke.py`)

**End-to-end validation:**
- `test_ingest_change_rates_day_live()`: Single day ingestion with real writer
- `test_ingest_change_rates_range_live()`: Multi-day including holiday, verify per-day isolation
- `test_compute_and_persist_adj_factors_live()`: Post-hoc factor computation

**Sample output pattern:**
```python
def test_ingest_change_rates_range_live(raw_client, tmp_path):
    # Setup writer
    writer = CSVSnapshotWriter(...)
    
    # Ingest with holiday
    dates = ["20250814", "20250815", "20250818"]
    counts = ingest_change_rates_range(raw_client, dates=dates, writer=writer)
    
    print(f"\n=== INGESTION RESULTS ===")
    for date, count in counts.items():
        print(f"{date}: {count} rows")
    
    # Compute factors post-hoc
    snapshots = read_snapshots_from_csv(...)
    factor_count = compute_and_persist_adj_factors(snapshots, writer)
    
    print(f"\n=== FACTORS COMPUTED: {factor_count} ===")
    
    assert counts["20250815"] == 0  # Holiday
    assert counts["20250814"] > 0
    assert counts["20250818"] > 0
```

Test execution:

```bash
# Run live smoke tests with output visible
pytest tests/test_pipelines_snapshots_live_smoke.py -v -s --capture=no

# Run all live smoke tests
pytest tests/test_*_live_smoke.py -v -s
```

Benefits:

- Real data validation ensures preprocessing works with actual KRX responses
- Print statements reveal actual field names, data types, and values
- Holiday handling validated with real empty responses
- Full pipeline confidence with end-to-end real data flow
- Visual inspection aids debugging and documentation
