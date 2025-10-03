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

### ✅ Completed Core Infrastructure

- **ConfigFacade**: Implemented with Pydantic validation, env overrides supported; tests green (unit).
- **AdapterRegistry**: Implemented with `EndpointSpec` and `ParamSpec`; normalizes `client_policy.chunking` and infers `date_params` from param roles; tests green (unit).
- **Transport**: Implemented (requests-based), header merge, timeouts, retries, **config-driven rate limiting**; tests green (unit + live smoke).
- **RateLimiter**: **NEW** - Token bucket rate limiter with per-host throttling; thread-safe; auto-configured from config; tests green (unit, 6/6 passing).
- **Orchestrator**: Implemented; chunking/extraction/merge ordering; tests green (unit).
- **RawClient**: Implemented; required param validation and defaults; tests green (unit + live smoke).
- **Production Config**: Created `config/config.yaml` with safe defaults (1 req/sec rate limit); build script auto-detects prod vs test config.

### ✅ Completed Storage & Pipelines

- **Parquet Storage**: Hive-partitioned by `TRD_DD`; sorted writes by `ISU_SRT_CD` for row-group pruning; Zstd compression; schema definitions.
- **ParquetSnapshotWriter**: Three tables (snapshots, adj_factors, liquidity_ranks); idempotent writes; tests green (live smoke).
- **Pipelines**: Resume-safe daily ingestion (`ingest_change_rates_range`); post-hoc adjustment factor computation; tests green (live smoke).
- **Transforms**: Preprocessing (TRD_DD injection, numeric coercion including OPNPRC/HGPRC/LWPRC); adjustment (per-symbol LAG semantics); shaping (pivot_long_to_wide).
- **Production Scripts**: `build_db.py` (market-wide DB builder with rate limiting), `inspect_db.py` (DB inspection and validation tool).

### 🔄 In Progress / Pending

- **Storage Query Layer** (`storage/query.py`): PyArrow/DuckDB query helpers for partition pruning and filtering.
- **Universe Builder** (`pipelines/universe_builder.py`): Liquidity rank computation (top 100, 500, 1000, 2000 by `ACC_TRDVAL`).
- **Layer 2 Services**: FieldMapper, UniverseService, QueryEngine (DB-first + API fallback).
- **High-Level DataLoader**: Field-based queries, wide-format output, universe filtering.
- **Field Mappings Config** (`config/field_mappings.yaml`): Field name → endpoint mapping.

### 🚧 Open Gaps Against PRD/Architecture

- Transport hardening (HTTP/2, impersonation via curl_cffi, jittered backoff)
- Structured observability (logs/metrics) across Transport/Orchestrator
- Schema-first validation for endpoint registry (JSON Schema), versioned evolution
- Error taxonomy finalization and propagation (typed errors across layers)
- More endpoints and tidy shaping rules in `apis` (keeping transforms explicit)

---

## Next tasks (prioritized) with tests and acceptance criteria

### 🎯 **IMMEDIATE PRIORITY: Complete Storage Query Layer & Universe Builder**

These are critical for the high-level DataLoader API to work (DB-first with API fallback).

1) **Storage Query Layer** (`storage/query.py`) - **NEXT UP**
   
   **Design Philosophy:**
   - **Data-type-neutral**: Single generic query function works for all Hive-partitioned tables
   - **Pandas default**: Returns Pandas DataFrame (user-friendly), PyArrow for internal operations
   - **Optimization-first**: Leverage partition pruning, row-group pruning, column pruning
   - **Hidden complexity**: Adjustment factors queried internally; users only see `adjusted=True` flag
   
   **Public API:**
   ```python
   def query_parquet_table(
       db_path: str | Path,
       table_name: str,  # 'snapshots', 'adj_factors', 'liquidity_ranks'
       *,
       start_date: str,
       end_date: str,
       symbols: Optional[List[str]] = None,
       fields: Optional[List[str]] = None,
   ) -> pd.DataFrame:
       """
       Generic Hive-partitioned table query.
       
       Optimizations:
       1. Partition pruning: Only read TRD_DD=[start_date, end_date] partitions
       2. Row-group pruning: Filter by ISU_SRT_CD (leverages sorted writes)
       3. Column pruning: Only read requested fields
       
       Returns: Pandas DataFrame with TRD_DD injected from partition names
       """
   
   def load_universe_symbols(
       db_path: str | Path,
       universe_name: str,  # 'univ100', 'univ200', 'univ500', 'univ1000', 'univ2000'
       *,
       start_date: str,
       end_date: str,
   ) -> Dict[str, List[str]]:
       """
       Load pre-computed universe symbol lists per date.
       
       Returns: {date: [symbols]} mapping for per-date filtering
       Example: {'20240101': ['005930', '000660', ...], '20240102': [...]}
       
       Survivorship bias-free: Universe membership changes daily
       """
   ```
   
   **Internal Helpers:**
   - `_read_partitions_pyarrow()`: PyArrow zero-copy reads with filters
   - `_discover_partitions()`: Find available partitions (handles missing dates/holidays)
   - `_build_symbol_filter()`: Create PyArrow filter expressions for row-group pruning
   - `_inject_trd_dd_column()`: Add TRD_DD from partition names
   - `_parse_universe_rank()`: Map universe names to rank thresholds
   
   **Tests:**
   - Live smoke: `test_query_parquet_table_snapshots()` - Query snapshots with symbol filter
   - Live smoke: `test_query_parquet_table_adj_factors()` - Query sparse adj_factors table
   - Live smoke: `test_load_universe_symbols()` - Load univ100 (requires universe_builder first)
   - Unit: `test_discover_partitions_with_holidays()` - Missing partitions handled gracefully
   - Unit: `test_inject_trd_dd_column()` - Partition name parsing correct
   
   **Acceptance:**
   - Returns Pandas DataFrame by default
   - Fast queries (<500ms for 100 stocks × 252 days on SSD)
   - Missing partitions (holidays) handled silently
   - Correct partition/row-group/column pruning applied

2) **Universe Builder** (`pipelines/universe_builder.py`) - **AFTER QUERY LAYER**
   - Batch liquidity rank computation:
     - Read snapshots from Parquet DB
     - Group by `TRD_DD`, rank by `ACC_TRDVAL` descending
     - Persist to `liquidity_ranks` table (Hive-partitioned)
   - Tests:
     - Unit: Mock snapshot reader; verify ranking logic
     - Live smoke: Build ranks for existing `krx_db_test` (3 days), verify rank 1 = highest `ACC_TRDVAL`
   - Acceptance: Idempotent; correct cross-sectional ranking; resume-safe

3) **Layer 2 Services** (`apis/`) - **AFTER UNIVERSE BUILDER**
   - `field_mapper.py`: Load `config/field_mappings.yaml`; map field names → (endpoint_id, response_key)
   - `universe_service.py`: Resolve universe specs (`'univ100'`, explicit list) → per-date symbol lists
   - `query_engine.py`: DB-first query with API fallback; incremental ingestion
   - Tests:
     - Unit: Mock dependencies; verify mapping/resolution/fallback logic
     - Integration: End-to-end with test DB; verify API fallback triggers on missing dates
   - Acceptance: Clean interfaces; DB-first pattern works; incremental ingestion validated

4) **High-Level DataLoader Refactor** (`apis/dataloader.py`) - **FINAL STEP**
   - Implement field-based `get_data(fields, universe, start_date, end_date, options)`
   - Compose FieldMapper, UniverseService, QueryEngine
   - Return wide-format DataFrame (dates × symbols)
   - Tests:
     - Unit: Mock Layer 2 services; verify composition
     - Live smoke: Query with `universe='univ100'`, multiple fields, date range
   - Acceptance: Ergonomic API; wide-format output; universe filtering works

### 🔧 **SECONDARY PRIORITY: Infrastructure Hardening**

1) ~~Rate limiter (per-host)~~ - **✅ COMPLETED**
   - Token-bucket limiter driven by `requests_per_second` per host; injected into Transport.
   - Tests: 6/6 passing (unit)
   - Acceptance: ✅ Enforces rate limits; thread-safe; auto-configured from config

2) Transport hardening (curl_cffi)
   - Implement `curl_cffi.requests` client option with config-driven `http_version`, `impersonate`, `verify_tls`.
   - Add jittered exponential backoff for retryable statuses.
   - Tests:
     - Unit: ensure selected options are passed; backoff callable invoked; no retry on 4xx.
     - Live smoke (kept tiny): confirm basic compatibility with KRX using HTTP/2 where available.
   - Acceptance: parity with current behavior + options honored; no regressions in suite.

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
- `storage/writers.py`: `ParquetSnapshotWriter` (Hive-partitioned), `CSVSnapshotWriter` (legacy/debugging).
- `storage/schema.py`: Parquet schema definitions for snapshots, adj_factors, liquidity_ranks tables.
- `storage/query.py`: PyArrow/DuckDB query helpers for partition pruning and filtering.
- `pipelines/snapshots.py`:
  - `ingest_change_rates_day(raw_client, *, date, market, adjusted_flag, writer)` – fetch, preprocess, persist one day; returns row count.
  - `ingest_change_rates_range(raw_client, *, dates, market, adjusted_flag, writer)` – iterate days with per-day isolation (errors on one day do not halt subsequent days).
  - `compute_and_persist_adj_factors(snapshot_rows, writer)` – post-hoc batch job; computes factors and persists.
- `pipelines/universe_builder.py`: 
  - `build_liquidity_ranks(snapshots_path, *, date_range, writer)` – batch post-processing; ranks stocks by `ACC_TRDVAL` per date.
  - Outputs: `liquidity_ranks` table with `(TRD_DD, ISU_SRT_CD, xs_liquidity_rank, ACC_TRDVAL)`.

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

Storage contract (Parquet with Hive partitioning):

**Directory structure:**
```
db/
├── snapshots/
│   ├── TRD_DD=20230101/data.parquet
│   ├── TRD_DD=20230102/data.parquet
│   └── ...
├── adj_factors/
│   ├── TRD_DD=20230101/data.parquet
│   └── ...
└── liquidity_ranks/
    ├── TRD_DD=20230101/data.parquet
    └── ...
```

**Parquet write specifications:**

1. **Sorted writes (critical for row-group pruning):**
   ```python
   # In ParquetSnapshotWriter.write_snapshot_rows()
   rows_sorted = sorted(rows, key=lambda r: r['ISU_SRT_CD'])
   ```
   - Enables row-group pruning: Query for specific symbols only reads relevant row groups
   - Row group 1: `ISU_SRT_CD` ['000001' - '010000']
   - Row group 2: `ISU_SRT_CD` ['010001' - '020000']
   - Query for '005930' → only reads row group 1 (50-90% I/O reduction)

2. **Row group size:**
   ```python
   pq.write_to_dataset(
       table,
       root_path=snapshots_path,
       partition_cols=['TRD_DD'],
       row_group_size=1000,  # ~1000 stocks per row group
   )
   ```
   - Balances row-group pruning granularity vs. metadata overhead

3. **Compression:**
   ```python
   compression='zstd',
   compression_level=3,  # Better than Snappy, fast decompression
   ```

4. **Schema definitions (storage/schema.py):**
   ```python
   snapshots_schema = pa.schema([
       ('TRD_DD', pa.string()),          # Partition key (in directory, not data)
       ('ISU_SRT_CD', pa.string()),      # Primary filter key (first for stats)
       ('ISU_ABBRV', pa.string()),
       ('MKT_NM', pa.string()),
       ('BAS_PRC', pa.int64()),          # Base price (int, not comma-string)
       ('TDD_CLSPRC', pa.int64()),       # Close price
       ('CMPPREVDD_PRC', pa.int64()),    # Price change
       ('ACC_TRDVAL', pa.int64()),       # Trading value (for liquidity rank)
       # ... other fields
   ])
   
   adj_factors_schema = pa.schema([
       ('TRD_DD', pa.string()),
       ('ISU_SRT_CD', pa.string()),
       ('adj_factor', pa.float64()),
   ])
   
   liquidity_ranks_schema = pa.schema([
       ('TRD_DD', pa.string()),
       ('ISU_SRT_CD', pa.string()),
       ('xs_liquidity_rank', pa.int32()),  # Cross-sectional rank
       ('ACC_TRDVAL', pa.int64()),
   ])
   ```

5. **Partition behavior:**
   ```python
   existing_data_behavior='overwrite_or_ignore'  # Idempotent re-ingestion
   ```
   - Re-ingesting same date overwrites partition (resume-safe)
   - Holidays naturally produce no rows; no special handling required

**Expected file sizes:**
- Snapshots: ~500 KB - 2 MB per partition (compressed)
- Adj factors: ~10-50 KB per partition (sparse, only corporate action days)
- Liquidity ranks: ~100 KB per partition

**Query performance (SSD):**
- 100 stocks × 252 days: ~100-500 ms
- 500 stocks × 252 days: ~200-800 ms
- Full market (3000 stocks × 252 days): ~1-3 seconds

Exposure and layering:

- Ingestion is performed in pipelines and persists snapshots per day (append-only). The public API remains "as-is" by default and only returns transformed results when explicitly requested.
- Adjustment is a post job over stored snapshots (or the in-memory preprocessed set). Optional SQL/window-function re-computation can be added later for backfills and verification.

---

## Liquidity universe builder pipeline (pipelines/universe_builder.py)

Scope and rationale:

- Compute cross-sectional liquidity ranks per trading day based on `ACC_TRDVAL` (accumulated trading value).
- Enable pre-computed universe queries: `universe='univ100'` (top 100 by liquidity), `'univ500'`, etc.
- Survivorship bias-free: Rankings reflect actual liquidity on each historical date; delisted stocks included if they were liquid on that date.
- Batch post-processing: Run AFTER snapshot ingestion completes; does not block ingestion pipeline.

Algorithm (batch liquidity ranking):

1. **Read snapshots** from Parquet DB:
   ```python
   df = read_parquet('db/snapshots/', filters=[('TRD_DD', '>=', start_date), ('TRD_DD', '<=', end_date)])
   ```

2. **Group by date and rank by `ACC_TRDVAL` descending:**
   ```python
   # Using PyArrow/DuckDB or Pandas
   df_ranked = df.groupby('TRD_DD').apply(
       lambda g: g.assign(xs_liquidity_rank=g['ACC_TRDVAL'].rank(method='dense', ascending=False).astype(int))
   )
   ```
   - Per-date cross-sectional ranking (independent per `TRD_DD`)
   - Dense rank: No gaps in ranking (1, 2, 3, ... N)
   - Higher `ACC_TRDVAL` → lower rank number (rank 1 = most liquid)

3. **Persist liquidity ranks** to Parquet:
   ```python
   # ParquetSnapshotWriter.write_liquidity_ranks()
   ranks_sorted = sorted(ranks, key=lambda r: r['xs_liquidity_rank'])  # Sort by rank for row-group pruning
   
   pq.write_to_dataset(
       pa.Table.from_pylist(ranks_sorted, schema=liquidity_ranks_schema),
       root_path='db/liquidity_ranks/',
       partition_cols=['TRD_DD'],
       row_group_size=500,  # Smaller row groups (top 100, 500 often queried together)
       compression='zstd',
       compression_level=3,
   )
   ```

**Storage schema:**
```python
liquidity_ranks_schema = pa.schema([
    ('TRD_DD', pa.string()),
    ('ISU_SRT_CD', pa.string()),
    ('xs_liquidity_rank', pa.int32()),  # Cross-sectional rank (1 = most liquid)
    ('ACC_TRDVAL', pa.int64()),         # Trading value for reference
])
```

**Universe resolution in DataLoader:**
```python
# In UniverseService.resolve_universe()
if universe == 'univ100':
    df_universe = read_parquet(
        'db/liquidity_ranks/',
        filters=[
            ('TRD_DD', 'in', dates),
            ('xs_liquidity_rank', '<=', 100)
        ],
        columns=['TRD_DD', 'ISU_SRT_CD']
    )
    # Returns: {date: [symbols]} mapping
```

**Idempotency:**
- Can re-run `build_liquidity_ranks()` for new date ranges without re-ranking old dates
- Existing partitions overwritten if recomputing (e.g., after snapshot corrections)

**Testing strategy:**
- Live smoke test: Build ranks for 3 consecutive days, verify rank 1 has highest `ACC_TRDVAL`
- Unit test: Mock Parquet reader, verify ranking logic with synthetic data
- Integration test: End-to-end from snapshot ingestion → universe builder → DataLoader query
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
