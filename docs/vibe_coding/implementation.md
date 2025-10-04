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

### ✅ Completed Core Infrastructure (Layer 1)

- **ConfigFacade**: Implemented with Pydantic validation, env overrides supported; tests green (unit).
- **AdapterRegistry**: Implemented with `EndpointSpec` and `ParamSpec`; normalizes `client_policy.chunking` and infers `date_params` from param roles; tests green (unit).
- **Transport**: Implemented (requests-based), header merge, timeouts, retries, **config-driven rate limiting**; tests green (unit + live smoke).
- **RateLimiter**: Token bucket rate limiter with per-host throttling; thread-safe; auto-configured from config; tests green (unit, 6/6 passing).
- **Orchestrator**: Implemented; chunking/extraction/merge ordering; tests green (unit).
- **RawClient**: Implemented; required param validation and defaults; tests green (unit + live smoke).
- **Production Config**: Created `config/config.yaml` with safe defaults (1 req/sec rate limit); build script auto-detects prod vs test config.

### ✅ Completed Storage & Core Pipelines

- **Parquet Storage**: Hive-partitioned by `TRD_DD`; sorted writes by `ISU_SRT_CD` for row-group pruning; Zstd compression.
- **ParquetSnapshotWriter**: **Needs update** - Currently supports 3 tables (snapshots, adj_factors, liquidity_ranks); needs 2 more methods for universes + cumulative_adjustments.
- **Storage Query Layer** (`storage/query.py`): **✅ COMPLETED** - Generic `query_parquet_table()` + `load_universe_symbols()`; partition/row-group/column pruning; tests green (6/6, 1 skipped).
- **Pipeline Stage 1** (`pipelines/snapshots.py`): **✅ COMPLETED** - Resume-safe daily ingestion (`ingest_change_rates_range`); post-hoc adjustment factor computation; tests green (live smoke).
- **Transforms**: Preprocessing (TRD_DD injection, numeric coercion); adjustment (per-symbol LAG semantics + **needs cumulative multiplier function**); shaping (pivot_long_to_wide).
- **Production Scripts**: `build_db.py` (market-wide DB builder with rate limiting), `inspect_db.py` (DB inspection tool).

### 🔄 In Progress / Next Priorities

#### **IMMEDIATE: Fix Phantom Columns Bug**
- **storage/schema.py**: Remove OPNPRC, HGPRC, LWPRC from SNAPSHOTS_SCHEMA (don't exist in endpoint)
- **transforms/preprocessing.py**: Remove lines 60-62 (parsing non-existent fields)
- **Impact**: Discovered in Samsung split experiment; causes NaN values and wastes storage

#### **Phase 1: Post-Processing Pipelines (Stages 2-4)**
- **Pipeline Stage 2**: `pipelines/liquidity_ranking.py` (NEW) - Compute cross-sectional ranks by ACC_TRDVAL
- **Pipeline Stage 3**: `pipelines/universe_builder.py` (NEW) - Materialize universes table from liquidity_ranks
- **Schema Updates**: Add UNIVERSES_SCHEMA, CUMULATIVE_ADJUSTMENTS_SCHEMA
- **Writer Updates**: Add `write_universes()`, `write_cumulative_adjustments()` methods

#### **Phase 2: Layer 2 Services (Essential Abstractions)**
- **apis/field_mapper.py** (NEW) - Map field names → (table, column)
- **apis/universe_service.py** (NEW) - Resolve universe specs → per-date symbols
- **apis/query_engine.py** (NEW) - Orchestrate multi-table queries with adjustments

#### **Phase 3: DataLoader Rewrite (Range-Locked Design)**
- **apis/dataloader.py** (COMPLETE REWRITE) - Current stub needs replacement
  - Range-locked initialization with ephemeral cache build
  - Compose Layer 2 services
  - Wide-format output

### 🚧 Open Gaps Against PRD/Architecture (Deferred Post-MVP)

- Transport hardening (HTTP/2, impersonation via curl_cffi, jittered backoff)
- Structured observability (logs/metrics) across Transport/Orchestrator
- Schema-first validation for endpoint registry (JSON Schema), versioned evolution
- Error taxonomy finalization and propagation (typed errors across layers)
- Multi-field queries (PER, PBR, etc.) - FieldMapper ready but fields not configured

---

## Next tasks (prioritized) with tests and acceptance criteria

### 🎯 **IMMEDIATE PRIORITY: Fix Phantom Columns Bug**

**Context**: Samsung split experiment revealed OPNPRC, HGPRC, LWPRC don't exist in `stock.all_change_rates` endpoint but are defined in schema and preprocessing, causing NaN values.

**Tasks:**
1. Remove phantom columns from `storage/schema.py` (lines 44-46)
2. Remove phantom column parsing from `transforms/preprocessing.py` (lines 60-62)
3. Verify no other modules reference these columns
4. Re-run Samsung experiment to confirm fix

**Acceptance:**
- Schema only includes fields actually returned by KRX API
- No NaN columns in ingested data
- Storage size reduced (no wasted columns)

---

### 🎯 **PHASE 1: Post-Processing Pipelines (5-Stage Data Flow)**

These implement the corrected architecture's Stages 2-4 (Stage 1 already complete, Stage 5 is ephemeral cache).

### **CRITICAL: Precision Requirements for Cumulative Adjustments**

**Minimum precision for `cum_adj_multiplier`:** `1e-6` (6 decimal places)

**Rationale:**
- Stock splits can be as extreme as 50:1 (factor = 0.02)
- Cumulative products compound rounding errors if not precise enough
- Example: `0.02 × 0.5 × 2.0 = 0.02` (needs 6 decimals to represent accurately)

**Storage:** 
- `cum_adj_multiplier`: `pa.float64()` with minimum 1e-6 precision
- `adjusted_price`: `pa.int64()` (Korean Won is integer currency, round at application time)

**Computation:**
- Use `Decimal` for intermediate cumulative products (arbitrary precision)
- Convert to `float64` for storage (sufficient for 1e-6 precision)
- Round final prices to integers when applying adjustments

---

#### **Stage 2: Liquidity Ranking Pipeline** (`pipelines/liquidity_ranking.py`) - **NEW MODULE**

**Purpose**: Compute daily cross-sectional liquidity ranks based on `ACC_TRDVAL` (trading value).

**Algorithm:**
```python
def compute_liquidity_ranks(
    db_path: str | Path,
    *,
    start_date: str,
    end_date: str,
    writer: SnapshotWriter,
) -> int:
    """
    Read snapshots, rank by ACC_TRDVAL per date, persist to liquidity_ranks table.
    
    Returns: Number of rank rows persisted
    """
    # Step 1: Query snapshots for date range
    snapshots = query_parquet_table(
        db_path, 'snapshots',
        start_date=start_date, end_date=end_date,
        fields=['TRD_DD', 'ISU_SRT_CD', 'ACC_TRDVAL']
    )
    
    # Step 2: Group by TRD_DD, rank by ACC_TRDVAL descending
    ranks = snapshots.groupby('TRD_DD').apply(
        lambda g: g.assign(
            xs_liquidity_rank=g['ACC_TRDVAL'].rank(
                method='dense', ascending=False
            ).astype(int)
        )
    )
    
    # Step 3: Persist to liquidity_ranks table (Hive-partitioned)
    for date in ranks['TRD_DD'].unique():
        date_ranks = ranks[ranks['TRD_DD'] == date]
        writer.write_liquidity_ranks(
            date_ranks.to_dict('records'), date=date
        )
    
    return len(ranks)
```

**Tests:**
- Live smoke: Build ranks for 3 consecutive days from existing `krx_db_test`
- Unit: Mock query function, verify ranking logic with synthetic data
- Validate: Rank 1 = highest ACC_TRDVAL; dense ranking (no gaps)

**Acceptance:**
- Idempotent (can re-run for same date range)
- Cross-sectional (per-date independent)
- Resume-safe (per-day partition writes)

---

#### **Stage 3: Universe Materialization** (`pipelines/universe_builder.py`) - **NEW MODULE**

**Purpose**: Pre-compute universe symbol lists (univ100, univ500, etc.) from liquidity ranks.

**Algorithm:**
```python
def build_universes(
    db_path: str | Path,
    *,
    start_date: str,
    end_date: str,
    universe_tiers: Dict[str, int],  # {'univ100': 100, 'univ500': 500, ...}
    writer: SnapshotWriter,
) -> int:
    """
    Query liquidity_ranks, filter by rank thresholds, materialize to universes table.
    
    Returns: Number of universe membership rows persisted
    """
    # Step 1: Query liquidity ranks
    ranks = query_parquet_table(
        db_path, 'liquidity_ranks',
        start_date=start_date, end_date=end_date,
        fields=['TRD_DD', 'ISU_SRT_CD', 'xs_liquidity_rank']
    )
    
    # Step 2: For each universe tier, filter symbols
    universe_rows = []
    for universe_name, rank_threshold in universe_tiers.items():
        tier_symbols = ranks[ranks['xs_liquidity_rank'] <= rank_threshold]
        tier_symbols['universe_name'] = universe_name
        universe_rows.extend(tier_symbols.to_dict('records'))
    
    # Step 3: Persist to universes table (Hive-partitioned by TRD_DD)
    for date in ranks['TRD_DD'].unique():
        date_universes = [r for r in universe_rows if r['TRD_DD'] == date]
        writer.write_universes(date_universes, date=date)
    
    return len(universe_rows)
```

**Schema Addition** (`storage/schema.py`):
```python
UNIVERSES_SCHEMA = pa.schema([
    ('ISU_SRT_CD', pa.string()),
    ('universe_name', pa.string()),  # 'univ100', 'univ500', etc.
    ('xs_liquidity_rank', pa.int32()),  # For reference
])
```

**Tests:**
- Live smoke: Build universes for 3 days from existing liquidity_ranks
- Unit: Mock ranks reader, verify filtering logic
- Validate: univ100 ⊂ univ500 ⊂ univ1000 (proper subsets)

**Acceptance:**
- Survivorship bias-free (per-date lists)
- Fast universe queries (pre-computed, no on-the-fly ranking)
- Persistent table (not ephemeral)

---

#### **Stage 4: Schema & Writer Updates**

**Schema additions** (`storage/schema.py`):
```python
# Add to schema.py:
CUMULATIVE_ADJUSTMENTS_SCHEMA = pa.schema([
    ('TRD_DD', pa.string()),              # Trade date (also partition key)
    ('ISU_SRT_CD', pa.string()),          # Security ID
    ('cum_adj_multiplier', pa.float64()),  # Product of future adj_factors (MINIMUM 1e-6 precision)
])

UNIVERSES_SCHEMA = pa.schema([
    ('ISU_SRT_CD', pa.string()),
    ('universe_name', pa.string()),
    ('xs_liquidity_rank', pa.int32()),
])

__all__ = [
    "SNAPSHOTS_SCHEMA",
    "ADJ_FACTORS_SCHEMA",
    "LIQUIDITY_RANKS_SCHEMA",
    "CUMULATIVE_ADJUSTMENTS_SCHEMA",  # NEW
    "UNIVERSES_SCHEMA",  # NEW
]
```

**Writer method additions** (`storage/writers.py`):
```python
# Add to ParquetSnapshotWriter class:

def write_universes(self, rows: List[Dict[str, Any]], *, date: str) -> None:
    """Write universe membership rows for a specific date."""
    if not rows:
        return
    
    table = pa.Table.from_pylist(rows, schema=UNIVERSES_SCHEMA)
    universes_path = self._root / 'universes'
    
    pq.write_to_dataset(
        table, root_path=universes_path,
        partition_cols=['TRD_DD'],  # Inject from date param
        row_group_size=500,
        compression='zstd', compression_level=3,
        existing_data_behavior='overwrite_or_ignore'
    )

def write_cumulative_adjustments(
    self, rows: List[Dict[str, Any]], *, date: str, temp_root: Path
) -> None:
    """
    Write cumulative adjustment multipliers to EPHEMERAL temp cache.
    
    Note: temp_root should be data/temp/, NOT data/krx_db/
    """
    if not rows:
        return
    
    table = pa.Table.from_pylist(rows, schema=CUMULATIVE_ADJUSTMENTS_SCHEMA)
    temp_path = temp_root / 'cumulative_adjustments'
    
    pq.write_to_dataset(
        table, root_path=temp_path,
        partition_cols=['TRD_DD'],
        row_group_size=1000,
        compression='zstd', compression_level=3,
        existing_data_behavior='overwrite_or_ignore'
    )
```

**Tests:**
- Unit: Mock writes, verify correct paths (persistent vs temp)
- Live smoke: Write sample data, read back, verify schema

---

### 🎯 **PHASE 2: Layer 2 Services (Essential Abstractions)**

These services were initially removed as "over-engineered" but the corrected architecture reinstates them as essential.

#### **Service 1: FieldMapper** (`apis/field_mapper.py`) - **NEW MODULE**

**Purpose**: Map user-facing field names → (table, column, transform_hints).

**Design:**
```python
from typing import NamedTuple, Optional

class FieldMapping(NamedTuple):
    table: str  # 'snapshots', 'adj_factors', etc.
    column: str  # Actual column name in Parquet
    transform_hint: Optional[str] = None  # 'log_scale', 'cumulative_adj', etc.

class FieldMapper:
    """
    Map user-facing field names to storage locations.
    
    For MVP: Hardcode mappings in __init__ (no YAML).
    For future: Load from config/field_mappings.yaml.
    """
    
    def __init__(self):
        self._mappings = {
            'close': FieldMapping('snapshots', 'TDD_CLSPRC', None),
            'open': FieldMapping('snapshots', 'OPNPRC', None),  # When added
            'high': FieldMapping('snapshots', 'HGPRC', None),
            'low': FieldMapping('snapshots', 'LWPRC', None),
            'volume': FieldMapping('snapshots', 'ACC_TRDVOL', None),
            'value': FieldMapping('snapshots', 'ACC_TRDVAL', None),
            'base_price': FieldMapping('snapshots', 'BAS_PRC', None),
            # Future: 'per', 'pbr', 'div_yield', etc.
        }
    
    def resolve(self, field_name: str) -> FieldMapping:
        """Resolve field name to (table, column, transform_hint)."""
        if field_name not in self._mappings:
            raise ValueError(f"Unknown field: {field_name}")
        return self._mappings[field_name]
    
    def supports_adjustment(self, field_name: str) -> bool:
        """Check if field can be adjusted for corporate actions."""
        # Only price fields can be adjusted
        return field_name in ('close', 'open', 'high', 'low', 'base_price')
```

**Tests:**
- Unit: Verify resolve() for known fields, raises for unknown
- Unit: Verify supports_adjustment() logic

**Acceptance:**
- Extensible (easy to add new fields)
- Type-safe (NamedTuple instead of dict)
- Fast (dict lookup, no I/O)

---

#### **Service 2: UniverseService** (`apis/universe_service.py`) - **NEW MODULE**

**Purpose**: Resolve universe specifications → per-date symbol lists.

**Design:**
```python
from typing import List, Dict, Union

class UniverseService:
    """
    Resolve universe specs to per-date symbol lists.
    
    Supports:
    - Pre-computed universes: 'univ100', 'univ500', etc.
    - Explicit symbol lists: ['005930', '000660', ...]
    - Future: Index constituents, sector filters, etc.
    """
    
    def __init__(self, db_path: str | Path):
        self._db_path = db_path
    
    def resolve(
        self,
        universe: Union[str, List[str]],
        *,
        start_date: str,
        end_date: str,
    ) -> Dict[str, List[str]]:
        """
        Resolve universe spec to per-date symbol lists.
        
        Returns: {date: [symbols]} mapping
        
        Examples:
          resolve('univ100', ...) → {'20240101': ['005930', ...], ...}
          resolve(['005930', '000660'], ...) → {'20240101': ['005930', '000660'], ...}
        """
        if isinstance(universe, list):
            # Explicit list: same symbols for all dates
            dates = self._get_trading_dates(start_date, end_date)
            return {date: universe for date in dates}
        
        elif universe.startswith('univ'):
            # Pre-computed universe: query universes table
            return self._load_precomputed_universe(
                universe, start_date, end_date
            )
        
        else:
            raise ValueError(f"Unknown universe type: {universe}")
    
    def _load_precomputed_universe(
        self, universe_name: str, start_date: str, end_date: str
    ) -> Dict[str, List[str]]:
        """Query universes table via storage/query.py"""
        from ..storage.query import load_universe_symbols
        return load_universe_symbols(
            self._db_path, universe_name, start_date=start_date, end_date=end_date
        )
    
    def _get_trading_dates(self, start_date: str, end_date: str) -> List[str]:
        """Get available trading dates from DB (query snapshots partitions)."""
        from ..storage.query import query_parquet_table
        # Query for TRD_DD only (fast)
        df = query_parquet_table(
            self._db_path, 'snapshots',
            start_date=start_date, end_date=end_date,
            fields=['TRD_DD']
        )
        return sorted(df['TRD_DD'].unique().tolist())
```

**Tests:**
- Unit: Mock query functions, verify explicit list handling
- Live smoke: Resolve 'univ100' from test DB, verify per-date lists
- Validate: univ100 has 100 symbols per date (±delisting edge cases)

**Acceptance:**
- Supports both explicit lists and pre-computed universes
- Survivorship bias-free (per-date lists from materialized table)
- Fast (delegates to optimized storage/query.py)

---

#### **Service 3: QueryEngine** (`apis/query_engine.py`) - **NEW MODULE**

**Purpose**: Orchestrate multi-table queries with adjustment application.

**Design:**
```python
from typing import Optional, List, Dict
import pandas as pd

class QueryEngine:
    """
    Orchestrate DB queries with adjustment application.
    
    Responsibilities:
    - Query raw prices from snapshots
    - Apply cumulative adjustments (if adjusted=True)
    - Filter by universe (per-date symbol lists)
    - Merge multiple fields
    """
    
    def __init__(
        self,
        db_path: str | Path,
        temp_path: str | Path,  # For cumulative_adjustments cache
        field_mapper: FieldMapper,
        universe_service: UniverseService,
    ):
        self._db_path = db_path
        self._temp_path = temp_path
        self._field_mapper = field_mapper
        self._universe_service = universe_service
    
    def query_field(
        self,
        field_name: str,
        *,
        start_date: str,
        end_date: str,
        symbols: Optional[List[str]] = None,
        universe: Optional[str] = None,
        adjusted: bool = True,
    ) -> pd.DataFrame:
        """
        Query a single field with optional adjustment and universe filtering.
        
        Returns: DataFrame with columns [TRD_DD, ISU_SRT_CD, <field_name>]
        """
        from ..storage.query import query_parquet_table
        
        # Step 1: Resolve field to (table, column)
        mapping = self._field_mapper.resolve(field_name)
        
        # Step 2: Query raw data
        df = query_parquet_table(
            self._db_path, mapping.table,
            start_date=start_date, end_date=end_date,
            symbols=symbols,  # None = all symbols
            fields=['TRD_DD', 'ISU_SRT_CD', mapping.column]
        )
        
        # Step 3: Apply adjustment if requested and supported
        if adjusted and self._field_mapper.supports_adjustment(field_name):
            df = self._apply_adjustment(df, mapping.column, start_date, end_date)
        
        # Step 4: Apply universe filter
        if universe:
            universe_symbols = self._universe_service.resolve(
                universe, start_date=start_date, end_date=end_date
            )
            df = self._filter_by_universe(df, universe_symbols)
        
        # Step 5: Rename column to user-facing field name
        df = df.rename(columns={mapping.column: field_name})
        
        return df
    
    def _apply_adjustment(
        self, df: pd.DataFrame, column: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """Apply cumulative adjustments from temp cache."""
        from ..storage.query import query_parquet_table
        
        # Query cumulative adjustments from ephemeral cache
        cum_adj = query_parquet_table(
            self._temp_path, 'cumulative_adjustments',
            start_date=start_date, end_date=end_date,
            fields=['TRD_DD', 'ISU_SRT_CD', 'cum_adj_multiplier']
        )
        
        # Merge and multiply
        df = df.merge(cum_adj, on=['TRD_DD', 'ISU_SRT_CD'], how='left')
        df['cum_adj_multiplier'] = df['cum_adj_multiplier'].fillna(1.0)
        
        # Apply adjustment and round to integer (Korean Won is integer currency)
        # cum_adj_multiplier maintains 1e-6 precision, but final price is integer
        df[column] = (df[column] * df['cum_adj_multiplier']).round(0).astype(int)
        df = df.drop(columns=['cum_adj_multiplier'])
        
        return df
    
    def _filter_by_universe(
        self, df: pd.DataFrame, universe_symbols: Dict[str, List[str]]
    ) -> pd.DataFrame:
        """Filter DataFrame by per-date symbol lists."""
        # Create filter mask per date
        mask = df.apply(
            lambda row: row['ISU_SRT_CD'] in universe_symbols.get(row['TRD_DD'], []),
            axis=1
        )
        return df[mask]
```

**Tests:**
- Unit: Mock dependencies, verify query flow
- Live smoke: Query 'close' with adjusted=True, verify prices are adjusted
- Live smoke: Query with universe='univ100', verify only 100 symbols per date

**Acceptance:**
- Composes FieldMapper + UniverseService correctly
- Applies adjustments only when requested and supported
- Per-date universe filtering works (survivorship bias-free)

---

### 🎯 **PHASE 3: DataLoader Rewrite (Range-Locked Design)**

Complete rewrite of `apis/dataloader.py` to match corrected architecture.

**Current status**: Stub from old design (only has `get_daily_quotes`, `get_individual_history`); needs complete replacement.

**New Design:**
```python
from pathlib import Path
from typing import List, Optional, Union
import pandas as pd

class DataLoader:
    """
    Range-locked, stateful DataLoader with ephemeral adjustment cache.
    
    Design principles:
    - Initialized with fixed [start_date, end_date] window
    - Builds ephemeral cumulative adjustment cache on __init__
    - Composes Layer 2 services (FieldMapper, UniverseService, QueryEngine)
    - Returns wide-format DataFrames (dates × symbols)
    - No automatic API fallback (DB must be pre-built)
    """
    
    def __init__(
        self,
        db_path: str | Path,
        *,
        start_date: str,
        end_date: str,
        temp_path: Optional[str | Path] = None,
    ):
        """
        Initialize range-locked DataLoader and build ephemeral cache.
        
        Parameters:
          db_path: Path to Parquet DB (data/krx_db/)
          start_date: Start of query window (YYYYMMDD)
          end_date: End of query window (YYYYMMDD)
          temp_path: Path for ephemeral cache (default: data/temp/)
        
        Cache build process (~1-2 seconds):
          1. Query adj_factors from persistent DB
          2. Compute cumulative multipliers per-symbol (reverse chronological)
          3. Write to temp_path/cumulative_adjustments/ (Hive-partitioned)
        """
        from .field_mapper import FieldMapper
        from .universe_service import UniverseService
        from .query_engine import QueryEngine
        from ..transforms.adjustment import compute_cumulative_adjustments
        from ..storage.writers import ParquetSnapshotWriter
        
        self._db_path = Path(db_path)
        self._temp_path = Path(temp_path or 'data/temp')
        self._start_date = start_date
        self._end_date = end_date
        
        # Initialize Layer 2 services
        self._field_mapper = FieldMapper()
        self._universe_service = UniverseService(self._db_path)
        self._query_engine = QueryEngine(
            self._db_path, self._temp_path,
            self._field_mapper, self._universe_service
        )
        
        # Build ephemeral adjustment cache
        self._build_adjustment_cache()
    
    def _build_adjustment_cache(self) -> None:
        """
        Build ephemeral cumulative adjustment cache for [start_date, end_date].
        
        Stage 5 of data flow (ephemeral, not persistent).
        """
        from ..storage.query import query_parquet_table
        from ..transforms.adjustment import compute_cumulative_adjustments
        from ..storage.writers import ParquetSnapshotWriter
        
        print(f"[DataLoader] Building ephemeral adjustment cache...")
        print(f"  Window: {self._start_date} → {self._end_date}")
        
        # Step 1: Query adjustment factors (event markers)
        adj_factors = query_parquet_table(
            self._db_path, 'adj_factors',
            start_date=self._start_date, end_date=self._end_date,
            fields=['TRD_DD', 'ISU_SRT_CD', 'adj_factor']
        )
        
        # Step 2: Compute cumulative multipliers (per-symbol, reverse chronological)
        cum_adj_rows = compute_cumulative_adjustments(
            adj_factors.to_dict('records')
        )
        
        # Step 3: Write to ephemeral cache (temp directory)
        writer = ParquetSnapshotWriter(root_path=self._temp_path)
        for date in set(r['TRD_DD'] for r in cum_adj_rows):
            date_rows = [r for r in cum_adj_rows if r['TRD_DD'] == date]
            writer.write_cumulative_adjustments(
                date_rows, date=date, temp_root=self._temp_path
            )
        
        print(f"  ✓ Cache built: {len(cum_adj_rows)} rows")
    
    def get_data(
        self,
        fields: Union[str, List[str]],
        *,
        symbols: Optional[List[str]] = None,
        universe: Optional[str] = None,
        query_start: Optional[str] = None,
        query_end: Optional[str] = None,
        adjusted: bool = True,
    ) -> pd.DataFrame:
        """
        Query data with optional adjustment and universe filtering.
        
        Parameters:
          fields: Field name(s) to query ('close', 'volume', etc.)
          symbols: Explicit symbol list (mutually exclusive with universe)
          universe: Pre-computed universe ('univ100', etc.)
          query_start: Sub-range start (must be within [start_date, end_date])
          query_end: Sub-range end (must be within [start_date, end_date])
          adjusted: Apply cumulative adjustments (default: True)
        
        Returns: Wide-format DataFrame (dates as index, symbols as columns)
        
        Raises:
          ValueError: If query range outside loader's [start_date, end_date]
        """
        # Normalize fields to list
        if isinstance(fields, str):
            fields = [fields]
        
        # Validate query range
        q_start = query_start or self._start_date
        q_end = query_end or self._end_date
        if q_start < self._start_date or q_end > self._end_date:
            raise ValueError(
                f"Query range [{q_start}, {q_end}] outside loader window "
                f"[{self._start_date}, {self._end_date}]. "
                f"Create new DataLoader instance for different range."
            )
        
        # Query each field
        field_dfs = []
        for field in fields:
            df = self._query_engine.query_field(
                field,
                start_date=q_start, end_date=q_end,
                symbols=symbols, universe=universe,
                adjusted=adjusted
            )
            field_dfs.append(df)
        
        # Merge fields if multiple
        if len(field_dfs) == 1:
            merged = field_dfs[0]
        else:
            merged = field_dfs[0]
            for df in field_dfs[1:]:
                merged = merged.merge(df, on=['TRD_DD', 'ISU_SRT_CD'], how='outer')
        
        # Pivot to wide format (dates × symbols)
        wide = self._pivot_to_wide(merged, fields)
        
        return wide
    
    def _pivot_to_wide(
        self, df: pd.DataFrame, fields: List[str]
    ) -> pd.DataFrame:
        """
        Pivot long-format DataFrame to wide format.
        
        Input: [TRD_DD, ISU_SRT_CD, field1, field2, ...]
        Output: MultiIndex(field, symbol) columns, TRD_DD index
        
        For single field: Simple columns (symbol names)
        For multiple fields: MultiIndex columns (field, symbol)
        """
        from ..transforms.shaping import pivot_long_to_wide
        
        if len(fields) == 1:
            # Single field: Simple columns
            wide = df.pivot(
                index='TRD_DD', columns='ISU_SRT_CD', values=fields[0]
            )
            wide.index.name = 'date'
            wide.columns.name = 'symbol'
        else:
            # Multiple fields: MultiIndex columns
            wide = df.set_index(['TRD_DD', 'ISU_SRT_CD']).unstack('ISU_SRT_CD')
            wide.index.name = 'date'
            wide.columns.names = ['field', 'symbol']
        
        return wide
```

**Tests:**
- Unit: Mock Layer 2 services, verify composition
- Unit: Verify query range validation (rejects out-of-range queries)
- Live smoke: Initialize with 3-day range, query 'close' with adjusted=True
- Live smoke: Query with universe='univ100', verify wide-format output
- Validate: Cache rebuilt if new instance with different range

**Acceptance:**
- Range-locked pattern enforced (errors on out-of-range queries)
- Ephemeral cache built once per instance (~1-2s overhead)
- Sub-range queries within window are instant
- Wide-format output matches legacy kor-quant-dataloader API
- Clean composition of Layer 2 services

---

## Implementation Priority Summary

### **Week 1: Fix Bugs & Post-Processing Pipelines**
1. ✅ **Day 1**: Fix phantom columns bug (schema + preprocessing)
2. ✅ **Day 2-3**: Implement `pipelines/liquidity_ranking.py` with tests
3. ✅ **Day 3-4**: Implement `pipelines/universe_builder.py` with tests
4. ✅ **Day 4-5**: Update schema (add 2 new schemas) and writer (add 2 new methods)

### **Week 2: Layer 2 Services**
1. **Day 1**: Implement `apis/field_mapper.py` with unit tests
2. **Day 2**: Implement `apis/universe_service.py` with unit tests
3. **Day 3-4**: Implement `apis/query_engine.py` with live smoke tests
4. **Day 5**: Integration tests for Layer 2 services

### **Week 3: DataLoader Rewrite & Integration**
1. **Day 1-2**: Rewrite `apis/dataloader.py` (range-locked design)
2. **Day 3**: Add cumulative adjustment computation to `transforms/adjustment.py`
3. **Day 4**: Live smoke tests for full end-to-end flow
4. **Day 5**: Samsung split experiment rerun (validation)

### **Week 4: Production Scripts & Documentation**
1. **Day 1**: Update `build_db.py` to run all post-processing stages
2. **Day 2**: Create production scripts (compute_liquidity_ranks.py, build_universes.py)
3. **Day 3-4**: Update user-facing documentation and examples
4. **Day 5**: Final integration tests and release prep

---

---

## Original sections (kept for reference)

The detailed implementation plans above supersede the following older outlines:

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
