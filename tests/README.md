# Tests overview

This document explains what each test file covers, how tests are organized by layer, and how to run them.

## How to run

- Run everything (default includes live smoke tests):
  - `poetry run pytest -q`
- Run only unit tests (no network):
  - `poetry run pytest -q -m unit`
- Skip live tests:
  - `poetry run pytest -q -m "not live"`

Markers (strict): `unit`, `integration`, `live`, `slow`.

## Fixtures and config

- `tests/fixtures/test_config.yaml`: Canonical test configuration used across tests.
- `tests/conftest.py`: Provides `test_config_path` (absolute path to the YAML).

## Test files

### test_config_facade.py (unit)

- Validates configuration loading and invariants:
  - Loads defaults from YAML (hosts, headers, paths, timeouts).
  - Environment overrides via prefix take precedence.
  - Enforces HTTPS base_url; invalid values raise.
  - Retry policy fields load as declared.
  - Settings are immutable; idempotent loads produce equivalent values.

### test_adapter_registry.py (unit)

- Validates endpoint registry parsing from YAML into immutable specs:
  - Loads `stock.daily_quotes` and `stock.individual_history` with method/path/bld.
  - Preserves `response.root_keys` order from YAML.
  - Exposes optional `date_params`, `chunking`, and `order_by` when present.
  - Specs are immutable (writes raise).
  - Malformed specs (e.g., missing `bld`) raise with diagnostics.
  - Unknown keys under an endpoint are ignored (do not surface on the spec).

### test_transport_policy_unit.py (unit, no network)

- Validates transport policy and request building using a fake HTTP client:
  - Retries only on configured statuses; no retry on 4xx; errors raise.
  - Propagates configured connect/read timeouts.
  - GET uses `params` and POST uses `data`; default headers merge with overrides.

### test_transport_live_smoke.py (integration, live)

- Minimal live call through `Transport` built from config + `AdapterRegistry` spec to KRX:
  - Confirms host/path/headers and `bld` produce a real JSON response with rows.

### test_orchestrator_mechanics_unit.py (unit)

- Validates the orchestrator’s mechanical workflow with a fake transport:
  - Splits long date ranges into chunks per spec and merges results.
  - Honors ordered response root keys; supports mixed roots across chunks.
  - Sorts final rows by `order_by` when specified.
  - Raises `ExtractionError` when no known root keys are present.
  - Single-date endpoints are not chunked (single call).

### test_live_smoke_krx.py (integration, live)

- Early live smoke that uses `ConfigFacade` + `AdapterRegistry` to build a real request via `requests`:
  - Confirms endpoint schema (method/path/bld/root_keys) is valid against KRX.

### test_raw_client_validation.py (unit)

- Validates the public raw interface behavior with a fake orchestrator:
  - Unknown endpoint id raises a clear error.
  - Missing required parameters raise a validation error.
  - Declared defaults (`params.*.default`) are applied.
  - Errors from orchestrator propagate (no swallowing).

### test_raw_client_live_smoke.py (integration, live)

- Full-stack live smoke through Config → Adapter → Transport → Orchestrator → RawClient:
  - Confirms end-to-end wiring returns non-empty rows for a small, real request.

## Layer mapping

- Config: `test_config_facade.py`
- Adapter (registry): `test_adapter_registry.py`
- Transport: `test_transport_policy_unit.py`, `test_transport_live_smoke.py`
- Orchestrator: `test_orchestrator_mechanics_unit.py`
- Raw client: `test_raw_client_validation.py`, `test_raw_client_live_smoke.py`
- Direct live smoke (schema/path sanity): `test_live_smoke_krx.py`
