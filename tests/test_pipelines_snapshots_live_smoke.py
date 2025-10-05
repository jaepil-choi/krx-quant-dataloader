"""
Live smoke tests for pipelines.snapshots module

⚠️ DEPRECATED: This test file validates the OLD pipelines.snapshots module
which has been superseded by the PipelineOrchestrator 3-stage enrichment pipeline.

The pipelines.snapshots module is kept for backward compatibility but is no longer
the primary ingestion path. New code should use:
- PipelineOrchestrator for data ingestion
- TempSnapshotWriter / PriceVolumeWriter for storage
- Progressive enrichment (Stage 1: raw, Stage 2: adj_factor, Stage 3: liquidity_rank)

This test file is kept to ensure backward compatibility but may be removed in future versions.

Purpose: Validate end-to-end pipeline with real KRX API calls.
Prints sample outputs for visual inspection and debugging.
"""

import csv
import json
from pathlib import Path

import pytest

from krx_quant_dataloader.config import ConfigFacade
from krx_quant_dataloader.adapter import AdapterRegistry
from krx_quant_dataloader.transport import Transport
from krx_quant_dataloader.orchestration import Orchestrator
from krx_quant_dataloader.client import RawClient
from krx_quant_dataloader.storage.writers import CSVSnapshotWriter
from krx_quant_dataloader.pipelines.snapshots import (
    ingest_change_rates_day,
    ingest_change_rates_range,
    compute_and_persist_adj_factors,
)


@pytest.fixture(scope="module")
def raw_client(test_config_path: str):
    """Raw client for live KRX API calls."""
    cfg = ConfigFacade.load(settings_path=test_config_path)
    reg = AdapterRegistry.load(config_path=test_config_path)
    transport = Transport(cfg)
    orch = Orchestrator(transport)
    return RawClient(registry=reg, orchestrator=orch)


@pytest.mark.live
def test_ingest_change_rates_day_live(raw_client, tmp_path):
    """Test single day ingestion with real KRX data.
    
    Validates:
    - Fetch succeeds for recent trading day
    - Preprocessing injects TRD_DD and coerces numeric fields
    - Writer persists rows correctly
    - Visual inspection of sample data
    """
    snapshot_csv = tmp_path / "snapshots_day.csv"
    factor_csv = tmp_path / "factors_day.csv"
    
    writer = CSVSnapshotWriter(
        snapshot_path=snapshot_csv,
        factor_path=factor_csv,
    )
    
    # Use recent trading day
    date = "20250814"
    
    count = ingest_change_rates_day(
        raw_client,
        date=date,
        market="ALL",
        adjusted_flag=False,
        writer=writer,
    )
    
    print(f"\n{'='*60}")
    print(f"=== SINGLE DAY INGESTION: {date} ===")
    print(f"{'='*60}")
    print(f"Rows written: {count}")
    
    # Read back and display sample
    assert snapshot_csv.exists(), "Snapshot CSV should exist"
    
    with open(snapshot_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    print(f"\n=== SAMPLE ROWS (first 3) ===")
    for i, row in enumerate(rows[:3]):
        print(f"\nRow {i}:")
        print(json.dumps(row, indent=2, ensure_ascii=False))
    
    # Assertions
    assert count > 0, "Should have rows for trading day"
    assert len(rows) == count, "CSV should match returned count"
    
    # Validate preprocessing
    first_row = rows[0]
    assert "TRD_DD" in first_row, "TRD_DD should be injected"
    assert first_row["TRD_DD"] == date, "TRD_DD should match date"
    assert "ISU_SRT_CD" in first_row, "ISU_SRT_CD should exist"
    assert "BAS_PRC" in first_row, "BAS_PRC should exist"
    assert "TDD_CLSPRC" in first_row, "TDD_CLSPRC should exist"
    
    # Numeric fields should be integers (not comma strings)
    # Note: CSV writes back as strings, so we check they're numeric strings without commas
    assert "," not in first_row.get("BAS_PRC", ""), "BAS_PRC should not have commas"
    assert "," not in first_row.get("TDD_CLSPRC", ""), "TDD_CLSPRC should not have commas"
    
    writer.close()


@pytest.mark.live
def test_ingest_change_rates_range_live(raw_client, tmp_path):
    """Test multi-day ingestion including holiday with real KRX data.
    
    Validates:
    - Per-day isolation (errors don't halt subsequent days)
    - Holiday handling (0 rows, no writes)
    - Resume-safe semantics
    - Visual inspection of per-day results
    """
    snapshot_csv = tmp_path / "snapshots_range.csv"
    factor_csv = tmp_path / "factors_range.csv"
    
    writer = CSVSnapshotWriter(
        snapshot_path=snapshot_csv,
        factor_path=factor_csv,
    )
    
    # Include holiday (20250815) between two trading days
    dates = ["20250814", "20250815", "20250818"]
    
    counts = ingest_change_rates_range(
        raw_client,
        dates=dates,
        market="ALL",
        adjusted_flag=False,
        writer=writer,
    )
    
    print(f"\n{'='*60}")
    print(f"=== MULTI-DAY INGESTION ===")
    print(f"{'='*60}")
    print(f"\nPer-day results:")
    for date, count in counts.items():
        status = "HOLIDAY" if count == 0 else f"{count} rows"
        print(f"  {date}: {status}")
    
    # Read back snapshots
    with open(snapshot_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        all_rows = list(reader)
    
    print(f"\nTotal rows in CSV: {len(all_rows)}")
    
    # Show sample from each day
    by_day = {}
    for row in all_rows:
        day = row["TRD_DD"]
        if day not in by_day:
            by_day[day] = []
        by_day[day].append(row)
    
    print(f"\n=== SAMPLES BY DAY ===")
    for day in sorted(by_day.keys()):
        day_rows = by_day[day]
        print(f"\n{day}: {len(day_rows)} rows")
        sample = day_rows[0]
        print(f"  Sample symbol: {sample['ISU_SRT_CD']}")
        print(f"  BAS_PRC: {sample['BAS_PRC']}, TDD_CLSPRC: {sample['TDD_CLSPRC']}")
    
    # Assertions
    assert counts["20250815"] == 0, "Holiday should return 0 rows"
    assert counts["20250814"] > 0, "Trading day 1 should have rows"
    assert counts["20250818"] > 0, "Trading day 2 should have rows"
    
    # Total rows should equal sum of non-holiday counts
    expected_total = counts["20250814"] + counts["20250818"]
    assert len(all_rows) == expected_total, "CSV should contain only non-holiday rows"
    
    # Should have rows from exactly 2 days
    assert len(by_day) == 2, "Should have 2 trading days in CSV"
    assert "20250814" in by_day, "Should have first trading day"
    assert "20250818" in by_day, "Should have second trading day"
    assert "20250815" not in by_day, "Should not have holiday"
    
    writer.close()


@pytest.mark.live
def test_compute_and_persist_adj_factors_live(raw_client, tmp_path):
    """Test post-hoc adjustment factor computation with real KRX data.
    
    Validates:
    - Factor computation from real snapshots
    - Per-symbol LAG semantics (first day empty, subsequent days computed)
    - Decimal precision preserved
    - Visual inspection of factor progression
    """
    snapshot_csv = tmp_path / "snapshots_factors.csv"
    factor_csv = tmp_path / "factors_output.csv"
    
    writer = CSVSnapshotWriter(
        snapshot_path=snapshot_csv,
        factor_path=factor_csv,
    )
    
    # First, ingest snapshots for multiple days
    dates = ["20250814", "20250815", "20250818"]
    
    counts = ingest_change_rates_range(
        raw_client,
        dates=dates,
        market="ALL",
        adjusted_flag=False,
        writer=writer,
    )
    
    # Read back snapshots
    with open(snapshot_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        snapshots = list(reader)
    
    # Convert numeric strings back to ints for factor computation
    for row in snapshots:
        if row.get("BAS_PRC"):
            row["BAS_PRC"] = int(row["BAS_PRC"]) if row["BAS_PRC"] else None
        if row.get("TDD_CLSPRC"):
            row["TDD_CLSPRC"] = int(row["TDD_CLSPRC"]) if row["TDD_CLSPRC"] else None
    
    # Compute and persist factors
    factor_count = compute_and_persist_adj_factors(snapshots, writer)
    
    print(f"\n{'='*60}")
    print(f"=== ADJUSTMENT FACTOR COMPUTATION ===")
    print(f"{'='*60}")
    print(f"Snapshots processed: {len(snapshots)}")
    print(f"Factors computed: {factor_count}")
    
    # Read back factors
    with open(factor_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        factors = list(reader)
    
    # Group by symbol
    by_symbol = {}
    for f in factors:
        symbol = f["ISU_SRT_CD"]
        if symbol not in by_symbol:
            by_symbol[symbol] = []
        by_symbol[symbol].append(f)
    
    print(f"\n=== FACTOR PROGRESSION (first 3 symbols) ===")
    for symbol in sorted(by_symbol.keys())[:3]:
        symbol_factors = sorted(by_symbol[symbol], key=lambda x: x["TRD_DD"])
        print(f"\nSymbol: {symbol}")
        for f in symbol_factors:
            factor_display = f["ADJ_FACTOR"] if f["ADJ_FACTOR"] else "NULL (first day)"
            print(f"  {f['TRD_DD']}: {factor_display}")
    
    # Assertions
    assert factor_count == len(snapshots), "Should have one factor per snapshot row"
    assert len(factors) == factor_count, "CSV should match factor count"
    
    # Check first day factors are empty
    first_day_factors = [f for f in factors if f["TRD_DD"] == "20250814"]
    assert all(f["ADJ_FACTOR"] == "" for f in first_day_factors), \
        "First day factors should be empty"
    
    # Check subsequent days have computed factors
    later_day_factors = [f for f in factors if f["TRD_DD"] == "20250818"]
    non_empty_factors = [f for f in later_day_factors if f["ADJ_FACTOR"]]
    assert len(non_empty_factors) > 0, "Should have some computed factors for later days"
    
    # Validate a factor value is a valid decimal string
    if non_empty_factors:
        sample_factor = non_empty_factors[0]["ADJ_FACTOR"]
        from decimal import Decimal
        try:
            factor_val = Decimal(sample_factor)
            print(f"\n=== SAMPLE FACTOR VALIDATION ===")
            print(f"Symbol: {non_empty_factors[0]['ISU_SRT_CD']}")
            print(f"Factor value: {factor_val}")
            assert factor_val > 0, "Factor should be positive"
        except Exception as e:
            pytest.fail(f"Factor should be valid decimal string: {e}")
    
    writer.close()


