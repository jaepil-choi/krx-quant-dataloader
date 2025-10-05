"""
Live smoke tests for Parquet storage layer

⚠️ DEPRECATED: This test file validates the OLD ParquetSnapshotWriter and fragmented
storage structure (snapshots/, adj_factors/, liquidity_ranks/).

The storage layer has been refactored to use:
- Unified pricevolume/ database (single source of truth)
- TempSnapshotWriter for Stage 0 temporary storage
- PriceVolumeWriter for atomic writes with progressive enrichment
- TRD_DD= partitioning (not date=)

This test file is kept to ensure backward compatibility with ParquetSnapshotWriter
but may be removed in future versions.

Test period: 2024-08-20 to 2024-09-20 (short period for manageable testing)

What this tests:
- ParquetSnapshotWriter creates Hive-partitioned structure
- Snapshots written with sorted ISU_SRT_CD (row-group pruning)
- Adjustment factors computed and persisted correctly
- File sizes and compression reasonable
- Data can be read back and verified

Run with: poetry run pytest -v -m live tests/test_storage_parquet_live_smoke.py
"""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path
from datetime import datetime, timedelta

import pytest
import pyarrow.parquet as pq

from krx_quant_dataloader.client import RawClient
from krx_quant_dataloader.config import ConfigFacade
from krx_quant_dataloader.adapter import AdapterRegistry
from krx_quant_dataloader.orchestration import Orchestrator
from krx_quant_dataloader.transport import Transport
from krx_quant_dataloader.storage.writers import ParquetSnapshotWriter
from krx_quant_dataloader.pipelines.snapshots import (
    ingest_change_rates_range,
    compute_and_persist_adj_factors,
)


def generate_date_range(start_date: str, end_date: str) -> list[str]:
    """Generate list of dates in YYYYMMDD format between start and end."""
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    return dates


@pytest.fixture
def temp_parquet_db():
    """Create temporary directory for Parquet database."""
    tmpdir = tempfile.mkdtemp(prefix="krx_parquet_test_")
    yield Path(tmpdir)
    # Cleanup
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def raw_client(test_config_path: str):
    """Create RawClient for live API calls."""
    cfg = ConfigFacade.load(settings_path=test_config_path)
    registry = AdapterRegistry.load(config_path=test_config_path)
    transport = Transport(cfg)
    orchestrator = Orchestrator(transport=transport)
    return RawClient(registry=registry, orchestrator=orchestrator)


@pytest.mark.live
@pytest.mark.slow
def test_parquet_ingestion_short_period(temp_parquet_db, raw_client):
    """
    Live smoke test: Ingest one month of data (2024-08-20 to 2024-09-20) to Parquet.
    
    Validates:
    - Hive partitioning structure created
    - Snapshots written per day
    - Data readable and properly typed
    - File sizes reasonable (compressed)
    """
    print(f"\n{'='*60}")
    print("Parquet Live Smoke Test: 2024-08-20 to 2024-09-20")
    print(f"{'='*60}")
    print(f"DB root: {temp_parquet_db}")
    
    # Generate date range (includes weekends/holidays)
    dates = generate_date_range("20240820", "20240920")
    print(f"\nDate range: {len(dates)} calendar days")
    
    # Create Parquet writer
    writer = ParquetSnapshotWriter(root_path=temp_parquet_db)
    
    # Phase 1: Ingest snapshots
    print(f"\n{'='*60}")
    print("Phase 1: Ingest daily snapshots")
    print(f"{'='*60}")
    
    ingested_counts = ingest_change_rates_range(
        raw_client=raw_client,
        dates=dates,
        market="ALL",
        adjusted_flag=False,
        writer=writer,
    )
    
    # Display ingestion summary
    trading_days = [d for d, count in ingested_counts.items() if count > 0]
    holidays = [d for d, count in ingested_counts.items() if count == 0]
    errors = [d for d, count in ingested_counts.items() if count < 0]
    
    print(f"\nIngestion summary:")
    print(f"  Total days attempted: {len(dates)}")
    print(f"  Trading days: {len(trading_days)}")
    print(f"  Holidays/non-trading: {len(holidays)}")
    print(f"  Errors: {len(errors)}")
    
    if trading_days:
        print(f"\nFirst 3 trading days:")
        for d in trading_days[:3]:
            print(f"  {d}: {ingested_counts[d]} stocks")
    
    if errors:
        print(f"\nErrors on dates: {errors}")
    
    # Verify Hive partitioning structure
    snapshots_dir = temp_parquet_db / "snapshots"
    assert snapshots_dir.exists(), "Snapshots directory not created"
    
    partitions = sorted([p.name for p in snapshots_dir.iterdir() if p.is_dir()])
    print(f"\nHive partitions created: {len(partitions)}")
    if partitions:
        print(f"  First partition: {partitions[0]}")
        print(f"  Last partition: {partitions[-1]}")
    
    # Verify partition naming (TRD_DD=YYYYMMDD)
    for partition_name in partitions:
        assert partition_name.startswith("TRD_DD="), f"Invalid partition name: {partition_name}"
        assert partition_name[7:].isdigit(), f"Invalid date in partition: {partition_name}"
    
    # Read sample partition and verify schema
    if partitions:
        sample_partition = partitions[0]
        sample_path = snapshots_dir / sample_partition / "data.parquet"
        assert sample_path.exists(), f"Parquet file not found in {sample_partition}"
        
        print(f"\nReading sample partition: {sample_partition}")
        table = pq.read_table(sample_path)
        print(f"  Rows: {table.num_rows}")
        print(f"  Columns: {table.num_columns}")
        print(f"  Schema: {table.schema.names[:5]}...")  # First 5 columns
        
        # Verify core columns exist
        assert 'ISU_SRT_CD' in table.schema.names, "ISU_SRT_CD column missing"
        assert 'TDD_CLSPRC' in table.schema.names, "TDD_CLSPRC column missing"
        assert 'ACC_TRDVAL' in table.schema.names, "ACC_TRDVAL column missing"
        
        # Verify data is sorted by ISU_SRT_CD (for row-group pruning)
        symbols = table['ISU_SRT_CD'].to_pylist()
        assert symbols == sorted(symbols), "Data not sorted by ISU_SRT_CD"
        print(f"  ✓ Data sorted by ISU_SRT_CD")
        
        # Display sample data
        print(f"\nSample data (first 3 rows):")
        df = table.to_pandas()
        print(df[['ISU_SRT_CD', 'ISU_ABBRV', 'TDD_CLSPRC', 'ACC_TRDVAL']].head(3))
        
        # Check file size and compression
        file_size = sample_path.stat().st_size
        print(f"\nFile size: {file_size / 1024:.1f} KB")
        assert file_size < 5 * 1024 * 1024, f"File too large: {file_size / 1024 / 1024:.1f} MB (expected < 5 MB)"
    
    # Phase 2: Compute and persist adjustment factors
    print(f"\n{'='*60}")
    print("Phase 2: Compute adjustment factors")
    print(f"{'='*60}")
    
    # Read back all snapshots for factor computation
    all_snapshots = []
    for partition_name in partitions:
        partition_path = snapshots_dir / partition_name / "data.parquet"
        table = pq.read_table(partition_path)
        # Convert to list of dicts for factor computation
        df = table.to_pandas()
        # Extract TRD_DD from partition name
        trade_date = partition_name.split('=')[1]
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            row_dict['TRD_DD'] = trade_date
            all_snapshots.append(row_dict)
    
    print(f"Read {len(all_snapshots)} snapshot rows for factor computation")
    
    # Compute and persist factors
    factor_count = compute_and_persist_adj_factors(all_snapshots, writer)
    print(f"Computed {factor_count} adjustment factors")
    
    # Verify factors written
    factors_dir = temp_parquet_db / "adj_factors"
    assert factors_dir.exists(), "Adj factors directory not created"
    
    factor_partitions = sorted([p.name for p in factors_dir.iterdir() if p.is_dir()])
    print(f"\nFactor partitions created: {len(factor_partitions)}")
    
    # Read sample factor partition
    if factor_partitions:
        sample_factor_partition = factor_partitions[0]
        sample_factor_path = factors_dir / sample_factor_partition / "data.parquet"
        assert sample_factor_path.exists(), f"Factor parquet not found in {sample_factor_partition}"
        
        print(f"\nReading sample factor partition: {sample_factor_partition}")
        factor_table = pq.read_table(sample_factor_path)
        print(f"  Rows: {factor_table.num_rows}")
        print(f"  Schema: {factor_table.schema.names}")
        
        # Display sample factors
        factor_df = factor_table.to_pandas()
        print(f"\nSample adjustment factors (first 5):")
        print(factor_df.head(5))
        
        # Verify most factors are close to 1.0 (no corporate actions)
        non_null_factors = factor_df['adj_factor'].dropna()
        if len(non_null_factors) > 0:
            mean_factor = non_null_factors.mean()
            print(f"\nMean adj_factor (non-null): {mean_factor:.6f}")
            # Most stocks should have factor ~ 1.0 on most days
            factors_near_one = ((non_null_factors > 0.95) & (non_null_factors < 1.05)).sum()
            print(f"Factors near 1.0 (0.95-1.05): {factors_near_one}/{len(non_null_factors)} ({100*factors_near_one/len(non_null_factors):.1f}%)")
    
    print(f"\n{'='*60}")
    print("Live smoke test PASSED")
    print(f"{'='*60}")
    print(f"DB size: {sum(f.stat().st_size for f in temp_parquet_db.rglob('*.parquet')) / 1024 / 1024:.1f} MB")
    print(f"Snapshots partitions: {len(partitions)}")
    print(f"Factor partitions: {len(factor_partitions)}")


@pytest.mark.live
def test_parquet_writer_protocol_compliance(temp_parquet_db):
    """
    Test that ParquetSnapshotWriter implements SnapshotWriter protocol correctly.
    
    Validates:
    - write_snapshot_rows() returns row count
    - write_factor_rows() returns row count
    - close() is no-op (no errors)
    - Empty writes handled gracefully
    """
    writer = ParquetSnapshotWriter(root_path=temp_parquet_db)
    
    # Test empty writes
    assert writer.write_snapshot_rows([]) == 0
    assert writer.write_factor_rows([]) == 0
    
    # Test synthetic write
    snapshot_rows = [
        {
            'TRD_DD': '20240820',
            'ISU_SRT_CD': '005930',
            'ISU_ABBRV': 'Samsung',
            'MKT_NM': 'KOSPI',
            'BAS_PRC': 70000,
            'TDD_CLSPRC': 71000,
            'CMPPREVDD_PRC': 1000,
            'ACC_TRDVOL': 1000000,
            'ACC_TRDVAL': 70000000000,
            'FLUC_RT': '1.43',
            'FLUC_TP': '2',
            'OPNPRC': 70500,
            'HGPRC': 71200,
            'LWPRC': 70000,
            'MKT_ID': 'STK',
        }
    ]
    
    count = writer.write_snapshot_rows(snapshot_rows)
    assert count == 1
    
    # Verify file created
    partition_path = temp_parquet_db / "snapshots" / "TRD_DD=20240820" / "data.parquet"
    assert partition_path.exists()
    
    # Test factor write
    factor_rows = [
        {
            'TRD_DD': '20240820',
            'ISU_SRT_CD': '005930',
            'adj_factor': 1.014285,
        }
    ]
    
    factor_count = writer.write_factor_rows(factor_rows)
    assert factor_count == 1
    
    # Test close (no-op, should not raise)
    writer.close()
    
    print("\n✓ ParquetSnapshotWriter protocol compliance verified")

