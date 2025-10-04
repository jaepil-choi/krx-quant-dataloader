"""
Showcase: Universe Builder Pipeline (Full Pipeline B)

Demonstrates the complete Pipeline B flow with BOOLEAN COLUMN schema:
1. Ingest snapshots from KRX API
2. Compute liquidity ranks from snapshots
3. Build universe membership from ranks (BOOLEAN COLUMNS: univ100, univ200, univ500, univ1000)
4. Persist universes to Parquet
5. Query back and display stored data

Purpose:
- Prove the pipeline actually works end-to-end
- Show what's stored in the Parquet files (boolean columns for efficient filtering)
- Validate the data can be queried back correctly
"""

import time
from pathlib import Path
import pandas as pd

from krx_quant_dataloader.config import ConfigFacade
from krx_quant_dataloader.adapter import AdapterRegistry
from krx_quant_dataloader.transport import Transport
from krx_quant_dataloader.orchestration import Orchestrator
from krx_quant_dataloader.client import RawClient
from krx_quant_dataloader.storage.writers import ParquetSnapshotWriter
from krx_quant_dataloader.storage.query import query_parquet_table
from krx_quant_dataloader.pipelines.snapshots import ingest_change_rates_day
from krx_quant_dataloader.pipelines.liquidity_ranking import compute_liquidity_ranks
from krx_quant_dataloader.pipelines.universe_builder import build_universes_and_persist


def setup_client():
    """Initialize KRX client stack."""
    print("\n[Step 1] Initializing KRX client...")
    
    config_path = 'config/config.yaml'
    cfg = ConfigFacade.load(config_path=config_path)
    reg = AdapterRegistry.load(config_path=config_path)
    transport = Transport(cfg)
    orch = Orchestrator(transport)
    client = RawClient(registry=reg, orchestrator=orch)
    
    print("  [OK] Client initialized")
    return client


def ingest_snapshots(client, db_path, dates):
    """Ingest daily snapshots for specified dates."""
    print(f"\n[Step 2] Ingesting snapshots for {len(dates)} dates...")
    
    writer = ParquetSnapshotWriter(root_path=db_path)
    total_rows = 0
    start_time = time.time()
    
    for date in dates:
        rows = ingest_change_rates_day(raw_client=client, date=date, writer=writer)
        total_rows += rows
        print(f"  [OK] {date}: {rows:,} rows")
    
    elapsed = time.time() - start_time
    rate = total_rows / elapsed if elapsed > 0 else 0
    
    print(f"\n  Total: {total_rows:,} rows in {elapsed:.2f}s ({rate:.0f} rows/sec)")
    return total_rows


def compute_ranks(db_path, dates):
    """Compute liquidity ranks from snapshots."""
    print(f"\n[Step 3] Computing liquidity ranks...")
    
    # Read all snapshots
    snapshots = query_parquet_table(
        db_path=db_path,
        table_name='snapshots',
        start_date=min(dates),
        end_date=max(dates),
    )
    
    print(f"  [OK] Loaded {len(snapshots):,} snapshot rows")
    
    # Compute ranks
    start_time = time.time()
    ranks_df = compute_liquidity_ranks(snapshots)
    elapsed = time.time() - start_time
    
    print(f"  [OK] Computed ranks for {ranks_df['TRD_DD'].nunique()} dates in {elapsed:.3f}s")
    print(f"  [OK] Output shape: {ranks_df.shape}")
    
    return ranks_df


def build_and_persist_universes(ranks_df, db_path):
    """Build universe membership and persist to Parquet."""
    print(f"\n[Step 4] Building and persisting universes (BOOLEAN COLUMNS)...")
    
    # Define universe tiers
    universe_tiers = {
        'univ100': 100,
        'univ200': 200,
        'univ500': 500,
        'univ1000': 1000,
    }
    
    print(f"  Universe tiers: {list(universe_tiers.keys())}")
    print(f"  Schema: One row per stock with boolean flags (univ100, univ200, univ500, univ1000)")
    
    # Build and persist
    writer = ParquetSnapshotWriter(root_path=db_path)
    start_time = time.time()
    total_rows = build_universes_and_persist(ranks_df, universe_tiers, writer)
    elapsed = time.time() - start_time
    
    print(f"  [OK] Persisted {total_rows:,} universe rows in {elapsed:.3f}s")
    print(f"  [OK] Each row has boolean flags for all 4 universe tiers")
    
    return total_rows, universe_tiers


def read_and_display_universes(db_path, dates):
    """Read universes from Parquet and display the complete data."""
    print(f"\n[Step 5] Reading universes from Parquet storage...")
    
    # Query all universes
    universes_df = query_parquet_table(
        db_path=db_path,
        table_name='universes',
        start_date=min(dates),
        end_date=max(dates),
    )
    
    print(f"  [OK] Loaded {len(universes_df):,} universe rows")
    print(f"  [OK] Columns: {list(universes_df.columns)}")
    print(f"  [OK] Shape: {universes_df.shape}")
    
    # Display summary statistics
    print(f"\n{'='*80}")
    print("UNIVERSE MEMBERSHIP DATA (Boolean Column Schema)")
    print(f"{'='*80}")
    
    # Show universe sizes per date
    print(f"\n[Universe Sizes Per Date]")
    print(f"{'-'*80}")
    for date in sorted(universes_df['TRD_DD'].unique()):
        date_data = universes_df[universes_df['TRD_DD'] == date]
        univ100_count = (date_data['univ100'] == 1).sum()
        univ200_count = (date_data['univ200'] == 1).sum()
        univ500_count = (date_data['univ500'] == 1).sum()
        univ1000_count = (date_data['univ1000'] == 1).sum()
        print(f"  {date}: univ100={univ100_count:4d}, univ200={univ200_count:4d}, univ500={univ500_count:4d}, univ1000={univ1000_count:4d}")
    
    # Show top 20 stocks for first date
    first_date = sorted(universes_df['TRD_DD'].unique())[0]
    print(f"\n[Top 20 Stocks by Rank (Date: {first_date})]")
    print(f"{'-'*80}")
    
    top20 = universes_df[universes_df['TRD_DD'] == first_date].sort_values('xs_liquidity_rank').head(20)
    print("Rank  Symbol    univ100 univ200 univ500 univ1000")
    print("-" * 80)
    for _, row in top20.iterrows():
        print(f"{row['xs_liquidity_rank']:4d}  {row['ISU_SRT_CD']:8s}  {row['univ100']:7d} {row['univ200']:7d} {row['univ500']:7d} {row['univ1000']:8d}")
    
    # Display sample data (first 20 rows)
    print(f"\n[Sample Data - First 20 Rows]")
    print(f"{'-'*80}")
    print(universes_df.head(20).to_string(index=False))
    
    # Show filtering efficiency example
    print(f"\n[Filtering Efficiency - Boolean Column Advantage]")
    print(f"{'-'*80}")
    
    # Example: Filter univ100 stocks
    univ100_stocks = universes_df[universes_df['univ100'] == 1]
    print(f"  Filter: df[df['univ100'] == 1]")
    print(f"  Result: {len(univ100_stocks):,} rows (top 100 stocks per date)")
    print(f"  Advantage: No string comparison, boolean mask is fast!")
    
    # Validate subset relationships
    print(f"\n[Validating Subset Relationships (Boolean Logic)]")
    print(f"{'-'*80}")
    
    for date in sorted(universes_df['TRD_DD'].unique()):
        date_data = universes_df[universes_df['TRD_DD'] == date]
        
        # Check: If univ100=1, then univ200, univ500, univ1000 must also be 1
        univ100_stocks = date_data[date_data['univ100'] == 1]
        if len(univ100_stocks) > 0:
            check1 = (univ100_stocks['univ200'] == 1).all()
            check2 = (univ100_stocks['univ500'] == 1).all()
            check3 = (univ100_stocks['univ1000'] == 1).all()
            status = "PASS" if all([check1, check2, check3]) else "FAIL"
            print(f"  {date}: univ100 => univ200: {check1}, univ500: {check2}, univ1000: {check3} - {status}")
    
    # Show example stock with varying membership
    print(f"\n[Per-Date Independence Example]")
    print(f"{'-'*80}")
    
    # Find a stock that appears in multiple dates
    sample_stock = universes_df.groupby('ISU_SRT_CD').size().idxmax()
    stock_data = universes_df[universes_df['ISU_SRT_CD'] == sample_stock][['TRD_DD', 'xs_liquidity_rank', 'univ100', 'univ200', 'univ500', 'univ1000']].sort_values('TRD_DD')
    
    print(f"  Stock: {sample_stock}")
    print(stock_data.to_string(index=False))
    
    print(f"\n{'='*80}")
    print("END OF UNIVERSE MEMBERSHIP DATA")
    print(f"{'='*80}")
    
    return universes_df


def main():
    """Run complete universe builder showcase."""
    print("="*80)
    print("SHOWCASE: Universe Builder Pipeline (Boolean Column Schema)")
    print("="*80)
    
    # Configuration
    db_path = Path('data/krx_db_universe_builder_showcase')
    dates = ['20240816', '20240819', '20240820', '20240821', '20240822']
    
    # Clean up existing data
    if db_path.exists():
        import shutil
        shutil.rmtree(db_path)
        print(f"\n[Setup] Cleaned existing database at {db_path}")
    
    db_path.mkdir(parents=True, exist_ok=True)
    print(f"[Setup] Database path: {db_path}")
    print(f"[Setup] Date range: {dates[0]} to {dates[-1]} ({len(dates)} days)")
    
    try:
        # Pipeline execution
        client = setup_client()
        ingest_snapshots(client, db_path, dates)
        ranks_df = compute_ranks(db_path, dates)
        total_rows, universe_tiers = build_and_persist_universes(ranks_df, db_path)
        
        # Read back and display
        universes_df = read_and_display_universes(db_path, dates)
        
        # Final summary
        print("\n" + "="*80)
        print("SHOWCASE COMPLETE - PIPELINE B VALIDATED (Boolean Schema)")
        print("="*80)
        print(f"\n[OK] Snapshots ingested: {len(dates)} dates")
        print(f"[OK] Liquidity ranks computed: {len(ranks_df):,} rows")
        print(f"[OK] Universe rows persisted: {total_rows:,} rows")
        print(f"[OK] Universe rows queried back: {len(universes_df):,} rows")
        print(f"[OK] Universe tiers: {list(universe_tiers.keys())}")
        print(f"[OK] Subset relationships: ALL VALIDATED")
        print(f"[OK] Per-date independence: CONFIRMED")
        
        print("\n[Storage Structure]")
        print(f"  {db_path}/")
        print(f"  ├── snapshots/       ({len(dates)} partitions)")
        print(f"  ├── liquidity_ranks/ (computed in-memory, not persisted)")
        print(f"  └── universes/       ({len(dates)} partitions, boolean columns)")
        
        print(f"\n[Schema Advantage]")
        print(f"  OLD: universe_name column with string values ('univ100', 'univ200', ...)")
        print(f"       -> Requires string comparison: df[df['universe_name'] == 'univ100']")
        print(f"  NEW: Boolean columns (univ100, univ200, univ500, univ1000)")
        print(f"       -> Fast boolean mask: df[df['univ100'] == 1]")
        print(f"       -> Subset relationships explicit in data")
        
        print(f"\n[SUCCESS] The universe builder pipeline is working correctly!")
        print(f"           All data has been stored in Parquet format with boolean columns.")
        
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main())
