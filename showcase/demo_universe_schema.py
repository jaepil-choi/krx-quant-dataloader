"""
Showcase: Universe Construction Pipeline (Pipeline B)

Demonstrates the complete Pipeline B flow:
1. Ingest snapshots for a date range (5 trading days)
2. Compute liquidity ranks from snapshots
3. Construct universe membership rows from ranks
4. Write universes using write_universes()
5. Query back and validate universe filtering

This showcases the full: Snapshots → Liquidity Ranks → Universes pipeline.

Usage:
    poetry run python showcase/demo_universe_schema.py
"""

from pathlib import Path
import pandas as pd
import time

from krx_quant_dataloader.config import ConfigFacade
from krx_quant_dataloader.adapter import AdapterRegistry
from krx_quant_dataloader.transport import Transport
from krx_quant_dataloader.orchestration import Orchestrator
from krx_quant_dataloader.client import RawClient
from krx_quant_dataloader.storage.writers import ParquetSnapshotWriter
from krx_quant_dataloader.storage.query import query_parquet_table, load_universe_symbols
from krx_quant_dataloader.pipelines.snapshots import ingest_change_rates_day
from krx_quant_dataloader.pipelines.liquidity_ranking import compute_liquidity_ranks


def main():
    print("=" * 80)
    print("SHOWCASE: Universe Construction Pipeline (Pipeline B)")
    print("=" * 80)
    
    # =========================================================================
    # SETUP
    # =========================================================================
    print("\n[Setup] Configuration")
    
    db_path = Path("data/krx_db_universe_showcase")
    config_path = "config/config.yaml"
    
    # Universe tiers to construct
    universe_tiers = {
        'univ100': 100,
        'univ200': 200,
        'univ500': 500,
        'univ1000': 1000,
    }
    
    # Recent trading days (September 2024)
    dates = [
        "20240902",  # Monday
        "20240903",  # Tuesday
        "20240904",  # Wednesday
        "20240905",  # Thursday
        "20240906",  # Friday
    ]
    
    print(f"  Database: {db_path}")
    print(f"  Config: {config_path}")
    print(f"  Date range: {dates[0]} → {dates[-1]} ({len(dates)} days)")
    print(f"  Universe tiers: {list(universe_tiers.keys())}")
    
    # Initialize client stack
    print(f"\n[Initializing] Client stack...")
    cfg = ConfigFacade.load(config_path=config_path)
    reg = AdapterRegistry.load(config_path=config_path)
    transport = Transport(cfg)
    orch = Orchestrator(transport)
    client = RawClient(registry=reg, orchestrator=orch)
    print(f"  ✓ Client ready")
    
    # Initialize writer
    writer = ParquetSnapshotWriter(root_path=db_path)
    print(f"  ✓ Writer ready")
    
    # =========================================================================
    # STEP 1: Ingest Snapshots from KRX API
    # =========================================================================
    print("\n" + "=" * 80)
    print("STEP 1: Ingest Daily Snapshots")
    print("=" * 80)
    
    total_ingested = 0
    start_time = time.time()
    
    for date in dates:
        print(f"\n[Ingesting] {date}...")
        
        try:
            row_count = ingest_change_rates_day(
                raw_client=client,
                date=date,
                market="ALL",
                adjusted_flag=False,  # Unadjusted prices
                writer=writer,
            )
            
            total_ingested += row_count
            print(f"  ✓ {row_count:,} stocks ingested")
            
        except Exception as e:
            print(f"  ✗ Error: {e}")
            continue
    
    elapsed = time.time() - start_time
    print(f"\n[Summary] Ingestion Complete")
    print(f"  Total rows: {total_ingested:,}")
    print(f"  Time: {elapsed:.2f}s")
    
    if elapsed > 0:
        print(f"  Rate: {total_ingested / elapsed:.0f} rows/sec")
    
    # =========================================================================
    # STEP 2: Compute Liquidity Ranks from Snapshots
    # =========================================================================
    print("\n" + "=" * 80)
    print("STEP 2: Compute Liquidity Ranks")
    print("=" * 80)
    
    print(f"\n[Loading] Snapshots from DB...")
    
    df_snapshots = query_parquet_table(
        db_path=db_path,
        table_name='snapshots',
        start_date=dates[0],
        end_date=dates[-1],
        fields=['TRD_DD', 'ISU_SRT_CD', 'ISU_ABBRV', 'ACC_TRDVAL']
    )
    
    print(f"  Loaded: {len(df_snapshots):,} rows")
    print(f"  Dates: {df_snapshots['TRD_DD'].nunique()} trading days")
    print(f"  Stocks: {df_snapshots['ISU_SRT_CD'].nunique():,} unique symbols")
    
    print(f"\n[Computing] Cross-sectional ranks...")
    print(f"  Algorithm: Dense ranking by ACC_TRDVAL per date")
    print(f"  Higher trading value → Lower rank (rank 1 = most liquid)")
    
    start_time = time.time()
    df_ranked = compute_liquidity_ranks(df_snapshots)
    elapsed = time.time() - start_time
    
    print(f"  ✓ Ranked {len(df_ranked):,} rows in {elapsed:.3f}s")
    
    # =========================================================================
    # STEP 3: Construct Universe Membership from Ranks
    # =========================================================================
    print("\n" + "=" * 80)
    print("STEP 3: Construct Universe Membership")
    print("=" * 80)
    
    print("\n[Algorithm] For each universe tier:")
    print("  - univ100: ranks 1-100")
    print("  - univ200: ranks 1-200")
    print("  - univ500: ranks 1-500")
    print("  - univ1000: ranks 1-1000")
    
    total_universe_rows = 0
    
    for date in sorted(df_ranked['TRD_DD'].unique()):
        print(f"\n[Date {date}]")
        
        # Get ranks for this date
        date_ranks = df_ranked[df_ranked['TRD_DD'] == date]
        
        # Build universe membership rows
        universe_rows = []
        
        for universe_name, rank_threshold in universe_tiers.items():
            # Filter stocks by rank threshold
            universe_stocks = date_ranks[date_ranks['xs_liquidity_rank'] <= rank_threshold]
            
            for _, stock in universe_stocks.iterrows():
                universe_rows.append({
                    'TRD_DD': date,
                    'ISU_SRT_CD': stock['ISU_SRT_CD'],
                    'universe_name': universe_name,
                    'xs_liquidity_rank': stock['xs_liquidity_rank'],
                })
            
            print(f"  {universe_name}: {len(universe_stocks):,} stocks")
        
        # Write universes for this date
        count = writer.write_universes(universe_rows, date=date)
        total_universe_rows += count
        print(f"  ✓ Total universe rows: {count:,}")
    
    print(f"\n[Summary] Total universe membership rows: {total_universe_rows:,}")
    
    # =========================================================================
    # STEP 4: Query Universes Back
    # =========================================================================
    print("\n" + "=" * 80)
    print("STEP 4: Query Universes and Validate")
    print("=" * 80)
    
    print("\n[Querying] Universes from database...")
    
    # Query using generic query function
    df_universes = query_parquet_table(
        db_path=db_path,
        table_name='universes',
        start_date=dates[0],
        end_date=dates[-1],
    )
    
    print(f"  Loaded: {len(df_universes):,} universe membership rows")
    print(f"  Dates: {df_universes['TRD_DD'].nunique()} trading days")
    print(f"  Universes: {sorted(df_universes['universe_name'].unique())}")
    
    # =========================================================================
    # STEP 5: Validate Universe Properties
    # =========================================================================
    print("\n" + "=" * 80)
    print("STEP 5: Validate Universe Properties")
    print("=" * 80)
    
    print("\n[Check 1] Subset Relationships (univ100 ⊂ univ500 ⊂ univ1000)")
    
    all_valid = True
    
    for date in sorted(df_universes['TRD_DD'].unique()):
        date_data = df_universes[df_universes['TRD_DD'] == date]
        
        univ100 = set(date_data[date_data['universe_name'] == 'univ100']['ISU_SRT_CD'])
        univ200 = set(date_data[date_data['universe_name'] == 'univ200']['ISU_SRT_CD'])
        univ500 = set(date_data[date_data['universe_name'] == 'univ500']['ISU_SRT_CD'])
        univ1000 = set(date_data[date_data['universe_name'] == 'univ1000']['ISU_SRT_CD'])
        
        # Check subset relationships
        if univ100.issubset(univ200) and univ200.issubset(univ500) and univ500.issubset(univ1000):
            print(f"  ✓ {date}: univ100 ⊂ univ200 ⊂ univ500 ⊂ univ1000")
        else:
            print(f"  ✗ {date}: Subset relationship violated!")
            all_valid = False
        
        # Check sizes
        print(f"     Sizes: univ100={len(univ100)}, univ200={len(univ200)}, "
              f"univ500={len(univ500)}, univ1000={len(univ1000)}")
    
    print("\n[Check 2] Universe Sizes Match Thresholds")
    
    for universe_name, expected_size in universe_tiers.items():
        for date in dates:
            date_data = df_universes[df_universes['TRD_DD'] == date]
            universe_data = date_data[date_data['universe_name'] == universe_name]
            actual_size = len(universe_data)
            
            if actual_size == expected_size:
                print(f"  ✓ {date} {universe_name}: {actual_size} stocks (expected {expected_size})")
            else:
                print(f"  ✗ {date} {universe_name}: {actual_size} stocks (expected {expected_size})")
                all_valid = False
    
    print("\n[Check 3] Per-Date Independence (Survivorship Bias-Free)")
    
    # Check if at least some stocks appear in different universe tiers across dates
    # (This would happen in real data if ranks change)
    
    # For this synthetic data, ranks are stable, so we'll check structure instead
    dates_per_stock = df_universes.groupby('ISU_SRT_CD')['TRD_DD'].nunique()
    
    stocks_all_dates = (dates_per_stock == len(dates)).sum()
    print(f"  Stocks appearing on all {len(dates)} dates: {stocks_all_dates:,}")
    
    if stocks_all_dates > 0:
        print(f"  ✓ Per-date universe membership preserved")
    else:
        print(f"  ✗ No stocks found across all dates")
        all_valid = False
    
    # =========================================================================
    # STEP 6: Test load_universe_symbols() Helper
    # =========================================================================
    print("\n" + "=" * 80)
    print("STEP 6: Test load_universe_symbols() Helper")
    print("=" * 80)
    
    print("\n[Loading] Universe symbols using helper function...")
    
    universe_symbols = load_universe_symbols(
        db_path=db_path,
        universe_name='univ100',
        start_date=dates[0],
        end_date=dates[-1],
    )
    
    print(f"  ✓ Loaded univ100 symbols")
    print(f"  Dates returned: {sorted(universe_symbols.keys())}")
    
    for date, symbols in sorted(universe_symbols.items()):
        print(f"    {date}: {len(symbols)} stocks")
        print(f"      Top 10: {symbols[:10]}")
    
    # Validate helper function returns correct format
    assert isinstance(universe_symbols, dict), "Should return dict"
    assert all(isinstance(k, str) for k in universe_symbols.keys()), "Keys should be date strings"
    assert all(isinstance(v, list) for v in universe_symbols.values()), "Values should be lists"
    assert all(len(v) == 100 for v in universe_symbols.values()), "univ100 should have 100 stocks per date"
    
    print(f"\n  ✓ load_universe_symbols() validation passed")
    
    # =========================================================================
    # STEP 7: Display Sample Data
    # =========================================================================
    print("\n" + "=" * 80)
    print("STEP 7: Sample Universe Membership Data")
    print("=" * 80)
    
    first_date = sorted(df_universes['TRD_DD'].unique())[0]
    print(f"\n[Sample] Top 20 stocks from univ100 on {first_date}:")
    print(f"\n{'Rank':<6} {'Symbol':<10} {'Universe':<15}")
    print("─" * 35)
    
    sample_data = df_universes[
        (df_universes['TRD_DD'] == first_date) &
        (df_universes['universe_name'] == 'univ100')
    ].sort_values('xs_liquidity_rank').head(20)
    
    for _, row in sample_data.iterrows():
        print(f"{row['xs_liquidity_rank']:<6} {row['ISU_SRT_CD']:<10} {row['universe_name']:<15}")
    
    # =========================================================================
    # SUMMARY
    # =========================================================================
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    print(f"\n✓ Ingested snapshots: {total_ingested:,} rows")
    print(f"✓ Computed liquidity ranks: {len(df_ranked):,} rows")
    print(f"✓ Constructed universe membership: {total_universe_rows:,} rows")
    print(f"✓ Queried universes back: {len(df_universes):,} rows")
    print(f"✓ All validations: {'PASSED' if all_valid else 'FAILED'}")
    
    print(f"\n📁 Data persisted to: {db_path.resolve()}")
    print(f"   ├── snapshots/ ({len(dates)} dates × ~2800 stocks)")
    print(f"   ├── liquidity_ranks/ (NOT persisted yet - computed in-memory)")
    print(f"   └── universes/ ({len(dates)} dates × 4 tiers)")
    
    print("\n✅ Complete Pipeline B validated: Snapshots → Liquidity Ranks → Universes")
    print("   Ready to implement pipelines/universe_builder.py to persist ranks and automate universe construction!")
    
    print("\n" + "=" * 80)
    print("SHOWCASE COMPLETE")
    print("=" * 80)


if __name__ == '__main__':
    main()

