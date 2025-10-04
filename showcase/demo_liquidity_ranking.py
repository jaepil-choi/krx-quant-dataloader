"""
Showcase: Liquidity Ranking Pipeline

Demonstrates end-to-end liquidity ranking:
1. Ingest snapshots for a date range (5 trading days)
2. Compute cross-sectional liquidity ranks by ACC_TRDVAL
3. Query and display top 20 most liquid stocks per date

Usage:
    poetry run python showcase/demo_liquidity_ranking.py
"""

from pathlib import Path
import time

from krx_quant_dataloader.config import ConfigFacade
from krx_quant_dataloader.adapter import AdapterRegistry
from krx_quant_dataloader.transport import Transport
from krx_quant_dataloader.orchestration import Orchestrator
from krx_quant_dataloader.client import RawClient
from krx_quant_dataloader.storage.writers import ParquetSnapshotWriter
from krx_quant_dataloader.storage.query import query_parquet_table
from krx_quant_dataloader.pipelines.snapshots import ingest_change_rates_day
from krx_quant_dataloader.pipelines.liquidity_ranking import compute_liquidity_ranks


def main():
    print("=" * 80)
    print("SHOWCASE: Liquidity Ranking Pipeline")
    print("=" * 80)
    
    # =========================================================================
    # SETUP
    # =========================================================================
    print("\n[Setup] Configuration")
    
    db_path = Path("data/krx_db_showcase")
    config_path = "config/config.yaml"
    
    # Recent trading days (adjust dates as needed - these are from September 2024)
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
    
    # =========================================================================
    # STEP 1: Ingest Daily Snapshots
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
    # STEP 2: Compute Liquidity Ranks
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
        fields=['TRD_DD', 'ISU_SRT_CD', 'ISU_ABBRV', 'ACC_TRDVAL', 'TDD_CLSPRC']
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
    # STEP 3: Display Top 20 Most Liquid Stocks
    # =========================================================================
    print("\n" + "=" * 80)
    print("STEP 3: Top 20 Most Liquid Stocks (by Date)")
    print("=" * 80)
    
    for date in sorted(df_ranked['TRD_DD'].unique()):
        print(f"\n{'─' * 80}")
        print(f"Date: {date}")
        print(f"{'─' * 80}")
        
        # Get top 20 for this date
        date_data = df_ranked[df_ranked['TRD_DD'] == date].copy()
        top_20 = date_data.nsmallest(20, 'xs_liquidity_rank')
        
        # Format display
        print(f"\n{'Rank':<6} {'Symbol':<10} {'Name':<30} {'Trading Value (₩)':<20} {'Close':<10}")
        print("─" * 80)
        
        for _, row in top_20.iterrows():
            rank = row['xs_liquidity_rank']
            symbol = row['ISU_SRT_CD']
            name = row['ISU_ABBRV'][:28]  # Truncate long names
            trd_val = f"{row['ACC_TRDVAL']:,}"
            close = f"{row['TDD_CLSPRC']:,}"
            
            print(f"{rank:<6} {symbol:<10} {name:<30} {trd_val:<20} {close:<10}")
    
    # =========================================================================
    # VALIDATION: Check Algorithm Properties
    # =========================================================================
    print("\n" + "=" * 80)
    print("VALIDATION: Algorithm Properties")
    print("=" * 80)
    
    print("\n[Check 1] Dense Ranking (No Gaps)")
    all_valid = True
    for date in sorted(df_ranked['TRD_DD'].unique()):
        date_ranks = sorted(df_ranked[df_ranked['TRD_DD'] == date]['xs_liquidity_rank'].unique())
        max_rank = max(date_ranks)
        expected_ranks = list(range(1, max_rank + 1))
        
        if date_ranks != expected_ranks:
            print(f"  ✗ {date}: Gaps found in ranking")
            all_valid = False
        else:
            print(f"  ✓ {date}: No gaps (1 to {max_rank})")
    
    print("\n[Check 2] Rank 1 = Highest ACC_TRDVAL")
    for date in sorted(df_ranked['TRD_DD'].unique()):
        date_data = df_ranked[df_ranked['TRD_DD'] == date]
        rank_1 = date_data[date_data['xs_liquidity_rank'] == 1]
        max_trdval = date_data['ACC_TRDVAL'].max()
        rank_1_trdval = rank_1['ACC_TRDVAL'].iloc[0]
        
        if rank_1_trdval == max_trdval:
            stock_name = rank_1['ISU_ABBRV'].iloc[0]
            print(f"  ✓ {date}: Rank 1 = {stock_name} (₩{max_trdval:,})")
        else:
            print(f"  ✗ {date}: Rank 1 does NOT have highest ACC_TRDVAL")
            all_valid = False
    
    print("\n[Check 3] Cross-Sectional Independence")
    # Check if at least some stocks have varying ranks across dates
    stock_ranks = df_ranked.groupby('ISU_SRT_CD')['xs_liquidity_rank'].nunique()
    varying_stocks = (stock_ranks > 1).sum()
    total_stocks = len(stock_ranks)
    
    print(f"  Stocks with varying ranks: {varying_stocks}/{total_stocks} ({varying_stocks/total_stocks*100:.1f}%)")
    
    if varying_stocks > 0:
        print(f"  ✓ Cross-sectional independence confirmed")
    else:
        print(f"  ✗ No stocks have varying ranks (unexpected)")
        all_valid = False
    
    # =========================================================================
    # SUMMARY
    # =========================================================================
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    print(f"\n✓ Ingested {total_ingested:,} stock snapshots across {len(dates)} trading days")
    print(f"✓ Computed liquidity ranks for {len(df_ranked):,} stock-date observations")
    print(f"✓ All algorithm validations: {'PASSED' if all_valid else 'FAILED'}")
    
    print(f"\n📁 Data persisted to: {db_path.resolve()}")
    print(f"   └── snapshots/ (Hive-partitioned by TRD_DD)")
    
    print("\n" + "=" * 80)
    print("SHOWCASE COMPLETE")
    print("=" * 80)


if __name__ == '__main__':
    main()

