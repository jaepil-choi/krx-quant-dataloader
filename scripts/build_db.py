#!/usr/bin/env python3
"""
Production script to build Parquet DB from KRX data

Usage:
    poetry run python scripts/build_db.py --start 20240101 --end 20241231 --db ./data/krx_db
    poetry run python scripts/build_db.py --start 20240101 --end 20241231 --db ./data/krx_db --market STK
    poetry run python scripts/build_db.py --days 30 --db ./data/krx_db  # Last 30 days

What it does:
1. Fetches daily snapshots from KRX (market-wide, all stocks)
2. Preprocesses and stores to Hive-partitioned Parquet
3. Computes adjustment factors post-hoc
4. Displays summary and statistics

Resume-safe: Can restart from any date; already-ingested dates are overwritten (idempotent)
"""

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add src to path for direct script execution
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

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

import pyarrow.parquet as pq


def generate_date_range(start_date: str, end_date: str) -> list[str]:
    """Generate list of dates in YYYYMMDD format between start and end (inclusive)."""
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    return dates


def get_recent_dates(num_days: int) -> list[str]:
    """Generate list of dates for the last N days (including today)."""
    end = datetime.now()
    start = end - timedelta(days=num_days - 1)
    return generate_date_range(start.strftime("%Y%m%d"), end.strftime("%Y%m%d"))


def main():
    parser = argparse.ArgumentParser(
        description="Build KRX Parquet database from daily snapshots",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Build DB for full year 2024
  poetry run python scripts/build_db.py --start 20240101 --end 20241231 --db ./data/krx_db

  # Build DB for specific month
  poetry run python scripts/build_db.py --start 20240801 --end 20240831 --db ./data/krx_db

  # Build DB for last 90 days
  poetry run python scripts/build_db.py --days 90 --db ./data/krx_db

  # Build DB with specific market (STK=KOSPI, KSQ=KOSDAQ, KNX=KONEX, ALL=all markets)
  poetry run python scripts/build_db.py --start 20240101 --end 20241231 --db ./data/krx_db --market STK

  # Use custom config file
  poetry run python scripts/build_db.py --start 20240101 --end 20241231 --db ./data/krx_db --config ./my_config.yaml
        """,
    )
    
    # Date range arguments
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument(
        "--days",
        type=int,
        help="Number of recent days to fetch (including today)",
    )
    date_group.add_argument(
        "--start",
        type=str,
        help="Start date in YYYYMMDD format (requires --end)",
    )
    
    parser.add_argument(
        "--end",
        type=str,
        help="End date in YYYYMMDD format (requires --start)",
    )
    
    # Database path
    parser.add_argument(
        "--db",
        type=str,
        required=True,
        help="Root path for Parquet database (e.g., ./data/krx_db)",
    )
    
    # Optional parameters
    parser.add_argument(
        "--market",
        type=str,
        default="ALL",
        choices=["ALL", "STK", "KSQ", "KNX"],
        help="Market ID (ALL=all markets, STK=KOSPI, KSQ=KOSDAQ, KNX=KONEX). Default: ALL",
    )
    
    parser.add_argument(
        "--adjusted",
        action="store_true",
        help="Request adjusted prices from KRX (adjStkPrc=2 vs 1). Default: False (raw prices)",
    )
    
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to config YAML file. Default: tests/fixtures/test_config.yaml",
    )
    
    parser.add_argument(
        "--skip-factors",
        action="store_true",
        help="Skip adjustment factor computation (only ingest snapshots). Default: False",
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.start and not args.end:
        parser.error("--start requires --end")
    if args.end and not args.start:
        parser.error("--end requires --start")
    
    # Generate date range
    if args.days:
        dates = get_recent_dates(args.days)
        print(f"Fetching last {args.days} days: {dates[0]} to {dates[-1]}")
    else:
        dates = generate_date_range(args.start, args.end)
        print(f"Fetching date range: {args.start} to {args.end}")
    
    print(f"Total calendar days: {len(dates)}")
    print(f"Market: {args.market}")
    print(f"Adjusted prices: {args.adjusted}")
    print(f"DB path: {args.db}")
    print()
    
    # Initialize components
    if args.config:
        config_path = args.config
    else:
        # Try production config first, fall back to test config
        prod_config = Path(__file__).parent.parent / "config" / "config.yaml"
        test_config = Path(__file__).parent.parent / "tests" / "fixtures" / "test_config.yaml"
        
        if prod_config.exists():
            config_path = str(prod_config)
        else:
            config_path = str(test_config)
            print("⚠️  WARNING: Using test config (production config not found)")
            print(f"   Create production config at: {prod_config}")
    
    print(f"Loading config from: {config_path}")
    
    cfg = ConfigFacade.load(config_path=config_path)
    registry = AdapterRegistry.load(config_path=config_path)
    transport = Transport(cfg)
    orchestrator = Orchestrator(transport=transport)
    raw_client = RawClient(registry=registry, orchestrator=orchestrator)
    
    # Create Parquet writer
    db_path = Path(args.db)
    writer = ParquetSnapshotWriter(root_path=db_path)
    print(f"Initialized ParquetSnapshotWriter at: {db_path.absolute()}")
    print()
    
    # Phase 1: Ingest snapshots
    print("=" * 70)
    print("PHASE 1: Ingest daily snapshots (resume-safe)")
    print("=" * 70)
    print()
    
    ingested_counts = ingest_change_rates_range(
        raw_client=raw_client,
        dates=dates,
        market=args.market,
        adjusted_flag=args.adjusted,
        writer=writer,
    )
    
    # Analyze results
    trading_days = [(d, count) for d, count in ingested_counts.items() if count > 0]
    holidays = [d for d, count in ingested_counts.items() if count == 0]
    errors = [(d, count) for d, count in ingested_counts.items() if count < 0]
    
    print()
    print("=" * 70)
    print("INGESTION SUMMARY")
    print("=" * 70)
    print(f"Total days attempted:  {len(dates)}")
    print(f"Trading days:          {len(trading_days)}")
    print(f"Holidays/non-trading:  {len(holidays)}")
    print(f"Errors:                {len(errors)}")
    print()
    
    if trading_days:
        total_rows = sum(count for _, count in trading_days)
        avg_stocks_per_day = total_rows / len(trading_days)
        print(f"Total rows ingested:   {total_rows:,}")
        print(f"Avg stocks per day:    {avg_stocks_per_day:.0f}")
        print()
        print("First 5 trading days:")
        for d, count in trading_days[:5]:
            print(f"  {d}: {count:>4,} stocks")
        if len(trading_days) > 5:
            print("  ...")
            for d, count in trading_days[-2:]:
                print(f"  {d}: {count:>4,} stocks")
    
    if holidays:
        print()
        print(f"Holidays/non-trading days ({len(holidays)}):")
        for d in holidays[:10]:
            print(f"  {d}")
        if len(holidays) > 10:
            print(f"  ... and {len(holidays) - 10} more")
    
    if errors:
        print()
        print("ERRORS encountered on dates:")
        for d, count in errors:
            print(f"  {d}: error code {count}")
    
    # Verify Hive partitioning
    snapshots_dir = db_path / "snapshots"
    if snapshots_dir.exists():
        partitions = sorted([p.name for p in snapshots_dir.iterdir() if p.is_dir()])
        print()
        print(f"Hive partitions created: {len(partitions)}")
        if partitions:
            print(f"  First: {partitions[0]}")
            print(f"  Last:  {partitions[-1]}")
            
            # Calculate DB size
            total_size = sum(f.stat().st_size for f in snapshots_dir.rglob("*.parquet"))
            print(f"  Total size: {total_size / 1024 / 1024:.2f} MB")
    
    # Phase 2: Compute adjustment factors
    if not args.skip_factors and trading_days:
        print()
        print("=" * 70)
        print("PHASE 2: Compute adjustment factors (post-hoc)")
        print("=" * 70)
        print()
        print("Reading back snapshots for factor computation...")
        
        # Read all snapshots
        all_snapshots = []
        for partition_name in partitions:
            partition_path = snapshots_dir / partition_name / "data.parquet"
            if not partition_path.exists():
                continue
            
            table = pq.read_table(partition_path)
            df = table.to_pandas()
            
            # Extract TRD_DD from partition name
            trade_date = partition_name.split('=')[1]
            for _, row in df.iterrows():
                row_dict = row.to_dict()
                row_dict['TRD_DD'] = trade_date
                all_snapshots.append(row_dict)
        
        print(f"Read {len(all_snapshots):,} snapshot rows")
        
        # Compute and persist factors
        factor_count = compute_and_persist_adj_factors(all_snapshots, writer)
        print(f"Computed and persisted {factor_count:,} adjustment factors")
        
        # Verify factors
        factors_dir = db_path / "adj_factors"
        if factors_dir.exists():
            factor_partitions = sorted([p.name for p in factors_dir.iterdir() if p.is_dir()])
            factor_size = sum(f.stat().st_size for f in factors_dir.rglob("*.parquet"))
            print(f"Factor partitions: {len(factor_partitions)}")
            print(f"Factor DB size: {factor_size / 1024 / 1024:.2f} MB")
    elif args.skip_factors:
        print()
        print("⚠️  Skipped adjustment factor computation (--skip-factors)")
    
    # Final summary
    print()
    print("=" * 70)
    print("BUILD COMPLETE")
    print("=" * 70)
    print(f"Database root: {db_path.absolute()}")
    print()
    print("Directory structure:")
    print(f"  {db_path}/")
    print(f"    snapshots/")
    print(f"      TRD_DD=YYYYMMDD/data.parquet")
    if not args.skip_factors:
        print(f"    adj_factors/")
        print(f"      TRD_DD=YYYYMMDD/data.parquet")
    print()
    
    # Calculate total DB size
    total_db_size = sum(f.stat().st_size for f in db_path.rglob("*.parquet"))
    print(f"Total DB size: {total_db_size / 1024 / 1024:.2f} MB")
    print()
    print("✓ Database ready for queries")
    
    writer.close()


if __name__ == "__main__":
    main()

