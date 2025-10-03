#!/usr/bin/env python3
"""
Inspect and explore Parquet database

Usage:
    poetry run python scripts/inspect_db.py --db ./data/krx_db
    poetry run python scripts/inspect_db.py --db ./data/krx_db --table snapshots --date 20241101
    poetry run python scripts/inspect_db.py --db ./data/krx_db --table adj_factors --show-sample 10

What it does:
1. Lists all partitions in the database
2. Shows table schemas and metadata
3. Displays sample data for inspection
4. Shows statistics (row counts, value ranges, nulls)
5. Validates data integrity

Use this to verify your data looks correct!
"""

import argparse
import sys
from pathlib import Path
from typing import Optional

import pyarrow.parquet as pq
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 50)


def list_partitions(db_path: Path, table_name: str) -> list[str]:
    """List all TRD_DD partitions in a table."""
    table_path = db_path / table_name
    if not table_path.exists():
        return []
    
    partitions = sorted([
        p.name for p in table_path.iterdir() 
        if p.is_dir() and p.name.startswith("TRD_DD=")
    ])
    return partitions


def get_partition_date(partition_name: str) -> str:
    """Extract date from partition name (TRD_DD=20241101 -> 20241101)."""
    return partition_name.split('=')[1]


def read_partition(db_path: Path, table_name: str, partition_name: str) -> pd.DataFrame:
    """Read a specific partition into DataFrame."""
    partition_path = db_path / table_name / partition_name / "data.parquet"
    if not partition_path.exists():
        raise FileNotFoundError(f"Partition file not found: {partition_path}")
    
    table = pq.read_table(partition_path)
    df = table.to_pandas()
    
    # Add TRD_DD column from partition name
    df['TRD_DD'] = get_partition_date(partition_name)
    
    # Reorder columns to put TRD_DD first
    cols = ['TRD_DD'] + [col for col in df.columns if col != 'TRD_DD']
    df = df[cols]
    
    return df


def show_table_summary(db_path: Path, table_name: str):
    """Show summary statistics for a table."""
    partitions = list_partitions(db_path, table_name)
    
    if not partitions:
        print(f"❌ No partitions found for table '{table_name}'")
        return
    
    print(f"\n{'='*80}")
    print(f"TABLE: {table_name}")
    print(f"{'='*80}")
    print(f"Total partitions: {len(partitions)}")
    print(f"Date range: {get_partition_date(partitions[0])} to {get_partition_date(partitions[-1])}")
    
    # Read first partition to show schema
    first_df = read_partition(db_path, table_name, partitions[0])
    
    print(f"\n{'='*80}")
    print("SCHEMA")
    print(f"{'='*80}")
    print(f"Columns: {len(first_df.columns)}")
    print(f"\n{'Column':<20} {'Type':<20} {'Sample Value'}")
    print("-" * 80)
    for col in first_df.columns:
        dtype = str(first_df[col].dtype)
        sample = first_df[col].iloc[0] if len(first_df) > 0 else "N/A"
        # Truncate long values
        sample_str = str(sample)[:40]
        print(f"{col:<20} {dtype:<20} {sample_str}")
    
    # Calculate total rows across all partitions
    print(f"\n{'='*80}")
    print("PARTITION SUMMARY")
    print(f"{'='*80}")
    print(f"{'Date':<12} {'Rows':<10} {'File Size'}")
    print("-" * 80)
    
    total_rows = 0
    total_size = 0
    
    for partition in partitions[:10]:  # Show first 10
        df = read_partition(db_path, table_name, partition)
        date = get_partition_date(partition)
        rows = len(df)
        
        # Get file size
        file_path = db_path / table_name / partition / "data.parquet"
        file_size = file_path.stat().st_size
        
        total_rows += rows
        total_size += file_size
        
        print(f"{date:<12} {rows:<10,} {file_size / 1024:.1f} KB")
    
    if len(partitions) > 10:
        print(f"... and {len(partitions) - 10} more partitions")
        
        # Calculate remaining
        for partition in partitions[10:]:
            df = read_partition(db_path, table_name, partition)
            total_rows += len(df)
            file_path = db_path / table_name / partition / "data.parquet"
            total_size += file_path.stat().st_size
    
    print("-" * 80)
    print(f"{'TOTAL':<12} {total_rows:<10,} {total_size / 1024 / 1024:.2f} MB")


def show_data_sample(db_path: Path, table_name: str, date: Optional[str] = None, n_rows: int = 5):
    """Show sample data from a table."""
    partitions = list_partitions(db_path, table_name)
    
    if not partitions:
        print(f"❌ No partitions found for table '{table_name}'")
        return
    
    # Select partition
    if date:
        partition_name = f"TRD_DD={date}"
        if partition_name not in partitions:
            print(f"❌ Date {date} not found in table '{table_name}'")
            print(f"Available dates: {', '.join([get_partition_date(p) for p in partitions[:5]])}...")
            return
    else:
        # Use most recent partition
        partition_name = partitions[-1]
        date = get_partition_date(partition_name)
    
    print(f"\n{'='*80}")
    print(f"SAMPLE DATA: {table_name} - {date}")
    print(f"{'='*80}")
    
    df = read_partition(db_path, table_name, partition_name)
    
    print(f"\nTotal rows: {len(df):,}")
    print(f"\nFirst {n_rows} rows:")
    print(df.head(n_rows).to_string())
    
    # Show data statistics for numeric columns
    numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
    if len(numeric_cols) > 0:
        print(f"\n{'='*80}")
        print("NUMERIC STATISTICS")
        print(f"{'='*80}")
        stats = df[numeric_cols].describe()
        print(stats.to_string())
    
    # Check for nulls
    print(f"\n{'='*80}")
    print("NULL VALUES")
    print(f"{'='*80}")
    null_counts = df.isnull().sum()
    null_cols = null_counts[null_counts > 0]
    if len(null_cols) > 0:
        print(f"\n{'Column':<20} {'Null Count':<12} {'Null %'}")
        print("-" * 80)
        for col, count in null_cols.items():
            pct = 100 * count / len(df)
            print(f"{col:<20} {count:<12,} {pct:.2f}%")
    else:
        print("✓ No null values found")


def show_specific_stocks(db_path: Path, table_name: str, symbols: list[str], date: Optional[str] = None):
    """Show data for specific stock symbols."""
    partitions = list_partitions(db_path, table_name)
    
    if not partitions:
        print(f"❌ No partitions found for table '{table_name}'")
        return
    
    # Select partition
    if date:
        partition_name = f"TRD_DD={date}"
        if partition_name not in partitions:
            print(f"❌ Date {date} not found")
            return
    else:
        partition_name = partitions[-1]
        date = get_partition_date(partition_name)
    
    print(f"\n{'='*80}")
    print(f"SPECIFIC STOCKS: {table_name} - {date}")
    print(f"{'='*80}")
    
    df = read_partition(db_path, table_name, partition_name)
    
    # Filter for specific symbols
    filtered = df[df['ISU_SRT_CD'].isin(symbols)]
    
    if len(filtered) == 0:
        print(f"❌ No data found for symbols: {', '.join(symbols)}")
        print(f"\nAvailable symbols (first 20): {', '.join(df['ISU_SRT_CD'].head(20).tolist())}")
        return
    
    print(f"\nFound {len(filtered)} stock(s)")
    print(filtered.to_string())


def validate_data(db_path: Path, table_name: str):
    """Validate data integrity."""
    partitions = list_partitions(db_path, table_name)
    
    if not partitions:
        print(f"❌ No partitions found for table '{table_name}'")
        return
    
    print(f"\n{'='*80}")
    print(f"DATA VALIDATION: {table_name}")
    print(f"{'='*80}")
    
    issues = []
    
    for partition in partitions:
        df = read_partition(db_path, table_name, partition)
        date = get_partition_date(partition)
        
        # Check if sorted by ISU_SRT_CD
        if 'ISU_SRT_CD' in df.columns:
            symbols = df['ISU_SRT_CD'].tolist()
            if symbols != sorted(symbols):
                issues.append(f"❌ {date}: Data NOT sorted by ISU_SRT_CD")
            else:
                print(f"✓ {date}: Data sorted by ISU_SRT_CD")
        
        # Check for duplicates
        if 'ISU_SRT_CD' in df.columns:
            duplicates = df['ISU_SRT_CD'].duplicated().sum()
            if duplicates > 0:
                issues.append(f"❌ {date}: Found {duplicates} duplicate symbols")
            else:
                print(f"✓ {date}: No duplicate symbols")
        
        # Check for negative prices (if applicable)
        if table_name == 'snapshots':
            price_cols = ['TDD_CLSPRC', 'BAS_PRC', 'OPNPRC', 'HGPRC', 'LWPRC']
            for col in price_cols:
                if col in df.columns:
                    negative = (df[col] < 0).sum()
                    if negative > 0:
                        issues.append(f"❌ {date}: Found {negative} negative values in {col}")
    
    if issues:
        print(f"\n{'='*80}")
        print("ISSUES FOUND")
        print(f"{'='*80}")
        for issue in issues:
            print(issue)
    else:
        print(f"\n{'='*80}")
        print("✓ All validations passed!")
        print(f"{'='*80}")


def main():
    parser = argparse.ArgumentParser(
        description="Inspect and explore Parquet database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Show summary of all tables
  poetry run python scripts/inspect_db.py --db ./data/krx_db

  # Show sample data from snapshots (most recent date)
  poetry run python scripts/inspect_db.py --db ./data/krx_db --table snapshots

  # Show sample data from specific date
  poetry run python scripts/inspect_db.py --db ./data/krx_db --table snapshots --date 20241101

  # Show more sample rows
  poetry run python scripts/inspect_db.py --db ./data/krx_db --table snapshots --show-sample 10

  # Show data for specific stocks
  poetry run python scripts/inspect_db.py --db ./data/krx_db --table snapshots --stocks 005930 000660

  # Validate data integrity
  poetry run python scripts/inspect_db.py --db ./data/krx_db --validate
        """,
    )
    
    parser.add_argument(
        "--db",
        type=str,
        required=True,
        help="Root path for Parquet database",
    )
    
    parser.add_argument(
        "--table",
        type=str,
        choices=["snapshots", "adj_factors", "liquidity_ranks"],
        help="Table to inspect (default: show all tables)",
    )
    
    parser.add_argument(
        "--date",
        type=str,
        help="Specific date to inspect (YYYYMMDD). Default: most recent",
    )
    
    parser.add_argument(
        "--show-sample",
        type=int,
        default=5,
        metavar="N",
        help="Number of sample rows to display. Default: 5",
    )
    
    parser.add_argument(
        "--stocks",
        type=str,
        nargs='+',
        metavar="SYMBOL",
        help="Show data for specific stock symbols (e.g., 005930 000660)",
    )
    
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate data integrity (sorting, duplicates, etc.)",
    )
    
    args = parser.parse_args()
    
    db_path = Path(args.db)
    
    if not db_path.exists():
        print(f"❌ Database not found: {db_path}")
        print("\nHave you run the build script yet?")
        print("  poetry run python scripts/build_db.py --days 5 --db ./data/krx_db")
        sys.exit(1)
    
    print(f"\n{'='*80}")
    print(f"PARQUET DATABASE INSPECTOR")
    print(f"{'='*80}")
    print(f"Database: {db_path.absolute()}")
    
    # Determine which tables to inspect
    if args.table:
        tables = [args.table]
    else:
        # Auto-detect available tables
        tables = []
        for table_name in ["snapshots", "adj_factors", "liquidity_ranks"]:
            if (db_path / table_name).exists():
                tables.append(table_name)
    
    if not tables:
        print(f"\n❌ No tables found in database")
        sys.exit(1)
    
    print(f"Tables found: {', '.join(tables)}")
    
    # Show table summaries
    for table_name in tables:
        show_table_summary(db_path, table_name)
    
    # Show sample data if requested
    if args.table:
        if args.stocks:
            show_specific_stocks(db_path, args.table, args.stocks, args.date)
        else:
            show_data_sample(db_path, args.table, args.date, args.show_sample)
    
    # Validate data if requested
    if args.validate:
        for table_name in tables:
            validate_data(db_path, table_name)
    
    print(f"\n{'='*80}")
    print("INSPECTION COMPLETE")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()

