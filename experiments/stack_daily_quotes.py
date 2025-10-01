#%%
# Quick experiment: stack daily snapshots for MDCSTAT01602 (전종목등락률)
#
# Refactored to use the new pipeline modules:
# - pipelines.snapshots for resume-safe ingestion
# - storage.writers for CSV persistence
# - Demonstrates end-to-end: fetch → preprocess → persist → compute factors
#
# Dates: 2025-08-14, 2025-08-15 (holiday), 2025-08-18

#%%
import csv
from pathlib import Path

from krx_quant_dataloader.config import ConfigFacade
from krx_quant_dataloader.adapter import AdapterRegistry
from krx_quant_dataloader.transport import Transport
from krx_quant_dataloader.orchestration import Orchestrator
from krx_quant_dataloader.client import RawClient
from krx_quant_dataloader.storage.writers import CSVSnapshotWriter
from krx_quant_dataloader.pipelines.snapshots import (
    ingest_change_rates_range,
    compute_and_persist_adj_factors,
)


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    config_path = str(repo_root / "tests" / "fixtures" / "test_config.yaml")
    print(f"Using config: {config_path}")

    # Setup raw client
    cfg = ConfigFacade.load(config_path=config_path)
    reg = AdapterRegistry.load(config_path=config_path)
    transport = Transport(cfg)
    orch = Orchestrator(transport)
    raw = RawClient(registry=reg, orchestrator=orch)

    # Setup output paths
    log_dir = repo_root / "log"
    log_dir.mkdir(parents=True, exist_ok=True)
    snapshot_csv = log_dir / "change_rates_snapshot.csv"
    factors_csv = log_dir / "change_rates_adj_factor.csv"

    # Setup writer
    writer = CSVSnapshotWriter(
        snapshot_path=snapshot_csv,
        factor_path=factors_csv,
    )

    # Dates to fetch (includes holiday)
    dates = ["20250814", "20250815", "20250818"]

    print(f"\n{'='*60}")
    print("Phase 1: Ingest daily snapshots (resume-safe, per-day)")
    print(f"{'='*60}")

    # Ingest snapshots using pipeline function
    counts = ingest_change_rates_range(
        raw,
        dates=dates,
        market="ALL",
        adjusted_flag=False,
        writer=writer,
    )

    # Display results
    for date, count in counts.items():
        status = "HOLIDAY (no rows)" if count == 0 else f"{count} rows ingested"
        print(f"  {date}: {status}")

    print(f"\nWrote snapshots to: {snapshot_csv}")

    print(f"\n{'='*60}")
    print("Phase 2: Compute adjustment factors (post-hoc)")
    print(f"{'='*60}")

    # Read back snapshots for factor computation
    with open(snapshot_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        snapshots = list(reader)

    # Convert numeric strings back to ints (CSV reads everything as strings)
    def safe_int(value):
        if not value or value == 'None' or value == '':
            return None
        try:
            # Handle negative numbers and remove any commas
            cleaned = value.replace(',', '')
            return int(cleaned)
        except (ValueError, AttributeError):
            return None
    
    for row in snapshots:
        row["BAS_PRC"] = safe_int(row.get("BAS_PRC"))
        row["TDD_CLSPRC"] = safe_int(row.get("TDD_CLSPRC"))

    # Compute and persist factors
    factor_count = compute_and_persist_adj_factors(snapshots, writer)
    print(f"  Computed {factor_count} adjustment factors")
    print(f"\nWrote factors to: {factors_csv}")

    print(f"\n{'='*60}")
    print("Phase 3: Display sample results")
    print(f"{'='*60}")

    # Show sample snapshots
    print(f"\nSnapshot sample (first 3 rows):")
    for i, row in enumerate(snapshots[:3]):
        print(f"  Row {i}: {row['TRD_DD']} | {row['ISU_SRT_CD']} | "
              f"BAS_PRC={row['BAS_PRC']} | TDD_CLSPRC={row['TDD_CLSPRC']}")

    # Show sample factors
    with open(factors_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        factors = list(reader)

    print(f"\nFactor sample (first 5 rows):")
    for i, row in enumerate(factors[:5]):
        factor_display = row['ADJ_FACTOR'] if row['ADJ_FACTOR'] else "NULL (first day)"
        print(f"  Row {i}: {row['TRD_DD']} | {row['ISU_SRT_CD']} | {factor_display}")

    writer.close()
    print(f"\n{'='*60}")
    print("✓ Experiment complete!")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
