from __future__ import annotations

from pathlib import Path

from krx_quant_dataloader import create_dataloader_from_yaml


def main() -> None:
    # Use the test fixture config for demonstration; replace with your own path as needed
    repo_root = Path(__file__).resolve().parents[1]
    config_path = str(repo_root / "tests" / "fixtures" / "test_config.yaml")

    dl = create_dataloader_from_yaml(config_path)

    print("=== Daily quotes (2024-01-05, ALL) ===")
    rows = dl.get_daily_quotes(date="20240105", market="ALL")
    print(f"rows: {len(rows)}")
    print(rows[:3])

    print("\n=== Individual history (005930, sample range, unadjusted) ===")
    # Example ISIN for Samsung Electronics: KR7005930003
    rows2 = dl.get_individual_history(isin="KR7005930003", start="20240101", end="20240115", adjusted=False)
    print(f"rows: {len(rows2)}")
    print(rows2[:3])


if __name__ == "__main__":
    main()


