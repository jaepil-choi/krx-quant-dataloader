#%%
# Quick experiment: stack daily snapshots for MDCSTAT01602 (전종목등락률)
#
# Dates: 2025-08-14, 2025-08-15 (holiday), 2025-08-18
# Goal: fetch strtDd=endDd=D for each day and print first 3 rows

#%%
from pathlib import Path
import csv
from decimal import Decimal
from collections import defaultdict

from krx_quant_dataloader.config import ConfigFacade
from krx_quant_dataloader.adapter import AdapterRegistry
from krx_quant_dataloader.transport import Transport
from krx_quant_dataloader.orchestration import Orchestrator
from krx_quant_dataloader.client import RawClient


def _parse_int_krx(value):
    if value is None:
        return None
    if isinstance(value, int):
        return value
    try:
        return int(str(value).replace(",", ""))
    except Exception:
        return None


def _shape_snapshot_row(r: dict, trd_dd: str) -> dict:
    return {
        "TRD_DD": trd_dd,
        "ISU_SRT_CD": r.get("ISU_SRT_CD"),
        "ISU_ABBRV": r.get("ISU_ABBRV"),
        "BAS_PRC": _parse_int_krx(r.get("BAS_PRC")),
        "TDD_CLSPRC": _parse_int_krx(r.get("TDD_CLSPRC")),
        "CMPPREVDD_PRC": r.get("CMPPREVDD_PRC"),
        "FLUC_RT": r.get("FLUC_RT"),
        "ACC_TRDVOL": _parse_int_krx(r.get("ACC_TRDVOL")),
        "ACC_TRDVAL": _parse_int_krx(r.get("ACC_TRDVAL")),
        "FLUC_TP": r.get("FLUC_TP"),
    }


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    config_path = str(repo_root / "tests" / "fixtures" / "test_config.yaml")
    print(f"Using config: {config_path}")

    cfg = ConfigFacade.load(config_path=config_path)
    reg = AdapterRegistry.load(config_path=config_path)
    transport = Transport(cfg)
    orch = Orchestrator(transport)
    raw = RawClient(registry=reg, orchestrator=orch)

    endpoint_id = "stock.all_change_rates"
    dates = ["20250814", "20250815", "20250818"]

    log_dir = repo_root / "log"
    log_dir.mkdir(parents=True, exist_ok=True)
    snapshot_csv = log_dir / "change_rates_snapshot.csv"
    factors_csv = log_dir / "change_rates_adj_factor.csv"

    snapshot_fieldnames = [
        "TRD_DD",
        "ISU_SRT_CD",
        "ISU_ABBRV",
        "BAS_PRC",
        "TDD_CLSPRC",
        "CMPPREVDD_PRC",
        "FLUC_RT",
        "ACC_TRDVOL",
        "ACC_TRDVAL",
        "FLUC_TP",
    ]
    factor_fieldnames = ["TRD_DD", "ISU_SRT_CD", "ADJ_FACTOR"]

    # Phase 1: Collect and shape rows for all days
    all_shaped_rows: list[dict] = []
    for d in dates:
        try:
            rows = raw.call(
                endpoint_id,
                host_id="krx",
                params={
                    "strtDd": d,
                    "endDd": d,
                    "mktId": "ALL",
                    "adjStkPrc": 1,
                },
            )
            n = len(rows) if isinstance(rows, list) else 0
            print(f"Date {d}: {n} rows")
            if n == 0:
                continue  # holiday/non-trading day – skip appends

            shaped_rows = [_shape_snapshot_row(r, d) for r in rows]
            all_shaped_rows.extend(shaped_rows)

            # Print a small sample for observability
            print(
                "Sample:",
                [
                    {k: srow[k] for k in ("ISU_SRT_CD", "BAS_PRC", "TDD_CLSPRC")}
                    for srow in shaped_rows[:3]
                ],
            )
        except Exception as e:
            print(f"Date {d}: ERROR {type(e).__name__}: {e}")

    # Sort snapshots for stable output
    all_shaped_rows.sort(key=lambda r: (r["TRD_DD"], r["ISU_SRT_CD"]))

    # Phase 2: Write snapshot table once
    with open(snapshot_csv, "w", newline="", encoding="utf-8") as f_snap:
        snapshot_writer = csv.DictWriter(f_snap, fieldnames=snapshot_fieldnames)
        snapshot_writer.writeheader()
        for row in all_shaped_rows:
            snapshot_writer.writerow(row)
    print(f"Wrote snapshot CSV: {snapshot_csv}")

    # Phase 3: Compute adjustment factors from the complete snapshot table
    factors: list[dict] = []
    rows_by_symbol: dict[str, list[dict]] = defaultdict(list)
    for row in all_shaped_rows:
        symbol = row["ISU_SRT_CD"]
        rows_by_symbol[symbol].append(row)

    for symbol, rows_sym in rows_by_symbol.items():
        rows_sym.sort(key=lambda r: r["TRD_DD"])  # ensure chronological
        prev_close = None
        for row in rows_sym:
            bas = row["BAS_PRC"]
            factor_str = ""
            if prev_close is not None and prev_close != 0 and bas is not None:
                factor_dec = Decimal(bas) / Decimal(prev_close)
                factor_str = str(factor_dec)
            factors.append({"TRD_DD": row["TRD_DD"], "ISU_SRT_CD": symbol, "ADJ_FACTOR": factor_str})
            # advance prev_close to today's close for next trading day
            if row["TDD_CLSPRC"] is not None:
                prev_close = row["TDD_CLSPRC"]

    # Phase 4: Write factors table once
    with open(factors_csv, "w", newline="", encoding="utf-8") as f_fac:
        factor_writer = csv.DictWriter(f_fac, fieldnames=factor_fieldnames)
        factor_writer.writeheader()
        for row in factors:
            factor_writer.writerow(row)
    print(f"Wrote adj-factor CSV: {factors_csv}")


if __name__ == "__main__":
    main()
def _parse_int_krx(value):
    if value is None:
        return None
    if isinstance(value, int):
        return value
    try:
        return int(str(value).replace(",", ""))
    except Exception:
        return None


def _shape_snapshot_row(r: dict, trd_dd: str) -> dict:
    return {
        "TRD_DD": trd_dd,
        "ISU_SRT_CD": r.get("ISU_SRT_CD"),
        "ISU_ABBRV": r.get("ISU_ABBRV"),
        "BAS_PRC": _parse_int_krx(r.get("BAS_PRC")),
        "TDD_CLSPRC": _parse_int_krx(r.get("TDD_CLSPRC")),
        "CMPPREVDD_PRC": r.get("CMPPREVDD_PRC"),
        "FLUC_RT": r.get("FLUC_RT"),
        "ACC_TRDVOL": _parse_int_krx(r.get("ACC_TRDVOL")),
        "ACC_TRDVAL": _parse_int_krx(r.get("ACC_TRDVAL")),
        "FLUC_TP": r.get("FLUC_TP"),
    }

