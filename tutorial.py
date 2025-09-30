#%%
# KRX Quant Data Loader (KQDL) – Tutorial
#
# This notebook-like script demonstrates how to use the user-facing DataLoader
# as well as the strict RawClient. It follows the PRD principles:
# - As-is data by default (no silent transforms)
# - Explicit, opt-in transforms only (e.g., adjusted prices)
# - Config-driven endpoints

#%%
from pathlib import Path

from krx_quant_dataloader import create_dataloader_from_yaml

# Point to the test fixture config for convenience. Replace with your own YAML if needed.
REPO_ROOT = Path(__file__).resolve().parent
CONFIG_PATH = str(REPO_ROOT / "tests" / "fixtures" / "test_config.yaml")
print(f"Using config: {CONFIG_PATH}")

#%%
# Create a ready-to-use DataLoader via the factory.
dl = create_dataloader_from_yaml(CONFIG_PATH)
print("DataLoader ready.")

#%%
# Fetch daily quotes (as-is, wide output) for a single day and market.
rows_daily = dl.get_daily_quotes(date="20240105", market="ALL", tidy="wide")
print(f"Daily quotes rows: {len(rows_daily)}")
print("Sample:", rows_daily[:3])

#%%
# Fetch an individual instrument's history (as-is) for a small date range.
# Samsung Electronics ISIN example: KR7005930003
rows_hist = dl.get_individual_history(isin="KR7005930003", start="20240101", end="20240115", adjusted=False)
print(f"Individual history (unadjusted) rows: {len(rows_hist)}")
print("Sample:", rows_hist[:3])

#%%
# Optional: request adjusted prices explicitly (this flips a server parameter; data still returned as-is).
rows_hist_adj = dl.get_individual_history(isin="KR7005930003", start="20240101", end="20240115", adjusted=True)
print(f"Individual history (adjusted) rows: {len(rows_hist_adj)}")
print("Sample:", rows_hist_adj[:3])

#%%
# Raw path: build and use the strict client. This requires full endpoint params and returns server data as-is.
from krx_quant_dataloader.config import ConfigFacade
from krx_quant_dataloader.adapter import AdapterRegistry
from krx_quant_dataloader.transport import Transport
from krx_quant_dataloader.orchestration import Orchestrator
from krx_quant_dataloader.client import RawClient

cfg = ConfigFacade.load(config_path=CONFIG_PATH)
reg = AdapterRegistry.load(config_path=CONFIG_PATH)
transport = Transport(cfg)
orch = Orchestrator(transport)
raw = RawClient(registry=reg, orchestrator=orch)

rows_raw_daily = raw.call(
    "stock.daily_quotes",
    host_id="krx",
    params={"trdDd": "20240105", "mktId": "ALL"},
)
print(f"Raw daily quotes rows: {len(rows_raw_daily)}")
print("Sample:", rows_raw_daily[:3])

#%%
# Fetch market-wide change rates (MDCSTAT01602: 전종목등락률) for a small date range.
rows_change_rates = raw.call(
    "stock.all_change_rates",
    host_id="krx",
    params={
        "strtDd": "20240102",
        "endDd": "20240105",
        "mktId": "ALL",
        "adjStkPrc": 1,
    },
)
print(f"All change rates rows: {len(rows_change_rates)}")
print("Sample:", rows_change_rates[:3])

#%%
# Verify all endpoints declared in the YAML by calling each with minimal params.
import time
from typing import Any, Dict

def synthesize_params(endpoint_id: str, spec) -> Dict[str, Any]:
    params: Dict[str, Any] = {}
    for name, ps in spec.params.items():
        if ps.default is not None:
            params[name] = ps.default
            continue
        if getattr(ps, "role", None) == "start_date":
            params[name] = "20240102"
            continue
        if getattr(ps, "role", None) == "end_date":
            params[name] = "20240105"
            continue
        if ps.type and str(ps.type).startswith("date("):
            params[name] = "20240105"
            continue
        if ps.enum:
            vals = list(ps.enum)
            params[name] = "ALL" if "ALL" in vals else vals[0]
            continue
        if name == "isuCd":
            params[name] = "KR7005930003"
        elif name == "idxIndMidclssCd":
            params[name] = "01"
        elif name in ("group_id", "indTpCd"):
            params[name] = "5"
        elif name in ("ticker", "indTpCd2"):
            params[name] = "300"
        elif name == "market":
            params[name] = "1"
        elif name in ("inqCondTpCd", "mktTpCd", "trdVolVal", "askBid"):
            params[name] = 1
        elif name in ("etf", "etn", "els", "elw"):
            params[name] = ""
        elif name == "typeNo":
            params[name] = 0
        elif name == "searchText":
            params[name] = ""
        elif name == "mktsel":
            params[name] = "ALL"
        elif name == "secugrpId":
            params[name] = "STMFRTSCIFDRFS"
        else:
            params[name] = ""

    if "els" in params and "elw" not in params:
        params["elw"] = params["els"]
    return params

print("\n=== Bulk endpoint verification (live) ===")
ok = 0
fail = 0
for eid, spec in reg.specs.items():
    try:
        call_params = synthesize_params(eid, spec)
        rows = raw.call(eid, host_id="krx", params=call_params)
        print(f"{eid}: OK {len(rows)} rows")
        ok += 1
        time.sleep(0.3)
    except Exception as e:
        print(f"{eid}: FAIL {type(e).__name__}: {e}")
        fail += 1
        time.sleep(0.3)
print(f"Summary: {ok} OK, {fail} FAIL")

#%%
# Notes
# - All outputs above are server payloads (as-is). No silent transforms are applied.
# - For larger date ranges, the orchestrator automatically chunks requests per endpoint policy and merges rows.
# - Keep live calls small to respect rate limits.


