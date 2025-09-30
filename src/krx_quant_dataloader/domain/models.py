"""
Domain models (lightweight)

Contains simple type hints and row schemas used by transforms/pipelines.
No business logic should live here; keep structures minimal to avoid churn.

Examples:
- DailyChangeRow: minimal keys expected from MDCSTAT01602 snapshots
- QuoteRow: minimal keys expected from MDCSTAT01501 snapshots
"""

# from typing import TypedDict

# class DailyChangeRow(TypedDict, total=False):
#     ISU_SRT_CD: str
#     ISU_ABBRV: str
#     BAS_PRC: str
#     TDD_CLSPRC: str
#     CMPPREVDD_PRC: str
#     FLUC_RT: str
#     ACC_TRDVOL: str
#     ACC_TRDVAL: str
#     FLUC_TP: str
#     TRD_DD: str  # injected by client for snapshots


