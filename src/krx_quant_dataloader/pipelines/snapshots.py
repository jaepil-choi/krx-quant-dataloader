"""
Daily snapshots pipelines (e.g., MDCSTAT01602)

What this module does:
- Resume-safe ingestion: iterate dates, fetch one day at a time (strtDd=endDd=D),
  preprocess (label/coerce), and persist via a writer before proceeding to the next day.
- Post-hoc adjustment factor computation after ingestion completes.

Interaction:
- Consumed by apis/dataloader.py to deliver user-facing results.
- Uses RawClient under the hood; does not import transport/orchestration directly.
- Writer injected via dependency (protocols.SnapshotWriter).
"""


from __future__ import annotations

from typing import Any, Dict, Iterable, List

from ..transforms.preprocessing import preprocess_change_rates_rows
from ..transforms.adjustment import compute_adj_factors_grouped
from ..storage.protocols import SnapshotWriter


def ingest_change_rates_day(
    raw_client,
    *,
    date: str,
    market: str = "ALL",
    adjusted_flag: bool = False,
    writer: SnapshotWriter,
    endpoint_id: str = "stock.all_change_rates",
    host_id: str = "krx",
) -> int:
    """Fetch, preprocess, and persist one day of change-rate snapshots.

    Parameters
    ----------
    raw_client
        RawClient instance for calling endpoints.
    date : str
        Trading date in YYYYMMDD format.
    market : str
        Market ID (e.g., ALL, STK, KSQ).
    adjusted_flag : bool
        Whether to request adjusted prices (adjStkPrc=2 vs 1).
    writer : SnapshotWriter
        Writer instance for persisting preprocessed rows.
    endpoint_id : str
        Endpoint ID to call.
    host_id : str
        Host ID for the endpoint.

    Returns
    -------
    int
        Count of rows written (0 for holidays/empty responses).
    """
    rows = raw_client.call(
        endpoint_id,
        host_id=host_id,
        params={
            "strtDd": date,
            "endDd": date,
            "mktId": market,
            "adjStkPrc": 2 if adjusted_flag else 1,
        },
    )
    if not rows:
        return 0  # holiday/non-trading day

    preprocessed = preprocess_change_rates_rows(rows, trade_date=date)
    return writer.write_snapshot_rows(preprocessed)


def ingest_change_rates_range(
    raw_client,
    *,
    dates: Iterable[str],
    market: str = "ALL",
    adjusted_flag: bool = False,
    writer: SnapshotWriter,
    endpoint_id: str = "stock.all_change_rates",
    host_id: str = "krx",
) -> Dict[str, int]:
    """Ingest multiple days of change-rate snapshots; resume-safe.

    Each day is fetched, preprocessed, and persisted before proceeding to the next.
    Errors on a given day are logged and recorded; subsequent days continue.

    Parameters
    ----------
    raw_client
        RawClient instance.
    dates : Iterable[str]
        Trading dates in YYYYMMDD format.
    market : str
        Market ID.
    adjusted_flag : bool
        Whether to request adjusted prices.
    writer : SnapshotWriter
        Writer instance for persisting.
    endpoint_id : str
        Endpoint ID.
    host_id : str
        Host ID.

    Returns
    -------
    Dict[str, int]
        Mapping of date → row count written (or -1 for errors).
    """
    counts: Dict[str, int] = {}
    for d in dates:
        try:
            counts[d] = ingest_change_rates_day(
                raw_client,
                date=d,
                market=market,
                adjusted_flag=adjusted_flag,
                writer=writer,
                endpoint_id=endpoint_id,
                host_id=host_id,
            )
        except Exception as e:
            # Log error (structured logging can be added here)
            print(f"Error ingesting {d}: {type(e).__name__}: {e}")
            counts[d] = -1  # sentinel for error
    return counts


def compute_and_persist_adj_factors(
    snapshot_rows: Iterable[Dict[str, Any]],
    writer: SnapshotWriter,
) -> int:
    """Compute adjustment factors from snapshots and persist via writer.

    This is a post-hoc batch job after ingestion completes.

    Parameters
    ----------
    snapshot_rows : Iterable[Dict[str, Any]]
        Preprocessed snapshot rows (with TRD_DD, BAS_PRC, TDD_CLSPRC, ISU_SRT_CD).
    writer : SnapshotWriter
        Writer instance for persisting factor rows.

    Returns
    -------
    int
        Count of factor rows written.
    """
    factors = compute_adj_factors_grouped(snapshot_rows)
    return writer.write_factor_rows(factors)


__all__ = [
    "ingest_change_rates_day",
    "ingest_change_rates_range",
    "compute_and_persist_adj_factors",
]
