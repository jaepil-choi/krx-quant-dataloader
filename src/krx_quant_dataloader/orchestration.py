from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional

from .adapter import EndpointSpec


class ExtractionError(RuntimeError):
    pass


def _extract_rows(payload: Dict[str, Any], roots: Iterable[str]) -> List[Dict[str, Any]]:
    for key in roots:
        if key in payload:
            rows = payload[key]
            if isinstance(rows, list):
                return rows
    raise ExtractionError("No known root keys found in payload")


def _format_yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def _parse_yyyymmdd(s: str) -> datetime:
    return datetime.strptime(s, "%Y%m%d")


class Orchestrator:
    def __init__(self, transport):
        self._transport = transport

    def execute(
        self,
        *,
        spec: EndpointSpec,
        host_id: str,
        params: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        # Prepare base data including mandatory bld
        base_data: Dict[str, Any] = {**params, "bld": spec.bld}

        # Determine if we should chunk
        if spec.date_params and (
            spec.date_params.get("start") in params and spec.date_params.get("end") in params
        ):
            start_key = spec.date_params["start"]
            end_key = spec.date_params["end"]
            dt_start = _parse_yyyymmdd(str(params[start_key]))
            dt_end = _parse_yyyymmdd(str(params[end_key]))

            days = spec.chunking.days if spec.chunking and spec.chunking.days else 730
            gap_days = spec.chunking.gap_days if spec.chunking and spec.chunking.gap_days else 0

            rows: List[Dict[str, Any]] = []
            cur = dt_start
            while cur <= dt_end:
                chunk_end = min(cur + timedelta(days=days - 1), dt_end)
                data = deepcopy(base_data)
                data[start_key] = _format_yyyymmdd(cur)
                data[end_key] = _format_yyyymmdd(chunk_end)

                payload = self._transport.send(
                    method=spec.method,
                    host_id=host_id,
                    path=spec.path,
                    data=data,
                )
                chunk_rows = _extract_rows(payload, spec.response_roots)
                rows.extend(chunk_rows)

                # advance
                cur = chunk_end + timedelta(days=gap_days)
                if gap_days == 0:
                    cur += timedelta(days=1)

            # order if requested
            if spec.order_by:
                order_key = spec.order_by
                # Safe sort: place missing keys at the end; compare as strings to avoid None comparisons
                rows.sort(key=lambda r: (r.get(order_key) is None, str(r.get(order_key) or "")))
            return rows

        # No chunking: single call
        payload = self._transport.send(
            method=spec.method,
            host_id=host_id,
            path=spec.path,
            data=base_data,
        )
        rows = _extract_rows(payload, spec.response_roots)
        if spec.order_by:
            order_key = spec.order_by
            rows.sort(key=lambda r: (r.get(order_key) is None, str(r.get(order_key) or "")))
        return rows


