from __future__ import annotations

from typing import Any, Dict, List


class DataLoader:
    """High-level API over the raw client.

    This class composes raw calls and returns tidy outputs. By default it
    returns the server rows as-is without silent transforms.
    """

    def __init__(self, *, raw_client):
        self._raw = raw_client

    def get_daily_quotes(self, *, date: str, market: str = "ALL", tidy: str = "wide") -> List[Dict[str, Any]]:
        """Fetch daily quotes for a given date and market.

        Parameters
        ----------
        date: str
            Trading date in YYYYMMDD format.
        market: str
            Market id (e.g., ALL, STK, KSQ, KNX).
        tidy: str
            Output style. Only 'wide' (as-is) is currently supported.
        """
        if tidy != "wide":
            raise NotImplementedError("Only tidy='wide' (as-is) is supported at this time")
        rows = self._raw.call(
            "stock.daily_quotes",
            host_id="krx",
            params={"trdDd": date, "mktId": market},
        )
        return rows

    def get_individual_history(
        self,
        *,
        isin: str,
        start: str,
        end: str,
        adjusted: bool = False,
    ) -> List[Dict[str, Any]]:
        """Fetch individual stock history for a date range.

        Parameters
        ----------
        isin: str
            ISIN code.
        start: str
            Start date in YYYYMMDD format.
        end: str
            End date in YYYYMMDD format.
        adjusted: bool
            Whether to request adjusted prices (explicitly opt-in).
        """
        adj_flag = 2 if adjusted else 1
        rows = self._raw.call(
            "stock.individual_history",
            host_id="krx",
            params={
                "isuCd": isin,
                "adjStkPrc": adj_flag,
                "strtDd": start,
                "endDd": end,
            },
        )
        return rows


