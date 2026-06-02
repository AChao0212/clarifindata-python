"""Synchronous clarifindata client.

    from clarifindata import Client

    cfd = Client(api_key="cfd_demo_lite_0002")
    rows = cfd.get("TaiwanStockPrice", stock_id="2330", start="2026-04-01", limit=20)
    for r in rows:
        print(r["trade_date"], r["close"])

Authentication is a standard Bearer token (RFC 6750):

    Authorization: Bearer <your_api_key>

Get a key at https://clarifindata.com. Demo keys for quick testing:
    cfd_demo_free_0001  (free tier)
    cfd_demo_lite_0002  (lite tier)
    cfd_demo_plus_0003  (plus tier)
"""
from __future__ import annotations

from datetime import date
from typing import Any

import httpx

DEFAULT_BASE_URL = "https://clarifindata.com/api"


class Client:
    """A thin, synchronous client over the clarifindata HTTP API."""

    def __init__(
        self,
        api_key: str,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = 30.0,
    ) -> None:
        if not api_key:
            raise ValueError("api_key is required")
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self._http = httpx.Client(
            base_url=self.base_url,
            timeout=timeout,
            headers={
                "Authorization": f"Bearer {api_key}",
                "User-Agent": f"clarifindata-py/{_version()}",
            },
        )

    # ----- lifecycle -----
    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def close(self) -> None:
        self._http.close()

    # ----- low-level -----
    def datasets(self) -> dict[str, Any]:
        """Public dataset catalog (no auth required)."""
        r = self._http.get("/v1/data")
        r.raise_for_status()
        return r.json()

    def get(
        self,
        dataset: str,
        *,
        stock_id: str | None = None,
        start: date | str | None = None,
        end: date | str | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """Fetch rows from `dataset`. Returns the `data` array directly.

        Raises httpx.HTTPStatusError on 4xx/5xx — note 403 means your key's
        tier cannot access this dataset (upgrade at https://clarifindata.com).
        """
        params: dict[str, Any] = {"limit": limit}
        if stock_id is not None:
            params["stock_id"] = stock_id
        if start is not None:
            params["start"] = str(start)
        if end is not None:
            params["end"] = str(end)
        r = self._http.get(f"/v1/data/{dataset}", params=params)
        r.raise_for_status()
        return r.json()["data"]

    def ask(self, question: str) -> dict[str, Any]:
        """Natural-language → SQL query (Plus tier only)."""
        r = self._http.post("/v1/ask", json={"question": question})
        r.raise_for_status()
        return r.json()

    # ----- one-shortcut-per-dataset (Pythonic ergonomics) -----
    def taiwan_stock_price(self, **kw: Any) -> list[dict[str, Any]]:
        return self.get("TaiwanStockPrice", **kw)

    def taiwan_stock_price_adj(self, **kw: Any) -> list[dict[str, Any]]:
        return self.get("TaiwanStockPriceAdj", **kw)

    def taiwan_stock_week_price(self, **kw: Any) -> list[dict[str, Any]]:
        return self.get("TaiwanStockWeekPrice", **kw)

    def taiwan_stock_month_price(self, **kw: Any) -> list[dict[str, Any]]:
        return self.get("TaiwanStockMonthPrice", **kw)

    def taiwan_stock_institutional(self, **kw: Any) -> list[dict[str, Any]]:
        return self.get("TaiwanStockInstitutionalInvestorsBuySell", **kw)


def _version() -> str:
    from clarifindata import __version__

    return __version__
