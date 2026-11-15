"""clarifindata — Python client for the 金晰數據 Taiwan-stock data API.

    from clarifindata import Client, datasets
    cfd = Client(api_key="your_api_key")
    df = cfd.get(datasets.TaiwanStockPrice, stock_id="2330",
                 start="2026-04-01", as_df=True)

Concurrency (quant default):

    import asyncio
    from clarifindata import AsyncClient, datasets
    async def main():
        async with AsyncClient(api_key="...") as cfd:
            out = await asyncio.gather(*[
                cfd.get(datasets.TaiwanStockPrice, stock_id=s) for s in ("2330", "2317")])
"""
from __future__ import annotations

from . import datasets
from .client import AsyncClient, Client, __version__
from .datasets import DATASET_ROW_TYPES, DatasetRow, Row
from .errors import (AuthError, ClarifindataError, NotFoundError, RateLimitError,
                     ValidationError)

__all__ = [
    "Client", "AsyncClient", "datasets", "__version__",
    "DatasetRow", "Row", "DATASET_ROW_TYPES",
    "ClarifindataError", "AuthError", "RateLimitError",
    "NotFoundError", "ValidationError",
]
