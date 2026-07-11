# clarifindata — Python client

Python client for the **金晰數據 / clarifindata** Taiwan-stock data API (82 datasets:
prices, chips, fundamentals, derivatives, macro — full history, refreshed daily).

```bash
pip install clarifindata            # core
pip install 'clarifindata[pandas]'  # + DataFrame output (as_df=True)
pip install 'clarifindata[all]'     # + pandas + HTTP/2
```

## Quickstart

```python
from clarifindata import Client, datasets

cfd = Client(api_key="cfd_demo_free_0001")          # free demo key

# typed dataset names (autocomplete + typo-safe), DataFrame output
df = cfd.get(datasets.TaiwanStockPrice, stock_id="2330",
             start="2026-04-01", end="2026-06-03", as_df=True)
print(df.head())
```

**Demo keys** (rate-limited, for trying out): `cfd_demo_free_0001` (Free),
`cfd_demo_lite_0001` (Lite). Get your own at <https://clarifindata.com/signup>.

## Features

| What | How |
|------|-----|
| **Auto-retry** 429/5xx/timeouts (honours `Retry-After`, exp backoff) | on by default; tune `max_retries`, `backoff_base` |
| **Rate-limit aware** | `cfd.rate_limit_remaining`; warns at 80% used |
| **DataFrame output** | `cfd.get(..., as_df=True)` |
| **Full-history streaming** (auto-paginates the 10 000-row cap) | `cfd.iter_history(ds, stock_id=..., since="2015-01-01")` |
| **Async / concurrency** | `AsyncClient` (same surface) |
| **Multi-dataset pull** | `cfd.bulk_pull([...], start=, end=)` |
| **Disk cache** (for slow-changing datasets) | `Client(cache_dir="./.cache", cache_ttl=86400)` |
| **Typed errors w/ suggestions** | `ClarifindataError(dataset, status, detail, suggestion)` |
| **Logging** | `logging.getLogger("clarifindata")` (DEBUG = request/response) |

## Full history (any range, no manual chunking)

```python
for batch in cfd.iter_history(datasets.TaiwanStockPrice, stock_id="2330", since="2015-01-01"):
    process(batch)                                  # one year per batch, streamed

df = cfd.history(datasets.TaiwanStockPrice, stock_id="2330", since="2015-01-01", as_df=True)
```

## Concurrency (fetch many stocks)

```python
import asyncio
from clarifindata import AsyncClient, datasets

async def main():
    async with AsyncClient(api_key="...") as cfd:
        tasks = [cfd.get(datasets.TaiwanStockPrice, stock_id=s, as_df=True)
                 for s in ("2330", "2317", "2454")]
        return await asyncio.gather(*tasks)

frames = asyncio.run(main())
```

## Natural-language queries (Plus tier)

```python
ans = cfd.ask("找出 2330 在 2026 年 4 月最高收盤價那天", as_df=True)
```

## Errors

```python
from clarifindata import TierError, RateLimitError, ValidationError
try:
    cfd.get("TaiwanStockPirce")          # typo
except ValidationError as e:
    print(e)        # Unknown dataset 'TaiwanStockPirce'. Did you mean 'TaiwanStockPrice'? ...
```
