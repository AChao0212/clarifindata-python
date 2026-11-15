# clarifindata Python client

Python client for the 金晰數據 / clarifindata Taiwan market-data API.
The service is free and not-for-profit. Every authenticated account has the same
quota of 3,000 requests per hour per user, shared across that user's API keys.

```bash
pip install clarifindata
pip install 'clarifindata[pandas]'  # DataFrame output
pip install 'clarifindata[all]'     # pandas and HTTP/2
```

## Quickstart

Create an account and get an API key at <https://clarifindata.com/signup>.

```python
from clarifindata import Client, datasets

cfd = Client(api_key="your_api_key")
result = cfd.get(
    datasets.TaiwanStockPrice, stock_id="2330",
    start="2026-04-01", end="2026-04-30", with_meta=True,
)
print(result["data"])
for source in result["attribution"]:
    print(source["statement"])

df = cfd.get(datasets.TaiwanStockPER, stock_id="2330", as_df=True)
print(df.head())
print(df.attrs["attribution"])
```

## Data sources

本服務資料取自臺灣證券交易所、證券櫃檯買賣中心、臺灣期貨交易所等機構之公開網頁及開放資料，未與任何資料來源機構簽訂授權契約，非官方資料，僅為分享。各資料標示原始來源。權利人如認為特定資料不宜公開，請來信support@clarifindata.com，我們將於收到後7日內處理並回覆。

Each attribution entry identifies the original provider, source URL and source
statement. `license_name` and `license_url` are null unless a verified licence
applies to that source. The SDK's MIT licence covers its code, not the data.

## Return types and attribution

`get()` and `history()` return lists of rows, `iter_history()` yields lists of
rows, and `bulk_pull()` returns a mapping from dataset names to lists of rows.
`as_df=True` changes rows to DataFrames and sets `df.attrs["attribution"]`,
including for empty results.

Use `with_meta=True` to retain source metadata:

| Method | Metadata result |
|--------|-----------------|
| `get()` | Response dictionary with `data`, `attribution` and server fields such as `next_cursor` |
| `iter_history()` | Each batch has `data` and deduplicated `attribution` from all its pages |
| `history()` | `data` and the union of attribution across all pages and years |
| `bulk_pull()` | `data` maps dataset names to rows; `attribution` is the union across all datasets and pages |

You can combine `with_meta=True` and `as_df=True`; `result["data"]` then holds
the DataFrame, or the mapping of DataFrames for `bulk_pull()`. Metadata and
DataFrame history modes retain empty batches so their attribution is not lost.
`catalog()` returns public dataset entries, each with its attribution.

## Full history

History methods follow cursor pagination and split dated ranges into calendar
years. Omit `since` for datasets with no date axis.

```python
for batch in cfd.iter_history(
    datasets.TaiwanStockPrice, stock_id="2330", since="2015-01-01", with_meta=True,
):
    process(batch["data"], batch["attribution"])

result = cfd.history(
    datasets.TaiwanStockPrice, stock_id="2330", since="2015-01-01", with_meta=True,
)
print(result["attribution"])

result = cfd.bulk_pull(
    [datasets.TaiwanStockPrice, datasets.TaiwanStockPER],
    stock_id="2330", start="2026-04-01", end="2026-04-30", with_meta=True,
)
print(result["attribution"])
```

## Async queries

`AsyncClient` supports the same metadata options. Its `iter_history()` method
returns a list of batches after `await`.

```python
import asyncio
from clarifindata import AsyncClient, datasets

async def main():
    async with AsyncClient(api_key="your_api_key") as cfd:
        return await asyncio.gather(*[
            cfd.get(datasets.TaiwanStockPrice, stock_id=s, with_meta=True)
            for s in ("2330", "2317", "2454")
        ])

results = asyncio.run(main())
```

## Retries, rate limits and caching

The client retries 429, selected 5xx responses and network failures with exponential
backoff, honouring `Retry-After`. Configure `max_retries` and `backoff_base` when
constructing the client. `rate_limit_limit` and `rate_limit_remaining` track the
server's quota headers; the client warns when 80% of the quota is used.
Use lower concurrency or wait for the quota window when requests are limited.
`Client(cache_dir="./.cache", cache_ttl=86400)` optionally caches GET responses,
including attribution. Logging uses `logging.getLogger("clarifindata")`.

## Errors

```python
from clarifindata import AuthError, RateLimitError, ValidationError

try:
    cfd.get("TaiwanStockPirce")
except ValidationError as error:
    print(error)  # Suggests TaiwanStockPrice.
```

Both 401 and 403 responses raise `AuthError` with the server's `detail`.
`RateLimitError` is raised after retries are exhausted.

## Changelog

### 0.3.0

The catalog includes live public datasets. All authenticated accounts have
3,000 requests per hour.

Source metadata is available through `with_meta=True` and DataFrame attributes
across sync and async queries, including paginated history and multi-dataset pulls.
