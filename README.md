# clarifindata-python

Official Python client for **[clarifindata](https://clarifindata.com)** — a lean
Taiwan-stock market-data API with deep history, built for retail quants and AI agents.

- 70+ datasets (prices, chips, fundamentals, derivatives, macro) with years of history
- Simple Bearer-token auth, JSON over HTTP
- Free tier for market aggregates + macro; paid tiers for per-stock & high-resolution data

## Install

```bash
pip install clarifindata
```

## Quick start

```python
from clarifindata import Client

cfd = Client(api_key="cfd_demo_lite_0002")  # demo key — get your own at clarifindata.com

# Per-dataset shortcut
rows = cfd.taiwan_stock_price(stock_id="2330", start="2026-04-01", limit=20)
for r in rows:
    print(r["trade_date"], r["close"])

# Or the generic getter for any dataset
news = cfd.get("TaiwanStockTotalInstitutionalInvestors", start="2026-05-01", limit=10)
```

### Browse the catalog (no key needed)

```python
from clarifindata import Client
print(Client(api_key="cfd_demo_free_0001").datasets())
```

## Authentication

Pass your API key; the client sends it as a standard Bearer token:

```
Authorization: Bearer <your_api_key>
```

Get a key at **<https://clarifindata.com>**. Demo keys for trying it out:

| Key | Tier |
|-----|------|
| `cfd_demo_free_0001` | free |
| `cfd_demo_lite_0002` | lite |
| `cfd_demo_plus_0003` | plus |

A `403` from `.get()` means your key's tier can't access that dataset — upgrade your plan.

## Tiers (summary)

- **Free** — market aggregates (大盤三大法人/融資融券), macro (匯率/利率/油金), fundamentals, news.
- **Lite** — per-stock daily/weekly/monthly K, adjusted price, per-stock chips, margin ratio, etc.
- **Plus** — tick-level, 5-second index, convertibles, night-session institutional, NL query (`ask`).

Full, up-to-date catalog and date ranges: **<https://clarifindata.com/datasets>**.

## License

MIT — see [LICENSE](LICENSE).
