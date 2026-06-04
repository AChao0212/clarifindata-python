"""clarifindata quickstart — pip install 'clarifindata[pandas]'"""
from clarifindata import Client, datasets

cfd = Client(api_key="cfd_demo_free_0001")          # free demo key

# typed dataset names + DataFrame output
df = cfd.get(datasets.TaiwanStockPER, stock_id="2330",
             start="2026-06-02", end="2026-06-03", as_df=True)
print(df)
print("rate limit remaining:", cfd.rate_limit_remaining)

# full history, auto-paginated around the 10k-row cap
for batch in cfd.iter_history(datasets.TaiwanStockPER, stock_id="2330", since="2025-01-01"):
    print("batch:", len(batch))
