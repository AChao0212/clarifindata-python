"""Free-service quickstart: pip install 'clarifindata[pandas]'."""
import os

from clarifindata import Client, datasets

with Client(api_key=os.environ["CLARIFINDATA_API_KEY"]) as cfd:
    df = cfd.get(datasets.TaiwanStockPER, stock_id="2330",
                 start="2026-06-02", end="2026-06-03", as_df=True)
    print(df)
    print("attribution:", df.attrs["attribution"])
    print("requests remaining:", cfd.rate_limit_remaining)

    for batch in cfd.iter_history(
        datasets.TaiwanStockPER, stock_id="2330", since="2025-01-01", with_meta=True,
    ):
        print("batch:", len(batch["data"]))
        for source in batch["attribution"]:
            print(source["statement"])
