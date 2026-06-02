"""Minimal quickstart for the clarifindata Python client.

Run:
    pip install clarifindata
    python examples/quickstart.py
"""
from clarifindata import Client


def main() -> None:
    # Demo lite key — replace with your own from https://clarifindata.com
    with Client(api_key="cfd_demo_lite_0002") as cfd:
        rows = cfd.taiwan_stock_price(stock_id="2330", limit=5)
        print(f"latest {len(rows)} closes for 2330:")
        for r in rows:
            print(f"  {r['trade_date']}  close={r['close']}")


if __name__ == "__main__":
    main()
