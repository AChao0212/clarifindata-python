"""Public dataset name constants from the live catalog.

Use these instead of raw strings to get IDE autocomplete + catch typos early:

    from clarifindata import datasets
    cfd.get(datasets.TaiwanStockPrice, stock_id="2330")
"""
from __future__ import annotations

from typing import Any, Dict, Type

try:  # py3.8+ stdlib; kept here for an explicit, py3.9-safe import
    from typing import TypedDict
except ImportError:  # pragma: no cover
    from typing_extensions import TypedDict  # type: ignore[assignment]

# --- response row typing -------------------------------------------------
#
# A single API response row is a plain JSON object (dict). The concrete
# columns vary per dataset and are defined by the live server catalog, which
# is NOT shipped with this client, so we cannot hand-author a verified
# TypedDict per dataset without guessing column names.
#
# `DatasetRow` is the generic, always-correct row type — use it to annotate
# response rows and get a typed alias for `list[DatasetRow]`:
#
#     from clarifindata import DatasetRow
#     rows: list[DatasetRow] = cfd.get(datasets.TaiwanStockPrice, stock_id="2330")
#
# Per-dataset TypedDicts (for richer IDE autocomplete on columns) can be
# code-generated from the catalog; when available they should be registered
# in `DATASET_ROW_TYPES` below, mapping dataset name -> TypedDict subclass.
DatasetRow = Dict[str, Any]


class Row(TypedDict, total=False):
    """Generic typed view of a response row.

    Open shape (``total=False``) — every key is optional because columns
    differ per dataset. Subclass this for code-generated per-dataset row
    types and register them in :data:`DATASET_ROW_TYPES`.
    """


# dataset name -> per-dataset TypedDict row type (populate from catalog).
# Empty until verified per-dataset shapes are generated; consumers should
# fall back to ``DatasetRow`` for any dataset not present here.
DATASET_ROW_TYPES: Dict[str, Type[Row]] = {}

# Live public datasets; withdrawn datasets and collector stubs are not exposed.
PUBLIC_DATASETS: tuple[str, ...] = (
    "TaiwanStockOverview",
    "TaiwanStockOverviewIncludeWarrant",
    "TaiwanStockWarrantInfo",
    "TaiwanStockBorrowingCollateral",
    "TaiwanStockPrice",
    "TaiwanStockTradingDate",
    "TaiwanStockSectorPrice",
    "TaiwanStockPER",
    "TaiwanWeightedIndex",
    "TaiwanStockDayTradingInfo",
    "TaiwanReturnIndex",
    "TaiwanStockPriceAdj",
    "TaiwanStockTickHistory",
    "TaiwanStockWeekPrice",
    "TaiwanStockMonthPrice",
    "TaiwanStock10Year",
    "TaiwanStockTradingHaltAnnouncement",
    "TaiwanStockSuspendDayTrading",
    "TaiwanStockDailyPriceLimit",
    "TaiwanStockMarginPurchaseShortSale",
    "TaiwanStockTotalMarginPurchaseShortSale",
    "TaiwanStockInstitutionalInvestorsBuySell",
    "TaiwanStockTotalInstitutionalInvestors",
    "TaiwanStockShareholding",
    "TaiwanStockSecuritiesLending",
    "TaiwanStockMarginShortSaleSuspension",
    "TaiwanDailyShortSaleBalances",
    "TaiwanStockBlockTrade",
    "TaiwanStockCreditLimit",
    "TaiwanStockBrokerInfo",
    "TaiwanStockMarginRatio",
    "TaiwanStockHoldingSharesPer",
    "TaiwanStockDisposition",
    "TaiwanStockCashFlowsStatement",
    "TaiwanStockFinancialStatement",
    "TaiwanStockBalanceSheet",
    "TaiwanStockDividend",
    "TaiwanStockDividendResult",
    "TaiwanStockDividendSchedule",
    "TaiwanStockMonthRevenue",
    "TaiwanStockCapitalReductionReferencePrice",
    "TaiwanStockDelisting",
    "TaiwanStockSplitPrice",
    "TaiwanStockChangeFaceReference",
    "TaiwanStockMarketValue",
    "TaiwanStockMarketValueWeight",
    "TaiwanMonthlyEconomicIndicator",
    "TaiwanStockCompanyIndustry",
    "TaiwanFutureOptionDailyTotalOverview",
    "TaiwanFutureOptionTickRealtime",
    "TaiwanFuturesDaily",
    "TaiwanOptionDaily",
    "TaiwanFutOptInstitutionalInvestors",
    "TaiwanFuturesInstitutionalInvestors",
    "TaiwanOptionInstitutionalInvestors",
    "TaiwanFuturesBrokerDaily",
    "TaiwanFuturesBrokerInfo",
    "TaiwanOptionsBrokerDaily",
    "TaiwanFuturesLargeTraders",
    "TaiwanOptionsLargeTraders",
    "TaiwanFuturesNightInstitutionalInvestors",
    "TaiwanOptionsNightInstitutionalInvestors",
    "TaiwanFuturesSettlement",
    "TaiwanOptionsSettlement",
    "TaiwanOptionsFSP",
    "TaiwanFuturesSpread",
    "TaiwanCBOverview",
    "TaiwanCBDaily",
    "TaiwanCBInstitutionalInvestors",
    "TaiwanCBDailyOverview",
    "TaiwanRelatedNews",
    "TaiwanStockAssetTransaction",
    "CurrencyCirculation",
    "ExchangeRate",
    "CentralBankRate",
    "USBondYield",
    "TaiwanFinancialHoldingsCompany",
    "CryptoFearAndGreedIndex",
)

ALL: frozenset[str] = frozenset(PUBLIC_DATASETS)

# Expose each dataset name as a module attribute: datasets.TaiwanStockPrice.
for _n in PUBLIC_DATASETS:
    globals()[_n] = _n
del _n
