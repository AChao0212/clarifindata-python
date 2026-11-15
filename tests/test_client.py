import asyncio
import inspect

import httpx
import pytest

import clarifindata
from clarifindata import AsyncClient, AuthError, Client, RateLimitError, ValidationError, datasets


PRICE = datasets.TaiwanStockPrice
PER = datasets.TaiwanStockPER
WITHDRAWN = {
    "TaiwanStockAnalystEstimate", "USStockPrice", "USStockKBars",
    "GoldPrice", "CrudeOilPrices", "FearAndGreedIndex",
}


def source(name):
    return {
        "source_id": name,
        "provider": name,
        "provider_en": name,
        "url": f"https://{name}.example/",
        "license_name": None,
        "license_url": None,
        "statement": f"Source: {name}",
    }


A, B, C = source("a"), source("b"), source("c")


def resolve(value):
    return asyncio.run(value) if inspect.isawaitable(value) else value


@pytest.fixture(params=[Client, AsyncClient], ids=["sync", "async"])
def make_client(request):
    clients = []

    def make(handler, **kwargs):
        client = request.param("test-key", **kwargs)
        resolve(client.close())
        transport = httpx.MockTransport(handler)
        http_class = httpx.AsyncClient if request.param is AsyncClient else httpx.Client
        client._http = http_class(base_url=client.base_url, transport=transport)
        clients.append(client)
        return client

    yield make
    for client in clients:
        resolve(client.close())


def test_public_dataset_constants():
    assert datasets.ALL == frozenset(datasets.PUBLIC_DATASETS)
    assert len(datasets.PUBLIC_DATASETS) == len(datasets.ALL)
    assert not WITHDRAWN & datasets.ALL
    assert {"TaiwanStockTickHistory", "TaiwanStockDividendSchedule", "TaiwanStockAssetTransaction"} <= datasets.ALL
    assert "TaiwanStockRealtime" not in datasets.ALL
    assert not hasattr(datasets, "DATASET_TIERS")
    assert not hasattr(clarifindata, "TierError")
    for name in datasets.ALL:
        assert getattr(datasets, name) == name
    for name in WITHDRAWN:
        assert not hasattr(datasets, name)


@pytest.mark.parametrize("rows", [[], [{"close": 100}]])
@pytest.mark.parametrize("as_df", [False, True])
@pytest.mark.parametrize("with_meta", [False, True])
def test_get_metadata_and_defaults(make_client, rows, as_df, with_meta):
    body = {"data": rows, "attribution": [A], "next_cursor": "cursor", "total": 1}
    client = make_client(lambda request: httpx.Response(200, json=body))
    result = resolve(client.get(PRICE, as_df=as_df, with_meta=with_meta))
    data = result["data"] if with_meta else result
    if with_meta:
        assert result["attribution"] == [A]
        assert result["next_cursor"] == "cursor"
        assert result["total"] == 1
    if as_df:
        assert data.attrs["attribution"] == [A]
        assert data.to_dict("records") == rows
    else:
        assert data == rows
    assert not hasattr(client, "tier")
    assert "tier=" not in repr(client)


def paged_handler(request):
    params = request.url.params
    year = params.get("start", "2025")[:4]
    extra = B if year == "2025" else C
    if params.get("cursor"):
        return httpx.Response(200, json={
            "data": [{"page": 2}], "attribution": [A, extra], "next_cursor": None,
        })
    return httpx.Response(200, json={
        "data": [{"page": 1}], "attribution": [A], "next_cursor": "second",
    })


@pytest.mark.parametrize("as_df", [False, True])
def test_history_unions_pages_and_years(make_client, as_df):
    client = make_client(paged_handler)
    result = resolve(client.history(
        PRICE, since="2025-01-01", until="2026-01-01", as_df=as_df, with_meta=True,
    ))
    assert result["attribution"] == [A, B, C]
    assert len(result["data"]) == 4
    if as_df:
        assert result["data"].attrs["attribution"] == [A, B, C]
    default = resolve(client.history(PRICE))
    assert default == [{"page": 1}, {"page": 2}]


def test_iter_history_metadata_and_defaults(make_client):
    client = make_client(paged_handler)
    batches = list(resolve(client.iter_history(
        PRICE, since="2025-01-01", until="2026-01-01", with_meta=True,
    )))
    assert [batch["attribution"] for batch in batches] == [[A, B], [A, C]]
    assert all(len(batch["data"]) == 2 for batch in batches)
    assert list(resolve(client.iter_history(PRICE))) == [[{"page": 1}, {"page": 2}]]
    frames = list(resolve(client.iter_history(PRICE, as_df=True)))
    assert frames[0].attrs["attribution"] == [A, B]


@pytest.mark.parametrize("method", ["history", "iter_history", "bulk_pull"])
@pytest.mark.parametrize("as_df", [False, True])
def test_empty_history_retains_attribution(make_client, method, as_df):
    client = make_client(lambda request: httpx.Response(200, json={"data": [], "attribution": [A]}))
    target = [PRICE] if method == "bulk_pull" else PRICE
    result = resolve(getattr(client, method)(target, with_meta=True, as_df=as_df))
    if method == "iter_history":
        result = list(result)[0]
    assert result["attribution"] == [A]
    data = result["data"][PRICE] if method == "bulk_pull" else result["data"]
    assert len(data) == 0
    if as_df:
        assert data.attrs["attribution"] == [A]


@pytest.mark.parametrize("as_df", [False, True])
def test_bulk_metadata_union(make_client, as_df):
    def handler(request):
        if request.url.path.endswith(PER):
            return httpx.Response(200, json={"data": [], "attribution": [C]})
        return paged_handler(request)

    client = make_client(handler)
    result = resolve(client.bulk_pull([PRICE, PER], as_df=as_df, with_meta=True))
    assert result["attribution"] == [A, B, C]
    assert len(result["data"][PRICE]) == 2
    if as_df:
        assert result["data"][PRICE].attrs["attribution"] == [A, B]
        assert result["data"][PER].attrs["attribution"] == [C]
    default = resolve(client.bulk_pull([PER]))
    assert default == {PER: []}


def test_catalog_path_and_attribution(make_client):
    def handler(request):
        assert request.url.path == "/v1/datasets"
        return httpx.Response(200, json={"datasets": [{"name": PRICE, "attribution": [A]}]})

    client = make_client(handler)
    assert resolve(client.catalog()) == [{"name": PRICE, "attribution": [A]}]


@pytest.mark.parametrize("status", [401, 403])
def test_auth_error_keeps_server_detail(make_client, status):
    client = make_client(lambda request: httpx.Response(status, json={"detail": "Account access denied"}))
    with pytest.raises(AuthError) as caught:
        resolve(client.get(PRICE))
    assert type(caught.value) is AuthError
    assert caught.value.status == status
    assert caught.value.detail == "Account access denied"
    assert "upgrade" not in str(caught.value).lower()


@pytest.mark.parametrize("status", [429, 503, "timeout"])
def test_retry_and_rate_limit_tracking(make_client, monkeypatch, status):
    waits = []
    monkeypatch.setattr("clarifindata.client.time.sleep", waits.append)

    async def sleep(delay):
        waits.append(delay)

    monkeypatch.setattr(asyncio, "sleep", sleep)
    attempts = []

    def handler(request):
        attempts.append(request)
        if len(attempts) == 1:
            if status == "timeout":
                raise httpx.ReadTimeout("timeout", request=request)
            return httpx.Response(status, json={"detail": "Retry"}, headers={"Retry-After": "2"})
        return httpx.Response(200, json={"data": [], "attribution": [A]}, headers={
            "X-RateLimit-Limit": "3000", "X-RateLimit-Remaining": "2999",
            "X-RateLimit-Window": "3600",
        })

    client = make_client(handler, max_retries=1)
    assert resolve(client.get(PRICE, with_meta=True))["attribution"] == [A]
    assert len(attempts) == 2
    assert waits == ([0.5] if status == "timeout" else [2.0])
    assert client.rate_limit_limit == 3000
    assert client.rate_limit_remaining == 2999


def test_rate_limit_error_after_retries(make_client):
    client = make_client(lambda request: httpx.Response(
        429, json={"detail": "Quota reached"}, headers={"Retry-After": "3"},
    ), max_retries=0)
    with pytest.raises(RateLimitError) as caught:
        resolve(client.get(PRICE))
    assert caught.value.retry_after == 3
    assert "upgrade" not in str(caught.value).lower()


@pytest.mark.parametrize("method", ["get", "history", "iter_history", "bulk_pull"])
def test_withdrawn_datasets_rejected(make_client, method):
    def handler(request):
        pytest.fail("Withdrawn dataset must not issue an HTTP request")

    client = make_client(handler)
    with pytest.raises(ValidationError):
        result = resolve(getattr(client, method)(["USStockPrice"] if method == "bulk_pull" else "USStockPrice"))
        if method == "iter_history":
            list(result)


def test_sync_cache_retains_attribution(tmp_path):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, json={"data": [], "attribution": [A]})

    with Client("test-key", cache_dir=str(tmp_path)) as client:
        client._http.close()
        client._http = httpx.Client(base_url=client.base_url, transport=httpx.MockTransport(handler))
        assert client.get(PRICE, with_meta=True)["attribution"] == [A]
        assert client.get(PRICE, with_meta=True)["attribution"] == [A]
        assert len(calls) == 1
