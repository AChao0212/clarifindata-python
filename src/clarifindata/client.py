"""clarifindata Python client — sync (`Client`) and async (`AsyncClient`).

    from clarifindata import Client, datasets
    cfd = Client(api_key="your_api_key")
    df = cfd.get(datasets.TaiwanStockPrice, stock_id="2330", start="2026-04-01", as_df=True)

Features: automatic retry (429/5xx, honours Retry-After), rate-limit awareness,
DataFrame output, date-chunked full-history streaming, optional disk cache,
typed errors, and an AsyncClient with the same surface.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from datetime import date, datetime
from typing import Any, Iterator

import httpx

from . import datasets as _ds
from .errors import (AuthError, ClarifindataError, NotFoundError, RateLimitError,
                     ValidationError)

try:
    from importlib.metadata import version as _pkg_version
    __version__ = _pkg_version("clarifindata")
except Exception:  # not installed (running from source)
    __version__ = "0.0.0+src"

log = logging.getLogger("clarifindata")
DEFAULT_BASE_URL = "https://api.clarifindata.com"
ROW_CAP = 10000               # server hard cap per request
_RETRY_STATUS = {429, 500, 502, 503, 504}


# ───────────────────────── shared helpers ─────────────────────────
def _mask(key: str) -> str:
    return f"{key[:8]}…{key[-2:]}" if len(key) > 10 else "***"


def _backoff(attempt: int, base: float, retry_after: float | None) -> float:
    if retry_after is not None:
        return retry_after
    # exponential + light deterministic jitter (no global RNG dependency)
    return min(60.0, base * (2 ** attempt)) * (1.0 + 0.1 * (attempt % 3))


def _suggest(status: int, dataset: str | None) -> str:
    if status in (401,):
        return "Check your api_key (Authorization: Bearer <key>)."
    if status == 403:
        return "Access denied; check the server detail and your account credentials."
    if status == 404:
        return "Unknown dataset/endpoint — see clarifindata.datasets.ALL or cfd.catalog()."
    if status in (400, 422):
        return "Bad params — check the dataset name (clarifindata.datasets.*), date format (YYYY-MM-DD), and limit ≤ 10000."
    if status == 429:
        return "Rate limit — the client retries automatically; lower concurrency or wait for the quota window."
    return "See https://clarifindata.com/docs"


def _err_from_response(r: httpx.Response, dataset: str | None) -> ClarifindataError:
    try:
        detail = r.json().get("detail")
    except Exception:
        detail = (r.text or "")[:200]
    kw = dict(dataset=dataset, status=r.status_code, detail=detail,
              suggestion=_suggest(r.status_code, dataset))
    s = r.status_code
    if s == 403:
        return AuthError("Forbidden", **kw)
    if s == 401:
        return AuthError("Unauthorized", **kw)
    if s == 404:
        return NotFoundError("Not found", **kw)
    if s == 429:
        ra = r.headers.get("retry-after")
        return RateLimitError("Rate limited", retry_after=float(ra) if ra else None, **kw)
    if s in (400, 422):
        return ValidationError("Invalid request", **kw)
    return ClarifindataError("HTTP error", **kw)


def _validate_dataset(dataset: str) -> None:
    if dataset not in _ds.ALL:
        # offer a close match for typos
        import difflib
        near = difflib.get_close_matches(dataset, _ds.ALL, n=1)
        hint = f" Did you mean '{near[0]}'?" if near else ""
        raise ValidationError(f"Unknown dataset '{dataset}'.{hint}",
                              dataset=dataset,
                              suggestion="Use clarifindata.datasets.* for valid names.")


def _params(stock_id, start, end, limit, cursor=None) -> dict[str, Any]:
    p: dict[str, Any] = {"limit": limit}
    if stock_id is not None:
        p["stock_id"] = stock_id
    if start is not None:
        p["start"] = str(start)
    if end is not None:
        p["end"] = str(end)
    if cursor is not None:
        p["cursor"] = cursor
    return p


def _to_df(rows: list[dict[str, Any]], attribution: list[dict[str, Any]]):
    try:
        import pandas as pd
    except ImportError as e:
        raise ClarifindataError(
            "pandas not installed", suggestion="pip install 'clarifindata[pandas]'") from e
    frame = pd.DataFrame(rows)
    frame.attrs["attribution"] = attribution
    return frame


def _merge_attribution(target: list[dict[str, Any]], incoming: list[dict[str, Any]]) -> None:
    """Keep the first statement for each source, in response order."""
    seen = {entry["source_id"] for entry in target}
    for entry in incoming:
        if entry["source_id"] not in seen:
            target.append(entry)
            seen.add(entry["source_id"])


def _result(body: dict[str, Any], as_df: bool, with_meta: bool):
    """Preserve metadata on request, and attribution on every DataFrame."""
    attribution = body.get("attribution", [])
    data = _to_df(body.get("data", []), attribution) if as_df else body.get("data", [])
    if with_meta:
        return {**body, "data": data, "attribution": attribution}
    return data


def _cache_path(cache_dir: str, method: str, url: str, params: dict) -> str:
    key = hashlib.sha256(f"{method}{url}{sorted(params.items())}".encode()).hexdigest()
    return os.path.join(cache_dir, key + ".json")


def _month_chunks(since: date, until: date):
    """Yield (chunk_start, chunk_end) spanning since..until, one calendar year
    each — small enough to stay under the 10000-row cap for one stock."""
    cur = since
    while cur <= until:
        end = min(date(cur.year, 12, 31), until)
        yield cur, end
        cur = date(cur.year + 1, 1, 1)


def _as_date(d) -> date:
    if isinstance(d, date):
        return d
    return datetime.strptime(str(d), "%Y-%m-%d").date()


# ───────────────────────── sync client ─────────────────────────
class Client:
    def __init__(self, api_key: str, *, base_url: str = DEFAULT_BASE_URL,
                 timeout: float = 30.0, max_retries: int = 4, backoff_base: float = 0.5,
                 cache_dir: str | None = None, cache_ttl: float = 86400.0,
                 warn_rate_limit_pct: float = 80.0) -> None:
        if not api_key:
            raise ValueError("api_key is required (get a free key at clarifindata.com/signup)")
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.cache_dir = cache_dir
        self.cache_ttl = cache_ttl
        self.warn_rate_limit_pct = warn_rate_limit_pct
        self.rate_limit_limit: int | None = None
        self.rate_limit_remaining: int | None = None
        if cache_dir:
            os.makedirs(cache_dir, exist_ok=True)
        try:
            self._http = httpx.Client(base_url=self.base_url, timeout=timeout, http2=True,
                                      headers=self._headers())
        except Exception:  # h2 not installed → HTTP/1.1
            self._http = httpx.Client(base_url=self.base_url, timeout=timeout,
                                      headers=self._headers())

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.api_key}",
                "User-Agent": f"clarifindata-py/{__version__}"}

    # lifecycle
    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def close(self) -> None:
        if getattr(self, "_http", None) is not None:
            self._http.close()

    def __repr__(self) -> str:
        rem = self.rate_limit_remaining
        return (f"Client(base_url={self.base_url!r}, key={_mask(self.api_key)}, "
                f"rate_limit_remaining={rem})")

    # core request with retry + rate-limit tracking + cache
    def _request(self, method: str, path: str, *, params: dict | None = None,
                 dataset: str | None = None) -> dict:
        params = params or {}
        cache_file = None
        if self.cache_dir and method == "GET":
            cache_file = _cache_path(self.cache_dir, method, path, params)
            if os.path.exists(cache_file) and (time.time() - os.path.getmtime(cache_file)) < self.cache_ttl:
                log.debug("cache hit %s", path)
                with open(cache_file) as f:
                    return json.load(f)
        last: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                log.debug("%s %s params=%s (attempt %d)", method, path, params, attempt)
                r = self._http.request(method, path, params=params)
                self._track_rate_limit(r)
                if r.status_code in _RETRY_STATUS and attempt < self.max_retries:
                    ra = r.headers.get("retry-after")
                    wait = _backoff(attempt, self.backoff_base, float(ra) if ra else None)
                    log.warning("HTTP %d on %s — retrying in %.1fs", r.status_code, path, wait)
                    time.sleep(wait)
                    continue
                if r.status_code >= 400:
                    raise _err_from_response(r, dataset)
                body = r.json()
                if cache_file is not None:
                    with open(cache_file, "w") as f:
                        json.dump(body, f)
                return body
            except (httpx.TimeoutException, httpx.TransportError) as e:
                last = e
                if attempt < self.max_retries:
                    wait = _backoff(attempt, self.backoff_base, None)
                    log.warning("network error on %s (%s) — retrying in %.1fs", path, e, wait)
                    time.sleep(wait)
                    continue
                raise ClarifindataError(f"network error after {self.max_retries} retries",
                                        dataset=dataset, suggestion="Check connectivity / base_url.") from e
        raise ClarifindataError("request failed", dataset=dataset) from last

    def _track_rate_limit(self, r: httpx.Response) -> None:
        h = r.headers
        if "x-ratelimit-limit" in h:
            try:
                self.rate_limit_limit = int(h["x-ratelimit-limit"])
                self.rate_limit_remaining = int(h["x-ratelimit-remaining"])
                used_pct = 100 * (1 - self.rate_limit_remaining / max(1, self.rate_limit_limit))
                if used_pct >= self.warn_rate_limit_pct:
                    log.warning("rate limit %.0f%% used (%d/%d remaining this window)",
                                used_pct, self.rate_limit_remaining, self.rate_limit_limit)
            except (ValueError, TypeError):
                pass

    # public API
    def catalog(self) -> list[dict[str, Any]]:
        """Public dataset catalog, including coverage, category and attribution."""
        return self._request("GET", "/v1/datasets").get("datasets", [])

    def get(self, dataset: str, *, stock_id: str | None = None,
            start: date | str | None = None, end: date | str | None = None,
            limit: int = 1000, as_df: bool = False, with_meta: bool = False):
        """Fetch rows, or a DataFrame with attribution in ``attrs``.

        ``with_meta=True`` returns the response dict, with rows or a DataFrame
        under ``data`` and source statements under ``attribution``.
        """
        _validate_dataset(dataset)
        if limit > ROW_CAP:
            raise ValidationError(f"limit {limit} exceeds server cap {ROW_CAP}",
                                  dataset=dataset, suggestion="Use iter_history() to stream large ranges.")
        body = self._request("GET", f"/v1/data/{dataset}",
                             params=_params(stock_id, start, end, limit), dataset=dataset)
        if len(body.get("data", [])) == limit and body.get("next_cursor") is None:
            log.warning("'%s' returned exactly limit=%d rows — result is likely TRUNCATED. "
                        "Raise limit or use iter_history().", dataset, limit)
        return _result(body, as_df, with_meta)

    def _pull_window(self, dataset: str, *, stock_id=None, start=None, end=None) -> dict:
        """Drain keyset pages and deduplicate attribution across every page."""
        _validate_dataset(dataset)
        out: dict[str, Any] = {"data": [], "attribution": []}
        cursor: str | None = None
        while True:
            body = self._request("GET", f"/v1/data/{dataset}",
                                 params=_params(stock_id, start, end, ROW_CAP, cursor),
                                 dataset=dataset)
            out["data"].extend(body.get("data", []))
            _merge_attribution(out["attribution"], body.get("attribution", []))
            cursor = body.get("next_cursor")
            if not cursor:
                return out

    def iter_history(self, dataset: str, *, stock_id: str | None = None,
                     since: date | str | None = None, until: date | str | None = None,
                     as_df: bool = False, with_meta: bool = False) -> Iterator:
        """Yield year-sized batches, draining every cursor page in each year.

        With no ``since``, drain the dataset as one batch. With ``with_meta``,
        each batch includes the union of its pages' attribution. Metadata and
        DataFrame modes also yield empty batches to retain source statements.
        """
        _validate_dataset(dataset)
        if since is None:
            body = self._pull_window(dataset, stock_id=stock_id)
            if body["data"] or with_meta or as_df:
                yield _result(body, as_df, with_meta)
            return
        s = _as_date(since)
        u = _as_date(until) if until else date.today()
        for cs, ce in _month_chunks(s, u):
            body = self._pull_window(dataset, stock_id=stock_id, start=cs, end=ce)
            if body["data"] or with_meta or as_df:
                yield _result(body, as_df, with_meta)

    def history(self, dataset: str, **kw):
        """Collect all history; metadata unions sources across years and pages."""
        as_df = kw.pop("as_df", False)
        with_meta = kw.pop("with_meta", False)
        out: dict[str, Any] = {"data": [], "attribution": []}
        for batch in self.iter_history(dataset, with_meta=True, **kw):
            out["data"].extend(batch["data"])
            _merge_attribution(out["attribution"], batch["attribution"])
        return _result(out, as_df, with_meta)

    def bulk_pull(self, datasets: list[str], *, start: date | str | None = None,
                  end: date | str | None = None, stock_id: str | None = None,
                  as_df: bool = False, with_meta: bool = False) -> dict[str, Any]:
        """Return {dataset: rows|DataFrame}, draining every page.

        With metadata, return ``{"data": mapping, "attribution": sources}``,
        where sources is the union across all datasets and pages.
        """
        out: dict[str, Any] = {}
        attribution: list[dict[str, Any]] = []
        for d in datasets:
            if self.rate_limit_remaining is not None and self.rate_limit_remaining < 5:
                log.warning("rate limit nearly exhausted — pausing 5s")
                time.sleep(5)
            body = self._pull_window(d, stock_id=stock_id, start=start, end=end)
            out[d] = _result(body, as_df, False)
            _merge_attribution(attribution, body["attribution"])
        return {"data": out, "attribution": attribution} if with_meta else out


# ───────────────────────── async client ─────────────────────────
class AsyncClient:
    """Async mirror of Client — same surface, for concurrent fetches.

        async with AsyncClient(api_key=...) as cfd:
            rows = await cfd.get(datasets.TaiwanStockPrice, stock_id="2330")
            results = await asyncio.gather(*[cfd.get(..., stock_id=s) for s in stocks])
    """

    def __init__(self, api_key: str, *, base_url: str = DEFAULT_BASE_URL,
                 timeout: float = 30.0, max_retries: int = 4, backoff_base: float = 0.5,
                 warn_rate_limit_pct: float = 80.0) -> None:
        if not api_key:
            raise ValueError("api_key is required")
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.warn_rate_limit_pct = warn_rate_limit_pct
        self.rate_limit_limit: int | None = None
        self.rate_limit_remaining: int | None = None
        hdrs = {"Authorization": f"Bearer {api_key}", "User-Agent": f"clarifindata-py/{__version__}"}
        try:
            self._http = httpx.AsyncClient(base_url=self.base_url, timeout=timeout, http2=True, headers=hdrs)
        except Exception:
            self._http = httpx.AsyncClient(base_url=self.base_url, timeout=timeout, headers=hdrs)

    async def __aenter__(self) -> "AsyncClient":
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    async def close(self) -> None:
        await self._http.aclose()

    def __repr__(self) -> str:
        return (f"AsyncClient(base_url={self.base_url!r}, key={_mask(self.api_key)}, "
                f"rate_limit_remaining={self.rate_limit_remaining})")

    def _track(self, r: httpx.Response) -> None:
        Client._track_rate_limit(self, r)  # reuse the same logic

    async def _request(self, method: str, path: str, *, params=None, dataset=None) -> dict:
        import asyncio
        params = params or {}
        last: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                r = await self._http.request(method, path, params=params)
                self._track(r)
                if r.status_code in _RETRY_STATUS and attempt < self.max_retries:
                    ra = r.headers.get("retry-after")
                    await asyncio.sleep(_backoff(attempt, self.backoff_base, float(ra) if ra else None))
                    continue
                if r.status_code >= 400:
                    raise _err_from_response(r, dataset)
                return r.json()
            except (httpx.TimeoutException, httpx.TransportError) as e:
                last = e
                if attempt < self.max_retries:
                    await asyncio.sleep(_backoff(attempt, self.backoff_base, None))
                    continue
                raise ClarifindataError("network error after retries", dataset=dataset) from e
        raise ClarifindataError("request failed", dataset=dataset) from last

    async def catalog(self) -> list[dict[str, Any]]:
        """Public dataset catalog, including attribution."""
        return (await self._request("GET", "/v1/datasets")).get("datasets", [])

    async def get(self, dataset: str, *, stock_id=None, start=None, end=None,
                  limit: int = 1000, as_df: bool = False, with_meta: bool = False):
        """Async mirror of Client.get, including metadata and DataFrame attrs."""
        _validate_dataset(dataset)
        if limit > ROW_CAP:
            raise ValidationError(f"limit {limit} exceeds {ROW_CAP}", dataset=dataset)
        body = await self._request("GET", f"/v1/data/{dataset}",
                                   params=_params(stock_id, start, end, limit), dataset=dataset)
        if len(body.get("data", [])) == limit and body.get("next_cursor") is None:
            log.warning("'%s' returned exactly limit=%d — likely truncated.", dataset, limit)
        return _result(body, as_df, with_meta)

    async def _pull_window(self, dataset: str, *, stock_id=None, start=None, end=None) -> dict:
        """Drain keyset pages and union their attribution."""
        _validate_dataset(dataset)
        out: dict[str, Any] = {"data": [], "attribution": []}
        cursor: str | None = None
        while True:
            body = await self._request("GET", f"/v1/data/{dataset}",
                                       params=_params(stock_id, start, end, ROW_CAP, cursor),
                                       dataset=dataset)
            out["data"].extend(body.get("data", []))
            _merge_attribution(out["attribution"], body.get("attribution", []))
            cursor = body.get("next_cursor")
            if not cursor:
                return out

    async def iter_history(self, dataset: str, *, stock_id=None,
                           since: date | str | None = None, until: date | str | None = None,
                           as_df: bool = False, with_meta: bool = False) -> list:
        """Return a list of batches, as before; this is not an async iterator.

        Each metadata batch unions sources across its pages. Empty batches are
        retained in metadata and DataFrame modes.
        """
        _validate_dataset(dataset)
        batches: list = []
        if since is None:
            body = await self._pull_window(dataset, stock_id=stock_id)
            if body["data"] or with_meta or as_df:
                batches.append(_result(body, as_df, with_meta))
            return batches
        s = _as_date(since)
        u = _as_date(until) if until else date.today()
        for cs, ce in _month_chunks(s, u):
            body = await self._pull_window(dataset, stock_id=stock_id, start=cs, end=ce)
            if body["data"] or with_meta or as_df:
                batches.append(_result(body, as_df, with_meta))
        return batches

    async def history(self, dataset: str, **kw):
        """Collect all history, preserving attribution across years and pages."""
        as_df = kw.pop("as_df", False)
        with_meta = kw.pop("with_meta", False)
        out: dict[str, Any] = {"data": [], "attribution": []}
        for batch in await self.iter_history(dataset, with_meta=True, **kw):
            out["data"].extend(batch["data"])
            _merge_attribution(out["attribution"], batch["attribution"])
        return _result(out, as_df, with_meta)

    async def bulk_pull(self, datasets: list[str], *, start: date | str | None = None,
                        end: date | str | None = None, stock_id: str | None = None,
                        as_df: bool = False, with_meta: bool = False) -> dict[str, Any]:
        """Fetch concurrently; metadata unions sources across datasets and pages."""
        import asyncio
        bodies = await asyncio.gather(*[
            self._pull_window(d, stock_id=stock_id, start=start, end=end)
            for d in datasets
        ])
        out: dict[str, Any] = {}
        attribution: list[dict[str, Any]] = []
        for d, body in zip(datasets, bodies):
            out[d] = _result(body, as_df, False)
            _merge_attribution(attribution, body["attribution"])
        return {"data": out, "attribution": attribution} if with_meta else out
