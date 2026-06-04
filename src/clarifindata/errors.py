"""Typed exceptions with actionable suggestions (instead of raw httpx errors)."""
from __future__ import annotations


class ClarifindataError(Exception):
    """Base error. Carries the dataset, HTTP status, server detail and a hint."""

    def __init__(self, message: str, *, dataset: str | None = None,
                 status: int | None = None, detail: str | None = None,
                 suggestion: str | None = None) -> None:
        self.dataset = dataset
        self.status = status
        self.detail = detail
        self.suggestion = suggestion
        parts = [message]
        if dataset:
            parts.append(f"dataset={dataset}")
        if status:
            parts.append(f"HTTP {status}")
        if detail:
            parts.append(f"detail={detail}")
        if suggestion:
            parts.append(f"\n  → {suggestion}")
        super().__init__(" ".join(parts))


class AuthError(ClarifindataError):
    """401/403 — bad/missing key, or the dataset needs a higher tier."""


class TierError(AuthError):
    """403 — your tier can't access this dataset/endpoint."""


class RateLimitError(ClarifindataError):
    """429 — rate limit hit (after retries were exhausted)."""

    def __init__(self, message: str, *, retry_after: float | None = None, **kw) -> None:
        self.retry_after = retry_after
        super().__init__(message, **kw)


class NotFoundError(ClarifindataError):
    """404 — unknown dataset/endpoint."""


class ValidationError(ClarifindataError):
    """400/422 — bad parameters (e.g. dataset typo, limit > 10000)."""
