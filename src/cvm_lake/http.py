"""HTTP access with retries.

Government sources go down. Callers skip and warn on :class:`SourceUnavailableError` instead
of failing the run.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Self

import httpx
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential
from tenacity.wait import wait_base

from cvm_lake import __version__

USER_AGENT = f"cvm-fundwatch/{__version__}"
RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})


class SourceUnavailableError(Exception):
    """The source could not be reached or refused the request, after retries."""


class NotFoundError(Exception):
    """HTTP 404. For CVM this means "not published (yet)", not an outage."""


class _RetryableError(Exception):
    pass


@dataclass(frozen=True)
class RemoteFileInfo:
    url: str
    last_modified: str | None
    etag: str | None
    content_length: int | None


@dataclass(frozen=True)
class Download:
    url: str
    path: Path
    sha256: str
    size: int
    last_modified: str | None
    etag: str | None


def _content_length(resp: httpx.Response) -> int | None:
    value = resp.headers.get("content-length")
    return int(value) if value and value.isdigit() else None


class HttpClient:
    def __init__(
        self,
        *,
        timeout: float = 120.0,
        max_attempts: int = 5,
        wait: wait_base | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._client = httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": USER_AGENT},
            transport=transport,
        )
        self._max_attempts = max_attempts
        self._wait = wait if wait is not None else wait_exponential(multiplier=2, min=2, max=60)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    def _retry[T](self, attempt: Callable[[], T]) -> T:
        retrying = Retrying(
            stop=stop_after_attempt(self._max_attempts),
            wait=self._wait,
            retry=retry_if_exception_type(_RetryableError),
            reraise=True,
        )
        try:
            return retrying(attempt)
        except _RetryableError as exc:
            raise SourceUnavailableError(f"{exc} (after {self._max_attempts} attempts)") from exc

    @staticmethod
    def _check(resp: httpx.Response) -> None:
        url, code = resp.request.url, resp.status_code
        if code == 404:
            raise NotFoundError(str(url))
        if code in RETRYABLE_STATUS:
            raise _RetryableError(f"HTTP {code} for {url}")
        if code >= 400:
            # A 403 from a WAF and similar: not worth retrying, but still an outage, not a bug.
            raise SourceUnavailableError(f"HTTP {code} for {url}")

    def head(self, url: str) -> RemoteFileInfo:
        def attempt() -> RemoteFileInfo:
            try:
                resp = self._client.head(url)
            except httpx.TransportError as exc:
                raise _RetryableError(f"{type(exc).__name__} for {url}: {exc}") from exc
            self._check(resp)
            return RemoteFileInfo(
                url=url,
                last_modified=resp.headers.get("last-modified"),
                etag=resp.headers.get("etag"),
                content_length=_content_length(resp),
            )

        return self._retry(attempt)

    def get_bytes(self, url: str) -> bytes:
        def attempt() -> bytes:
            try:
                resp = self._client.get(url)
            except httpx.TransportError as exc:
                raise _RetryableError(f"{type(exc).__name__} for {url}: {exc}") from exc
            self._check(resp)
            return resp.content

        return self._retry(attempt)

    def download(self, url: str, dest: Path) -> Download:
        """Stream ``url`` to ``dest`` and hash it. Partial files never reach ``dest``."""
        dest.parent.mkdir(parents=True, exist_ok=True)
        part = dest.with_name(dest.name + ".part")

        def attempt() -> Download:
            digest = hashlib.sha256()
            size = 0
            try:
                with self._client.stream("GET", url) as resp:
                    self._check(resp)
                    expected = _content_length(resp)
                    with part.open("wb") as fh:
                        for chunk in resp.iter_bytes(1 << 20):
                            fh.write(chunk)
                            digest.update(chunk)
                            size += len(chunk)
                    last_modified = resp.headers.get("last-modified")
                    etag = resp.headers.get("etag")
            except httpx.TransportError as exc:
                raise _RetryableError(f"{type(exc).__name__} for {url}: {exc}") from exc
            if expected is not None and size != expected:
                raise _RetryableError(f"truncated download of {url}: {size} of {expected} bytes")
            part.replace(dest)
            return Download(url, dest, digest.hexdigest(), size, last_modified, etag)

        try:
            return self._retry(attempt)
        finally:
            part.unlink(missing_ok=True)
