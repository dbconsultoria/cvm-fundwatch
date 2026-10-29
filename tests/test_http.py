import hashlib
from pathlib import Path

import httpx
import pytest

from cvm_lake.http import NotFoundError, SourceUnavailableError

from fakes import FakeServer

URL = "https://example.test/file.zip"


def test_head_returns_change_headers() -> None:
    server = FakeServer()
    server.add(URL, b"abc", last_modified="Mon, 01 Jan 2026 00:00:00 GMT", etag='"e1"')
    info = server.client().head(URL)
    assert info.last_modified == "Mon, 01 Jan 2026 00:00:00 GMT"
    assert info.etag == '"e1"'


def test_retries_transient_errors_then_succeeds(tmp_path: Path) -> None:
    server = FakeServer()
    server.add(URL, b"payload")
    server.fail(URL, 503, httpx.ConnectError("boom"), 429)
    dl = server.client(max_attempts=4).download(URL, tmp_path / "f.zip")
    assert (tmp_path / "f.zip").read_bytes() == b"payload"
    assert dl.size == 7
    assert dl.sha256 == hashlib.sha256(b"payload").hexdigest()
    assert server.count("GET", URL) == 4


def test_gives_up_after_max_attempts(tmp_path: Path) -> None:
    server = FakeServer()
    server.add(URL, b"payload")
    server.fail_forever(URL, 502)
    with pytest.raises(SourceUnavailableError, match="after 3 attempts"):
        server.client(max_attempts=3).download(URL, tmp_path / "f.zip")
    assert server.count("GET", URL) == 3
    assert not list(tmp_path.iterdir()), "no partial file may be left behind"


def test_404_is_not_retried() -> None:
    server = FakeServer()
    with pytest.raises(NotFoundError):
        server.client(max_attempts=5).head(URL)
    assert server.count("HEAD", URL) == 1


def test_403_is_unavailable_without_retry() -> None:
    server = FakeServer()
    server.add(URL, b"x")
    server.fail(URL, 403)
    with pytest.raises(SourceUnavailableError, match="HTTP 403"):
        server.client(max_attempts=5).get_bytes(URL)
    assert server.count("GET", URL) == 1
