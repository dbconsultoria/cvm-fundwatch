import socket

import pytest


def test_network_is_blocked() -> None:
    with pytest.raises(RuntimeError, match="Network access is not allowed"):
        socket.create_connection(("example.com", 443), timeout=1)
