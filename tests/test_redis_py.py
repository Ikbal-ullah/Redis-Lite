"""Live compatibility with the official redis-py client.

    KVSTORE_BIN=build/kvstore .venv/bin/pytest tests/test_redis_py.py -v
"""

from __future__ import annotations

import time

import pytest
import redis

from kvserver import Server


@pytest.fixture(params=["no", "group", "always"])
def server(request, tmp_path):
    srv = Server(tmp_path / "wal.log", fsync=request.param).start()
    yield srv
    srv.stop()


@pytest.fixture
def client(server):
    # The server speaks RESP2 only; pin it so redis-py does not probe HELLO.
    r = redis.Redis(host="127.0.0.1", port=server.port, socket_timeout=5, protocol=2)
    yield r
    r.close()


def test_default_client_falls_back_to_resp2(server, caplog):
    # redis-py's default first tries HELLO 3; an error reply must make it fall
    # back to RESP2 rather than fail.
    r = redis.Redis(host="127.0.0.1", port=server.port, socket_timeout=5)
    assert r.ping() is True
    assert "Falling back to RESP2" in caplog.text  # redis-py logs the fallback
    assert r.set("k", "v") and r.get("k") == b"v"
    r.close()


def test_ping_set_get_del(client):
    assert client.ping() is True
    assert client.set("k", "v") is True
    assert client.get("k") == b"v"
    assert client.delete("k", "missing") == 1
    assert client.get("k") is None


def test_set_ex_px_and_ttl(client):
    assert client.set("ex", "1", ex=100)
    assert client.ttl("ex") == 100
    assert client.set("px", "1", px=150)
    assert client.get("px") == b"1"
    time.sleep(0.25)
    assert client.get("px") is None
    assert client.ttl("px") == -2
    client.set("plain", "1")
    assert client.ttl("plain") == -1


def test_expire(client):
    client.set("k", "v")
    assert client.expire("k", 50) is True
    assert client.ttl("k") == 50
    assert client.expire("missing", 50) is False
    assert client.expire("k", -1) is True
    assert client.get("k") is None


def test_errors_are_raised_not_fatal(client):
    with pytest.raises(redis.ResponseError, match="invalid expire time"):
        client.set("k", "v", ex=0)
    with pytest.raises(redis.ResponseError, match="unknown command"):
        client.execute_command("FLUSHALL")
    assert client.ping()


def test_pipeline_of_1000_commands(client):
    pipe = client.pipeline(transaction=False)
    for i in range(500):
        pipe.set(f"p:{i}", i)
    for i in range(500):
        pipe.get(f"p:{i}")
    results = pipe.execute()
    assert results[:500] == [True] * 500
    assert results[500:] == [str(i).encode() for i in range(500)]


def test_binary_safe_values(client):
    blob = bytes(range(256)) * 64
    client.set(b"\x00bin\r\n", blob)
    assert client.get(b"\x00bin\r\n") == blob


def test_compact_keeps_data(client):
    for i in range(300):
        client.set("same", i)
    client.set("ttl", "v", ex=500)
    assert client.execute_command("COMPACT") == b"OK"
    assert client.get("same") == b"299"
    assert client.ttl("ttl") == 500
