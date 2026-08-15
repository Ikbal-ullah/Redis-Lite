"""A small, dependency-free client for the kvstore server.

Standard library only -- ``socket`` and nothing else. Drop this file next to
your code and import it.

    from redis_lite import Client

    with Client(host="127.0.0.1", port=6379) as client:
        client.set("user:1", "ikbal")
        print(client.get("user:1"))      # -> 'ikbal'
        client.expire("user:1", 60)
        client.delete("user:1")

Commands are sent as RESP arrays of bulk strings, so keys and values may
contain spaces, newlines, or arbitrary binary data.

Thread safety
-------------
A ``Client`` owns one socket and is **not** thread-safe. Give each thread its
own instance; that is how ``scripts/benchmark.py`` uses it. Sharing one client
across threads interleaves bytes from different commands on the same socket and
desynchronises the reply stream.
"""

from __future__ import annotations

import socket
from typing import Final, Iterator, List, Optional, Sequence, Union

__all__ = [
    "Client",
    "RedisLiteError",
    "ConnectionError",
    "ProtocolError",
    "CommandError",
]

Key = Union[str, bytes]
Value = Union[str, bytes, int, float]

_CRLF: Final[bytes] = b"\r\n"
_RECV_SIZE: Final[int] = 64 * 1024

# Refuse to allocate for a length the server should never send. A client is
# usually the trusting end of the connection, but "usually" is not a reason to
# let a bad reply exhaust memory.
_MAX_BULK: Final[int] = 512 * 1024 * 1024
_MAX_ARRAY: Final[int] = 1024 * 1024


class RedisLiteError(Exception):
    """Base class for every error raised by this module."""


# `ConnectionError` and the builtin of the same name are distinct types. The
# shadowing is deliberate and scoped to this module, where it reads naturally as
# `redis_lite.ConnectionError`. Internally we only ever catch `OSError`, which
# is the builtin's base class, so the two never get confused.
class ConnectionError(RedisLiteError):  # noqa: A001 - intentional, see above
    """The socket could not be established, or it dropped mid-conversation."""


class ProtocolError(RedisLiteError):
    """The server sent bytes that are not valid RESP."""


class CommandError(RedisLiteError):
    """The server understood the command and rejected it (a ``-ERR`` reply)."""


class _Incomplete(Exception):
    """Internal: the buffer does not yet hold a whole reply."""


def _parse(buf: bytes, start: int = 0, depth: int = 0) -> tuple[object, int]:
    """Parse one RESP value from ``buf[start:]``.

    Returns ``(value, end_index)``. Raises :class:`_Incomplete` if more bytes are
    needed, in which case the caller must read more and retry from the same
    offset -- nothing is consumed.
    """
    if depth > 32:
        raise ProtocolError("reply nested too deeply")
    if start >= len(buf):
        raise _Incomplete

    line_end = buf.find(_CRLF, start)
    if line_end == -1:
        # Bound the wait: a server dribbling digits forever must not grow the
        # buffer without limit.
        if len(buf) - start > 512:
            raise ProtocolError("reply line has no terminator")
        raise _Incomplete

    kind = buf[start : start + 1]
    head = buf[start + 1 : line_end]
    after_head = line_end + len(_CRLF)

    if kind == b"+":
        return head.decode("utf-8", "replace"), after_head

    if kind == b"-":
        return CommandError(head.decode("utf-8", "replace")), after_head

    if kind == b":":
        try:
            return int(head), after_head
        except ValueError:
            raise ProtocolError(f"invalid integer reply: {head!r}") from None

    if kind == b"$":
        try:
            length = int(head)
        except ValueError:
            raise ProtocolError(f"invalid bulk length: {head!r}") from None
        if length < 0:
            return None, after_head
        if length > _MAX_BULK:
            raise ProtocolError(f"bulk reply too large: {length}")
        end = after_head + length
        if len(buf) < end + len(_CRLF):
            raise _Incomplete
        return bytes(buf[after_head:end]), end + len(_CRLF)

    if kind == b"*":
        try:
            count = int(head)
        except ValueError:
            raise ProtocolError(f"invalid array length: {head!r}") from None
        if count < 0:
            return None, after_head
        if count > _MAX_ARRAY:
            raise ProtocolError(f"array reply too large: {count}")
        items: List[object] = []
        offset = after_head
        for _ in range(count):
            # An incomplete element makes the whole array incomplete; the
            # exception propagates and the caller retries from the start.
            item, offset = _parse(buf, offset, depth + 1)
            items.append(item)
        return items, offset

    raise ProtocolError(f"unknown reply type: {kind!r}")


def _encode_command(args: Sequence[Value]) -> bytes:
    """Encode arguments as a RESP array of bulk strings."""
    out = bytearray(b"*%d\r\n" % len(args))
    for arg in args:
        if isinstance(arg, bytes):
            raw = arg
        elif isinstance(arg, str):
            raw = arg.encode("utf-8")
        elif isinstance(arg, (int, float)):
            raw = str(arg).encode("ascii")
        else:
            raise TypeError(f"cannot encode argument of type {type(arg).__name__}")
        out += b"$%d\r\n" % len(raw)
        out += raw
        out += _CRLF
    return bytes(out)


class Client:
    """A synchronous connection to a kvstore server.

    Args:
        host: Server hostname or address.
        port: Server TCP port.
        timeout: Socket timeout in seconds for both connect and I/O.
            ``None`` blocks indefinitely.
        decode_responses: When true (the default), values come back as ``str``
            decoded with ``encoding``. Set false to receive raw ``bytes``.
        encoding: Codec used when ``decode_responses`` is true.
        connect_now: Connect during construction rather than lazily on the
            first command.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6379,
        timeout: Optional[float] = 5.0,
        decode_responses: bool = True,
        encoding: str = "utf-8",
        connect_now: bool = True,
    ) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout
        self.decode_responses = decode_responses
        self.encoding = encoding
        self._sock: Optional[socket.socket] = None
        self._buf = bytearray()
        if connect_now:
            self.connect()

    # -- connection management ---------------------------------------------

    def connect(self) -> None:
        """Open the socket. A no-op if already connected."""
        if self._sock is not None:
            return
        try:
            sock = socket.create_connection((self.host, self.port), self.timeout)
        except OSError as exc:
            raise ConnectionError(
                f"cannot connect to {self.host}:{self.port}: {exc}"
            ) from exc
        # Nagle's algorithm would hold our small request back for milliseconds
        # waiting for a companion packet that never comes. For a
        # request/response protocol it is pure added latency.
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._sock = sock
        self._buf.clear()

    def close(self) -> None:
        """Close the socket. Safe to call more than once."""
        if self._sock is not None:
            try:
                self._sock.close()
            finally:
                self._sock = None
                self._buf.clear()

    @property
    def connected(self) -> bool:
        return self._sock is not None

    def __enter__(self) -> "Client":
        self.connect()
        return self

    def __exit__(self, *_exc_info: object) -> None:
        self.close()

    def __repr__(self) -> str:
        state = "connected" if self.connected else "closed"
        return f"<redis_lite.Client {self.host}:{self.port} {state}>"

    # -- wire plumbing ------------------------------------------------------

    def _fill(self) -> None:
        """Read one chunk from the socket into the buffer."""
        assert self._sock is not None
        try:
            data = self._sock.recv(_RECV_SIZE)
        except socket.timeout as exc:
            raise ConnectionError(f"timed out reading from {self.host}:{self.port}") from exc
        except OSError as exc:
            raise ConnectionError(f"read failed: {exc}") from exc
        if not data:
            # An orderly FIN. Whatever we were waiting for is never arriving.
            self.close()
            raise ConnectionError("server closed the connection")
        self._buf += data

    def _read_reply(self) -> object:
        """Read exactly one reply, buffering across as many packets as it takes."""
        while True:
            try:
                value, end = _parse(self._buf, 0)
            except _Incomplete:
                self._fill()
                continue
            # Only now is it safe to retire the bytes. Surplus bytes stay in the
            # buffer for the next reply.
            del self._buf[:end]
            if isinstance(value, CommandError):
                raise value
            return value

    def execute(self, *args: Value) -> object:
        """Send an arbitrary command and return its decoded reply.

        The escape hatch for anything the typed helpers below do not cover.
        """
        if not args:
            raise ValueError("a command needs at least a name")
        if self._sock is None:
            self.connect()
        assert self._sock is not None
        try:
            # sendall loops internally: a single send() may transmit fewer bytes
            # than asked, and a half-sent command would desynchronise the stream.
            self._sock.sendall(_encode_command(args))
        except socket.timeout as exc:
            raise ConnectionError(f"timed out writing to {self.host}:{self.port}") from exc
        except OSError as exc:
            self.close()
            raise ConnectionError(f"write failed: {exc}") from exc
        return self._read_reply()

    def _decode(self, value: object) -> object:
        if isinstance(value, bytes) and self.decode_responses:
            return value.decode(self.encoding)
        return value

    # -- commands -----------------------------------------------------------

    def ping(self, message: Optional[Key] = None) -> Union[str, bytes]:
        """Check liveness. Returns ``'PONG'``, or ``message`` echoed back."""
        reply = self.execute("PING") if message is None else self.execute("PING", message)
        return self._decode(reply)  # type: ignore[return-value]

    def set(self, key: Key, value: Value) -> bool:
        """Store ``value`` under ``key``, clearing any existing TTL.

        Returns True; a failure raises instead of returning False.
        """
        return self.execute("SET", key, value) == "OK"

    def get(self, key: Key) -> Union[str, bytes, None]:
        """Return the value, or ``None`` if the key is absent or expired."""
        return self._decode(self.execute("GET", key))  # type: ignore[return-value]

    def delete(self, *keys: Key) -> int:
        """Delete one or more keys. Returns how many actually existed."""
        if not keys:
            raise ValueError("delete() needs at least one key")
        reply = self.execute("DEL", *keys)
        if not isinstance(reply, int):
            raise ProtocolError(f"DEL returned {reply!r}, expected an integer")
        return reply

    def expire(self, key: Key, seconds: int) -> bool:
        """Set a TTL. Returns False if the key does not exist.

        A non-positive ``seconds`` deletes the key immediately.
        """
        reply = self.execute("EXPIRE", key, int(seconds))
        if not isinstance(reply, int):
            raise ProtocolError(f"EXPIRE returned {reply!r}, expected an integer")
        return reply == 1

    # -- conveniences -------------------------------------------------------

    def __contains__(self, key: Key) -> bool:
        return self.get(key) is not None

    def __setitem__(self, key: Key, value: Value) -> None:
        self.set(key, value)

    def __getitem__(self, key: Key) -> Union[str, bytes]:
        value = self.get(key)
        if value is None:
            raise KeyError(key)
        return value

    def __delitem__(self, key: Key) -> None:
        if self.delete(key) == 0:
            raise KeyError(key)

    def pipeline(self, commands: Sequence[Sequence[Value]]) -> Iterator[object]:
        """Send several commands in one write, then yield their replies in order.

        Amortises the round-trip: N commands cost one network round-trip instead
        of N. Replies come back in the order the commands were sent, which is
        what makes the batching safe.

        An error reply raises at the point it is yielded, so wrap the iteration
        if you want to keep going past one.
        """
        if not commands:
            return
        if self._sock is None:
            self.connect()
        assert self._sock is not None
        payload = b"".join(_encode_command(command) for command in commands)
        try:
            self._sock.sendall(payload)
        except OSError as exc:
            self.close()
            raise ConnectionError(f"write failed: {exc}") from exc
        for _ in commands:
            yield self._decode(self._read_reply())


if __name__ == "__main__":  # pragma: no cover - a smoke check, not a test suite
    import sys

    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 6379

    with Client(host=host, port=port) as demo:
        print("ping   ->", demo.ping())
        print("set    ->", demo.set("sdk:demo", "hello world"))
        print("get    ->", demo.get("sdk:demo"))
        print("expire ->", demo.expire("sdk:demo", 60))
        print("delete ->", demo.delete("sdk:demo"))
        print("get    ->", demo.get("sdk:demo"))
