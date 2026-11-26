"""Launch and kill a real kvstore server process for the Python test suites."""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DEFAULT_BIN = REPO / "build" / "kvstore"


def server_binary() -> Path:
    path = Path(os.environ.get("KVSTORE_BIN", DEFAULT_BIN))
    if not path.exists():
        raise SystemExit(f"server binary not found: {path} (build it, or set KVSTORE_BIN)")
    return path


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Server:
    def __init__(self, wal: Path, fsync: str = "always", port: int | None = None):
        self.wal = Path(wal)
        self.fsync = fsync
        self.port = port or free_port()
        self.proc: subprocess.Popen | None = None
        self.log = b""

    def start(self, expect_ready: bool = True, timeout: float = 10.0) -> "Server":
        self.proc = subprocess.Popen(
            [str(server_binary()), "--port", str(self.port), "--wal", str(self.wal),
             "--fsync", self.fsync],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if not expect_ready:
            return self
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                out = self.proc.stdout.read() if self.proc.stdout else b""
                raise RuntimeError(f"server exited with {self.proc.returncode}: {out.decode()}")
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=0.2):
                    return self
            except OSError:
                time.sleep(0.02)
        raise RuntimeError("server did not start listening")

    def wait_exit(self, timeout: float = 10.0) -> tuple[int, str]:
        assert self.proc is not None
        out, _ = self.proc.communicate(timeout=timeout)
        return self.proc.returncode, out.decode(errors="replace")

    def sigkill(self) -> None:
        assert self.proc is not None
        self.proc.send_signal(signal.SIGKILL)
        out, _ = self.proc.communicate(timeout=10)
        self.log = out

    def stop(self) -> str:
        if self.proc is None or self.proc.poll() is not None:
            return ""
        self.proc.send_signal(signal.SIGTERM)
        out, _ = self.proc.communicate(timeout=10)
        self.log = out
        return out.decode(errors="replace")
