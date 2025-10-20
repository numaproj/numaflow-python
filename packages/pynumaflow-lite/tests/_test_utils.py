import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional

import pytest


def _wait_for_socket(path: Path, timeout: float = 10.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if path.exists():
            try:
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
                    s.settimeout(0.2)
                    s.connect(str(path))
                return
            except OSError:
                pass
        time.sleep(0.1)
    raise TimeoutError(f"Socket {path} not ready after {timeout}s")


def run_python_server_with_rust_client(
        script: str,
        sock_path: Path,
        server_info_path: Path,
        rust_bin_name: str,
        rust_bin_args: Optional[List[str]] = None,
        socket_timeout: float = 20.0,
        rust_timeout: float = 60.0,
        server_shutdown_timeout: float = 15.0,
) -> None:
    """
    Generic test runner for Python server + Rust client integration tests.

    Args:
        script: Name of the Python script to run (e.g., "map_cat.py")
        sock_path: Path to the Unix socket
        server_info_path: Path to the server info file
        rust_bin_name: Name of the Rust binary to run (e.g., "test_map")
        rust_bin_args: Optional additional arguments to pass to the Rust binary
        socket_timeout: Timeout for waiting for socket to be ready
        rust_timeout: Timeout for Rust client execution
        server_shutdown_timeout: Timeout for server graceful shutdown
    """
    # Ensure clean socket state
    for p in [sock_path, server_info_path]:
        try:
            if p.exists():
                p.unlink()
        except FileNotFoundError:
            pass

    # Start Python server
    tests_dir = Path(__file__).resolve().parent
    examples_dir = tests_dir / "examples"
    script_path = examples_dir / script
    assert script_path.exists(), f"Missing script: {script_path}"

    # Cargo needs to run from the pynumaflow-lite root (parent of tests)
    cargo_root = tests_dir.parent

    env = os.environ.copy()
    py_cmd = [sys.executable, "-u", str(script_path)]
    server = subprocess.Popen(
        py_cmd,
        cwd=str(cargo_root),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
        preexec_fn=os.setsid if hasattr(os, "setsid") else None,
    )

    try:
        _wait_for_socket(sock_path, timeout=socket_timeout)

        # Run Rust client bin
        rust_cmd = ["cargo", "run", "--quiet", "--bin", rust_bin_name]
        if rust_bin_args:
            rust_cmd.extend(["--"] + rust_bin_args)

        rust = subprocess.run(
            rust_cmd, cwd=str(cargo_root), capture_output=True, text=True, env=env, timeout=rust_timeout
        )
        if rust.returncode != 0:
            # Dump helpful logs for debugging
            server_logs = server.stdout.read() if server.stdout else ""
            pytest.fail(
                f"Rust client failed: code={rust.returncode}\nStdout:\n{rust.stdout}\nStderr:\n{rust.stderr}\nServer logs so far:\n{server_logs}"
            )

    finally:
        # Request graceful shutdown via SIGINT
        try:
            if server.poll() is None:
                if hasattr(os, "killpg") and server.pid:
                    os.killpg(os.getpgid(server.pid), signal.SIGINT)
                else:
                    server.send_signal(signal.SIGINT)
        except Exception:
            pass

        # Wait for server to exit
        try:
            server.wait(timeout=server_shutdown_timeout)
        except subprocess.TimeoutExpired:
            try:
                if hasattr(os, "killpg") and server.pid:
                    os.killpg(os.getpgid(server.pid), signal.SIGKILL)
                else:
                    server.kill()
            except Exception:
                pass

    assert server.returncode == 0, f"Server did not exit cleanly, code={server.returncode}"
