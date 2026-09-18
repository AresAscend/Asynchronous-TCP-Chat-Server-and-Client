#!/usr/bin/env python3
"""Regression test for server-side slow-peer backpressure eviction."""
import argparse
import socket
import struct
import subprocess
import time


def start_server(path, requested_port):
    server = subprocess.Popen([path, str(requested_port), "2"], stdout=subprocess.PIPE, text=True)
    line = server.stdout.readline().strip()
    if not line.startswith("Listening on port "):
        server.terminate()
        raise RuntimeError(f"server did not start: {line}")
    return server, int(line.rsplit(" ", 1)[1])


def send_message(sock, payload):
    sock.sendall(struct.pack("!I", len(payload)) + payload)


def receive_exact(sock, size):
    result = bytearray()
    while len(result) < size:
        chunk = sock.recv(size - len(result))
        if not chunk:
            raise RuntimeError("peer closed during read")
        result.extend(chunk)
    return bytes(result)


def receive_message(sock):
    return receive_exact(sock, struct.unpack("!I", receive_exact(sock, 4))[0])


def peer_was_evicted(sock, timeout):
    deadline = time.monotonic() + timeout
    sock.settimeout(0.1)
    while time.monotonic() < deadline:
        try:
            if not sock.recv(64 * 1024):
                return True
        except (ConnectionResetError, ConnectionAbortedError):
            return True
        except socket.timeout:
            pass
    return False


parser = argparse.ArgumentParser()
parser.add_argument("--server", required=True)
parser.add_argument("--port", type=int, default=0, help="0 lets the OS choose the port")
args = parser.parse_args()
server, port = start_server(args.server, args.port)
slow = sender = healthy = None
try:
    slow = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Keep the advertised receive window small so the first async write remains
    # pending. Later broadcasts then accumulate in the session's application
    # queue; a full kernel buffer blocks a write but does not itself cause an
    # error while this client remains connected.
    slow.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
    slow.settimeout(3)
    slow.connect(("127.0.0.1", port))
    sender = socket.create_connection(("127.0.0.1", port), timeout=3)
    healthy = socket.create_connection(("127.0.0.1", port), timeout=3)
    sender.settimeout(5)
    healthy.settimeout(5)
    time.sleep(0.1)

    payload = b"x" * (64 * 1024)
    # The queue rejects the next enqueue once it already contains 128 frames.
    # Reading the healthy peer after every send keeps it below that threshold.
    for _ in range(256):
        send_message(sender, payload)
        assert receive_message(healthy) == payload

    # Leave room for ThreadSanitizer's scheduling overhead in CI.
    assert peer_was_evicted(slow, timeout=15), "slow peer was not evicted"
    send_message(sender, b"server survived slow peer eviction")
    assert receive_message(healthy) == b"server survived slow peer eviction"
finally:
    for client in (slow, sender, healthy):
        if client:
            client.close()
    server.terminate()
    server.wait(timeout=5)
