#!/usr/bin/env python3
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


def send_message(sock, payload): sock.sendall(struct.pack("!I", len(payload)) + payload)
def receive_exact(sock, size):
    result = bytearray()
    while len(result) < size:
        chunk = sock.recv(size - len(result))
        if not chunk: raise RuntimeError("peer closed during read")
        result.extend(chunk)
    return bytes(result)
def receive_message(sock): return receive_exact(sock, struct.unpack("!I", receive_exact(sock, 4))[0])


parser = argparse.ArgumentParser()
parser.add_argument("--server", required=True)
parser.add_argument("--port", type=int, default=0, help="0 lets the OS choose the port")
args = parser.parse_args()
server, port = start_server(args.server, args.port)
try:
    first = socket.create_connection(("127.0.0.1", port), timeout=2)
    second = socket.create_connection(("127.0.0.1", port), timeout=2)
    first.settimeout(2); second.settimeout(2); time.sleep(.1)
    send_message(first, b"small payload")
    assert receive_message(second) == b"small payload"
    payload = b"x" * (64 * 1024)
    send_message(first, payload)
    assert receive_message(second) == payload
    first.close()
    third = socket.create_connection(("127.0.0.1", port), timeout=2)
    third.settimeout(2)
    time.sleep(.1)
    send_message(second, b"server survived disconnect")
    assert receive_message(third) == b"server survived disconnect"
    # TCP may fragment a header arbitrarily; an oversized declared body must
    # close only the malformed peer, not destabilize the room.
    malformed = socket.create_connection(("127.0.0.1", port), timeout=2)
    malformed.settimeout(2)
    for byte in struct.pack("!I", 64 * 1024 + 1):
        malformed.sendall(bytes([byte]))
    assert malformed.recv(1) == b""
    malformed.close()
    send_message(second, b"server survived malformed frame")
    assert receive_message(third) == b"server survived malformed frame"
    second.close(); third.close()
finally:
    server.terminate(); server.wait(timeout=5)
