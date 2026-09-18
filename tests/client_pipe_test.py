#!/usr/bin/env python3
"""Regression test: EOF on stdin must drain the interactive client's write queue."""
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


def receive_message(sock):
    def exact(size):
        data = bytearray()
        while len(data) < size:
            chunk = sock.recv(size - len(data))
            if not chunk: raise RuntimeError("peer closed before its complete message arrived")
            data.extend(chunk)
        return bytes(data)
    return exact(struct.unpack("!I", exact(4))[0])


parser = argparse.ArgumentParser()
parser.add_argument("--server", required=True)
parser.add_argument("--client", required=True)
parser.add_argument("--port", type=int, default=0, help="0 lets the OS choose the port")
args = parser.parse_args()
server, port = start_server(args.server, args.port)
receiver = None
try:
    receiver = socket.create_connection(("127.0.0.1", port), timeout=2)
    receiver.settimeout(3)
    time.sleep(.1)
    result = subprocess.run(
        [args.client, "127.0.0.1", str(port)],
        input=b"message from piped stdin\nfinal unterminated message",
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5, check=False,
    )
    assert result.returncode == 0, result.stderr.decode()
    assert receive_message(receiver) == b"message from piped stdin"
    assert receive_message(receiver) == b"final unterminated message"
finally:
    if receiver: receiver.close()
    server.terminate(); server.wait(timeout=5)
