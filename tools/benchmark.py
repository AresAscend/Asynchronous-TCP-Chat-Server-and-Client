#!/usr/bin/env python3
"""Loopback fan-out benchmark. Pipelines sends to exercise queued async writes."""
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


def send(sock, body): sock.sendall(struct.pack("!I", len(body)) + body)
def exact(sock, count):
    data = bytearray()
    while len(data) < count:
        chunk = sock.recv(count - len(data))
        if not chunk: raise RuntimeError("peer closed")
        data.extend(chunk)
    return bytes(data)
def receive(sock): return exact(sock, struct.unpack("!I", exact(sock, 4))[0])


parser = argparse.ArgumentParser()
parser.add_argument("--server", required=True)
parser.add_argument("--clients", type=int, default=5)
parser.add_argument("--messages", type=int, default=1000)
parser.add_argument("--inflight", type=int, default=64, help="messages pipelined per batch (must be <= 128)")
parser.add_argument("--port", type=int, default=0, help="0 lets the OS choose the port")
parser.add_argument("--latency", action="store_true", help="measure sequential fan-out completion latency percentiles")
args = parser.parse_args()
if args.clients < 2: parser.error("--clients must be at least 2")
if not 1 <= args.inflight <= 128: parser.error("--inflight must be between 1 and 128")
server, port = start_server(args.server, args.port)
clients = []
try:
    clients = [socket.create_connection(("127.0.0.1", port), timeout=3) for _ in range(args.clients)]
    for client in clients: client.settimeout(10)
    time.sleep(.1)
    if args.latency:
        latencies = []
        for i in range(args.messages):
            body = f"latency-{i}".encode()
            t0 = time.perf_counter()
            send(clients[0], body)
            for receiver in clients[1:]:
                if receive(receiver) != body: raise RuntimeError("payload mismatch")
            latencies.append((time.perf_counter() - t0) * 1000)
        latencies.sort()
        n = len(latencies)
        p50 = latencies[int(n * 0.50)]
        p95 = latencies[int(n * 0.95)]
        p99 = latencies[int(n * 0.99)]
        avg = sum(latencies) / n
        print(f"messages: {args.messages}; clients: {args.clients}; fan-out receivers: {args.clients - 1}")
        print(f"fan-out latency (avg): {avg:.3f} ms")
        print(f"fan-out latency (p50 / p95 / p99): {p50:.3f} ms / {p95:.3f} ms / {p99:.3f} ms")
    else:
        bodies = [f"benchmark-{number}".encode() for number in range(args.messages)]
        started = time.perf_counter()
        for first in range(0, len(bodies), args.inflight):
            batch = bodies[first:first + args.inflight]
            for body in batch: send(clients[0], body)
            for receiver in clients[1:]:
                for body in batch:
                    if receive(receiver) != body: raise RuntimeError("payload mismatch")
        elapsed = time.perf_counter() - started
        deliveries = args.messages * (args.clients - 1)
        print(f"messages: {args.messages}; clients: {args.clients}; deliveries: {deliveries}")
        print(f"fan-out throughput: {deliveries / elapsed:.0f} deliveries/sec")
        print(f"fan-out batch completion: {elapsed * 1_000:.3f} ms")
finally:
    for client in clients: client.close()
    server.terminate(); server.wait(timeout=5)
