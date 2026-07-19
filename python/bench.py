#!/usr/bin/env python3
"""Benchmarks for the Python client, mirroring tests/bench_broker.cpp so the two
are comparable. Launches its own broker_daemon; frames need the ring lib:
    make daemon libshm_ring && python3 python/bench.py
"""
import os
import queue
import struct
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from broker import Node

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DAEMON = os.path.join(ROOT, "bin", "broker_daemon")
HAVE_RING = os.path.exists(os.path.join(ROOT, "bin", "libshm_ring.so"))
_U32 = struct.Struct("=I")


def report_lat(name, samples):
    s = sorted(samples)
    n = len(s)
    p = lambda q: s[int(q * (n - 1))] * 1e6
    print(f"  {name:<22} n={n:<6} p50={p(.5):7.2f}  p90={p(.9):7.2f}  "
          f"p99={p(.99):7.2f}  mean={sum(s) / n * 1e6:7.2f} us")


def bench_pubsub_latency(sock):
    a, b = Node("a"), Node("b")
    a.connect(sock)
    b.connect(sock)
    q = queue.SimpleQueue()
    a.subscribe("pong", lambda p: q.put(1))
    b.subscribe("ping", lambda p: b.publish("pong", p))  # echo
    a.sync()
    b.sync()

    N, warm = 3000, 300
    lat = []
    for i in range(1, N + warm + 1):
        payload = _U32.pack(i)
        t0 = time.perf_counter()
        a.publish("ping", payload)
        q.get()
        dt = time.perf_counter() - t0
        if i > warm:
            lat.append(dt)
    report_lat("pub/sub RTT", lat)
    a.close()
    b.close()


def bench_pubsub_throughput(sock):
    a, b = Node("pub"), Node("sub")
    a.connect(sock)
    b.connect(sock)
    N = 50000
    c = [0]
    done = threading.Event()

    def on_msg(_):
        c[0] += 1
        if c[0] >= N:
            done.set()

    b.subscribe("data", on_msg)
    b.sync()
    payload = b"\xab" * 32
    t0 = time.perf_counter()
    for _ in range(N):
        a.publish("data", payload)
    done.wait(timeout=60)
    s = time.perf_counter() - t0
    print(f"  {'pub/sub throughput':<22} {N} msgs, {s:.3f}s => {N / s / 1e3:8.1f} "
          f"k msgs/s  ({N * 32 / s / 1e6:.1f} MB/s payload)")
    a.close()
    b.close()


def bench_kv(sock):
    w, r = Node("w"), Node("r")
    w.connect(sock)
    r.connect(sock)
    M = 3000
    keys = [f"k{i}" for i in range(M)]
    for i, k in enumerate(keys):
        w.set(k, _U32.pack(i))
    w.sync()

    cold, warm = [], []
    for k in keys:  # fresh key -> round-trips
        t0 = time.perf_counter()
        r.get(k)
        cold.append(time.perf_counter() - t0)
    for _ in range(M):  # same key -> cache
        t0 = time.perf_counter()
        r.get(keys[0])
        warm.append(time.perf_counter() - t0)
    report_lat("kv get cold (RTT)", cold)
    report_lat("kv get warm (cache)", warm)

    t0 = time.perf_counter()
    for i, k in enumerate(keys):
        w.set(k, _U32.pack(i + 1))
    w.sync()
    s = time.perf_counter() - t0
    print(f"  {'kv set throughput':<22} {M} sets, {s:.3f}s => {M / s / 1e3:8.1f} k sets/s")
    w.close()
    r.close()


def bench_frames(sock, W, H):
    topic = "bench.frame"
    try:
        os.unlink("/dev/shm/broker_" + topic)
    except OSError:
        pass
    sz = W * H
    prod, cons = Node("fp"), Node("fc")
    prod.connect(sock)
    cons.connect(sock)
    prod.create_frame_ring(topic, sz, 4)
    cons.attach_frame_ring(topic)

    c = [0]
    cons.subscribe_frame(topic, lambda m, v: c.__setitem__(0, c[0] + 1))
    cons.sync()

    N = max(300, min(3000, 500_000_000 // sz))
    data = bytes(sz)
    published = 0
    t0 = time.perf_counter()
    for i in range(N):
        if prod.publish_frame(topic, i, W, H, W, data):
            published += 1
    deadline = time.perf_counter() + 20
    while c[0] + cons.frame_drops() < published and time.perf_counter() < deadline:
        time.sleep(0.0005)
    s = time.perf_counter() - t0
    print(f"  {W:4}x{H:<4} ({sz // 1024:4} KB)  {published / s / 1e3:6.1f} k frames/s  "
          f"{published * sz / s / 1e9:6.2f} GB/s  ({published} sent)")
    prod.close()
    cons.close()


def main():
    if not os.path.exists(DAEMON):
        sys.exit("missing bin/broker_daemon — run `make daemon`")
    sock = os.path.join(tempfile.gettempdir(), f"broker_bench_{os.getpid()}.sock")
    proc = subprocess.Popen([DAEMON, sock], stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)
    try:
        for _ in range(100):
            if os.path.exists(sock):
                break
            time.sleep(0.02)

        print("== broker layer (Python client over unix socket) ==")
        bench_pubsub_latency(sock)
        bench_pubsub_throughput(sock)
        bench_kv(sock)
        if HAVE_RING:
            print("\n== frames (pixels via shm, FrameHandle via broker) ==")
            bench_frames(sock, 640, 480)
            bench_frames(sock, 1920, 1080)
        else:
            print("\n(frames skipped — run `make libshm_ring`)")
    finally:
        proc.terminate()
        proc.wait()
        try:
            os.unlink(sock)
        except OSError:
            pass


if __name__ == "__main__":
    main()
