#!/usr/bin/env python3
"""Same-methodology shootout: drive our broker, Redis, and Mosquitto (MQTT)
through ONE Python timing harness so the numbers are comparable. Each broker is
launched locally (unix socket / loopback TCP), in-memory, no persistence.

Needs: bin/chappe_daemon, redis-server, mosquitto on PATH, and a venv with
`redis` and `paho-mqtt<2`. Run with that venv's python:
    make daemon
    <venv>/bin/python python/bench_compare.py
"""
import os
import queue
import socket
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from chappe import Node

import redis
import paho.mqtt.client as mqtt

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DAEMON = os.path.join(ROOT, "bin", "chappe_daemon")
TMP = tempfile.gettempdir()
PID = os.getpid()


# ---- node adapters: uniform publish/subscribe/sync/kv over each client -------

class OursNode:
    def __init__(self, name, sock):
        self.n = Node(name)
        self.n.connect(sock)

    def publish(self, t, p): self.n.publish(t, p)
    def subscribe(self, t, h): self.n.subscribe(t, h)
    def sync(self): self.n.sync()
    def kvset(self, k, v): self.n.set(k, v)
    def kvget(self, k): return self.n.get(k)
    def close(self): self.n.close()


class RedisNode:
    def __init__(self, name, sock):
        self.r = redis.Redis(unix_socket_path=sock)
        self.ps = self.r.pubsub()
        self._handlers = {}
        self._thread = None

    def publish(self, t, p): self.r.publish(t, p)

    def subscribe(self, t, h):
        self._handlers[t] = h
        self.ps.subscribe(t)

    def sync(self):
        if self._handlers and self._thread is None:
            self._thread = threading.Thread(target=self._loop, daemon=True)
            self._thread.start()
            time.sleep(0.05)  # let SUBSCRIBE settle
        self.r.ping()

    def _loop(self):
        try:
            for msg in self.ps.listen():
                if msg.get("type") == "message":
                    h = self._handlers.get(msg["channel"].decode())
                    if h:
                        h(msg["data"])
        except Exception:
            pass  # connection closed on shutdown

    def kvset(self, k, v): self.r.set(k, v)
    def kvget(self, k): return self.r.get(k)

    def close(self):
        try:
            self.ps.close()
        except Exception:
            pass
        try:
            self.r.close()
        except Exception:
            pass


class MqttNode:
    def __init__(self, name, host, port):
        try:
            self.c = mqtt.Client(client_id=name)              # paho 1.x
        except (TypeError, ValueError):
            self.c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id=name)
        self._handlers = {}
        self.c.on_message = lambda cl, ud, m: self._dispatch(m)
        self.c.connect(host, port, keepalive=30)
        self.c.loop_start()

    def _dispatch(self, m):
        h = self._handlers.get(m.topic)
        if h:
            h(m.payload)

    def publish(self, t, p): self.c.publish(t, p, qos=0)

    def subscribe(self, t, h):
        self._handlers[t] = h
        self.c.subscribe(t, qos=0)

    def sync(self): time.sleep(0.1)  # let SUBSCRIBE register at the broker
    def close(self):
        self.c.loop_stop()
        self.c.disconnect()


# ---- benchmarks (generic over a node factory) --------------------------------

def pubsub_rtt(make_node, N=3000, warm=300):
    a, b = make_node("rtt-a"), make_node("rtt-b")
    q = queue.SimpleQueue()
    a.subscribe("pong", lambda p: q.put(1))
    b.subscribe("ping", lambda p: b.publish("pong", p))
    a.sync()
    b.sync()
    lat = []
    for i in range(N + warm):
        t0 = time.perf_counter()
        a.publish("ping", b"x")
        q.get()
        dt = time.perf_counter() - t0
        if i >= warm:
            lat.append(dt)
    a.close()
    b.close()
    s = sorted(lat)
    return {"p50_us": s[len(s) // 2] * 1e6, "mean_us": sum(s) / len(s) * 1e6}


def pubsub_throughput(make_node, N=50000):
    a, b = make_node("tp-a"), make_node("tp-b")
    c = [0]
    done = threading.Event()

    def h(_):
        c[0] += 1
        if c[0] >= N:
            done.set()

    b.subscribe("data", h)
    b.sync()
    a.sync()
    payload = b"\xab" * 32
    t0 = time.perf_counter()
    for _ in range(N):
        a.publish("data", payload)
    done.wait(timeout=30)
    elapsed = time.perf_counter() - t0
    a.close()
    b.close()
    return {"k_msg_s": c[0] / elapsed / 1e3, "recv": c[0], "sent": N}


def kv_get_rtt(make_node, M=3000):
    w, r = make_node("kv-w"), make_node("kv-r")
    keys = [f"k{i}" for i in range(M)]
    for k in keys:
        w.kvset(k, b"vvvv")
    w.sync()
    lat = []
    for k in keys:
        t0 = time.perf_counter()
        r.kvget(k)
        lat.append(time.perf_counter() - t0)
    w.close()
    r.close()
    s = sorted(lat)
    return {"p50_us": s[len(s) // 2] * 1e6, "mean_us": sum(s) / len(s) * 1e6}


def kv_set_tput(make_node, M=3000):
    w = make_node("kv-sw")
    keys = [f"s{i}" for i in range(M)]
    t0 = time.perf_counter()
    for k in keys:
        w.kvset(k, b"vvvv")
    w.sync()
    elapsed = time.perf_counter() - t0
    w.close()
    return {"k_set_s": M / elapsed / 1e3}


def run_suite(make_node, kv):
    res = {"pubsub_rtt": pubsub_rtt(make_node),
           "pubsub_tput": pubsub_throughput(make_node)}
    if kv:
        res["kv_get_rtt"] = kv_get_rtt(make_node)
        res["kv_set_tput"] = kv_set_tput(make_node)
    return res


# ---- broker launchers --------------------------------------------------------

def wait_socket_file(path, tries=100):
    for _ in range(tries):
        if os.path.exists(path):
            return True
        time.sleep(0.05)
    return False


def wait_tcp(host, port, tries=100):
    for _ in range(tries):
        try:
            with socket.create_connection((host, port), timeout=0.2):
                return True
        except OSError:
            time.sleep(0.05)
    return False


def launch_ours():
    sock = os.path.join(TMP, f"cmp_ours_{PID}.sock")
    if not os.path.exists(DAEMON):
        raise RuntimeError("missing bin/chappe_daemon — run `make daemon`")
    p = subprocess.Popen([DAEMON, sock], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if not wait_socket_file(sock):
        raise RuntimeError("ours daemon did not start")
    return (lambda name: OursNode(name, sock)), True, p


def launch_redis():
    sock = os.path.join(TMP, f"cmp_redis_{PID}.sock")
    p = subprocess.Popen(["redis-server", "--unixsocket", sock, "--port", "0",
                          "--save", "", "--appendonly", "no"],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if not wait_socket_file(sock):
        raise RuntimeError("redis did not start")
    return (lambda name: RedisNode(name, sock)), True, p


def launch_mqtt():
    port = 18833
    conf = os.path.join(TMP, f"cmp_mosq_{PID}.conf")
    with open(conf, "w") as f:
        f.write(f"listener {port}\nallow_anonymous true\n")
    p = subprocess.Popen(["mosquitto", "-c", conf],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if not wait_tcp("127.0.0.1", port):
        raise RuntimeError("mosquitto did not start")
    return (lambda name: MqttNode(name, "127.0.0.1", port)), False, p


# ---- main --------------------------------------------------------------------

def main():
    brokers = [("this broker", launch_ours),
               ("Redis", launch_redis),
               ("MQTT (Mosquitto)", launch_mqtt)]
    results = {}
    for label, launch in brokers:
        proc = None
        try:
            make_node, kv, proc = launch()
            print(f"running {label} ...", flush=True)
            results[label] = run_suite(make_node, kv)
        except Exception as e:
            print(f"  {label}: skipped ({e})", flush=True)
            results[label] = None
        finally:
            if proc is not None:
                proc.terminate()
                proc.wait()

    labels = [l for l, _ in brokers]
    col = 18

    def row(name, fn):
        cells = []
        for l in labels:
            r = results.get(l)
            cells.append(fn(r) if r else "n/a")
        print(f"  {name:<26}" + "".join(f"{c:>{col}}" for c in cells))

    print("\n== same-methodology shootout (loopback, one machine) ==")
    print(f"  {'metric':<26}" + "".join(f"{l:>{col}}" for l in labels))
    row("pub/sub RTT p50 (us)", lambda r: f"{r['pubsub_rtt']['p50_us']:.1f}")
    row("pub/sub RTT mean (us)", lambda r: f"{r['pubsub_rtt']['mean_us']:.1f}")
    row("pub/sub tput (k msg/s)", lambda r: f"{r['pubsub_tput']['k_msg_s']:.1f}")
    row("kv get RTT p50 (us)", lambda r: f"{r['kv_get_rtt']['p50_us']:.1f}" if 'kv_get_rtt' in r else "n/a")
    row("kv set tput (k/s)", lambda r: f"{r['kv_set_tput']['k_set_s']:.1f}" if 'kv_set_tput' in r else "n/a")


if __name__ == "__main__":
    main()
