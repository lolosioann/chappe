#!/usr/bin/env python3
"""Self-check for the Python client. Needs the daemon binary:
    make daemon && python3 python/test_broker.py
Launches a broker_daemon on a temp socket and exercises pub/sub + get/set.
"""
import os
import struct
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from broker import Node

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DAEMON = os.path.join(ROOT, "bin", "broker_daemon")


def main():
    if not os.path.exists(DAEMON):
        sys.exit(f"missing {DAEMON} — run `make daemon` first")

    sock = os.path.join(tempfile.gettempdir(), f"broker_pytest_{os.getpid()}.sock")
    proc = subprocess.Popen([DAEMON, sock], stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)
    try:
        for _ in range(100):
            if os.path.exists(sock):
                break
            time.sleep(0.02)

        with Node("a") as a, Node("b") as b:
            a.connect(sock)
            b.connect(sock)

            # pub/sub: b receives a's publishes
            got = []
            b.subscribe("t", lambda p: got.append(struct.unpack("=i", p)[0]))
            b.sync()
            for i in (5, 7):
                a.publish("t", struct.pack("=i", i))
            deadline = time.time() + 2
            while len(got) < 2 and time.time() < deadline:
                time.sleep(0.01)
            assert got == [5, 7], got

            # get/set: cold read, warm read, miss
            a.set("k", b"hello")
            a.sync()
            assert b.get("k") == b"hello"
            assert b.get("k") == b"hello"       # warm — from cache
            assert b.get("missing") is None

            # a later set is pushed into b's cache
            a.set("k", b"world")
            deadline = time.time() + 2
            while b.get("k") != b"world" and time.time() < deadline:
                time.sleep(0.01)
            assert b.get("k") == b"world"

        print("python client self-check OK")
    finally:
        proc.terminate()
        proc.wait()
        try:
            os.unlink(sock)
        except OSError:
            pass


if __name__ == "__main__":
    main()
