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

            # retained publish is replayed to a subscriber that joins after it;
            # a non-retained one is not.
            a.publish("status", b"ready", retain=True)
            a.publish("event", b"tick")  # not retained
            a.sync()
            with Node("late") as late:
                late.connect(sock)
                seen = {}
                late.subscribe("status", lambda p: seen.__setitem__("status", p))
                late.subscribe("event", lambda p: seen.__setitem__("event", p))
                deadline = time.time() + 2
                while "status" not in seen and time.time() < deadline:
                    time.sleep(0.01)
                late.sync()
                time.sleep(0.05)  # let any (wrongly) replayed event arrive
                assert seen.get("status") == b"ready", seen
                assert "event" not in seen, seen

            # info(): daemon status snapshot
            status = a.info()
            assert "clients:" in status, status
            assert "kv_keys:" in status, status

        print("python client self-check OK")
    finally:
        proc.terminate()
        proc.wait()
        try:
            os.unlink(sock)
        except OSError:
            pass


def _wait_socket(path, tries=100):
    for _ in range(tries):
        if os.path.exists(path):
            return
        time.sleep(0.02)


def test_patterns():
    """Wildcard pattern subscriptions: '+' one level, '*' the rest."""
    sock = os.path.join(tempfile.gettempdir(), f"broker_pypat_{os.getpid()}.sock")
    proc = subprocess.Popen([DAEMON, sock], stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)
    _wait_socket(sock)
    try:
        with Node("pub") as pub, Node("sub") as sub:
            pub.connect(sock)
            sub.connect(sock)
            star_hits, plus_hits = [], []
            sub.subscribe_pattern("cam/*", lambda t, p: star_hits.append(t))
            sub.subscribe_pattern("cam/+", lambda t, p: plus_hits.append(t))
            sub.sync()

            for topic in ("cam/front", "cam/front/left", "lidar/top"):
                pub.publish(topic, b"x")
            deadline = time.time() + 2
            while len(star_hits) < 2 and time.time() < deadline:
                time.sleep(0.01)
            time.sleep(0.05)

            assert sorted(star_hits) == ["cam/front", "cam/front/left"], star_hits
            assert plus_hits == ["cam/front"], plus_hits  # single level only
        print("python pattern self-check OK")
    finally:
        proc.terminate()
        proc.wait()
        try:
            os.unlink(sock)
        except OSError:
            pass


def test_reconnect():
    """Node survives a daemon restart: reconnects to the same address and
    resubscribes, and delivery resumes."""
    sock = os.path.join(tempfile.gettempdir(), f"broker_pyrc_{os.getpid()}.sock")
    proc = subprocess.Popen([DAEMON, sock], stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)
    _wait_socket(sock)
    try:
        sub, pub = Node("sub"), Node("pub")
        sub.connect(sock)
        pub.connect(sock)
        got = [0]
        sub.subscribe("t", lambda p: got.__setitem__(0, got[0] + 1))
        sub.sync()

        pub.publish("t", b"x")
        deadline = time.time() + 2
        while got[0] == 0 and time.time() < deadline:
            time.sleep(0.01)
        assert got[0] == 1, got  # baseline works

        # kill the daemon; nodes detect the drop and start reconnecting
        proc.terminate()
        proc.wait()
        deadline = time.time() + 2
        while (sub.connected() or pub.connected()) and time.time() < deadline:
            time.sleep(0.01)
        assert not sub.connected()  # observed the disconnect

        # bring the daemon back on the same address
        proc = subprocess.Popen([DAEMON, sock], stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL)
        _wait_socket(sock)

        before = got[0]
        deadline = time.time() + 6
        while got[0] == before and time.time() < deadline:
            pub.publish("t", b"y")  # dropped until pub reconnects, then routed
            time.sleep(0.02)
        assert got[0] > before, "no delivery after reconnect"

        sub.close()
        pub.close()
        print("python reconnect self-check OK")
    finally:
        proc.terminate()
        proc.wait()
        try:
            os.unlink(sock)
        except OSError:
            pass


if __name__ == "__main__":
    main()
    test_patterns()
    test_reconnect()
