#!/usr/bin/env python3
"""Self-check for the Python client. Needs the daemon binary:
    make daemon && python3 python/test_chappe.py
Launches a chappe_daemon on a temp socket and exercises pub/sub + get/set.
"""
import os
import re
import struct
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import chappe
from chappe import Node

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DAEMON = os.path.join(ROOT, "bin", "chappe_daemon")


def main():
    if not os.path.exists(DAEMON):
        sys.exit(f"missing {DAEMON} — run `make daemon` first")

    sock = os.path.join(tempfile.gettempdir(), f"chappe_pytest_{os.getpid()}.sock")
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


class Daemon:
    """A chappe_daemon on its own temp socket, as a context manager. Restartable
    so a check can drop the link under its nodes."""

    def __init__(self, tag):
        self.sock = os.path.join(tempfile.gettempdir(),
                                 f"chappe_{tag}_{os.getpid()}.sock")
        self.proc = None

    def start(self):
        self.proc = subprocess.Popen([DAEMON, self.sock],
                                     stdout=subprocess.DEVNULL,
                                     stderr=subprocess.DEVNULL)
        _wait_socket(self.sock)

    def stop(self):
        if self.proc is not None:
            self.proc.terminate()
            self.proc.wait()
            self.proc = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc):
        self.stop()
        try:
            os.unlink(self.sock)
        except OSError:
            pass


def test_patterns():
    """Wildcard pattern subscriptions: '+' one level, '*' the rest."""
    sock = os.path.join(tempfile.gettempdir(), f"chappe_pypat_{os.getpid()}.sock")
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
    sock = os.path.join(tempfile.gettempdir(), f"chappe_pyrc_{os.getpid()}.sock")
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


def test_subscribe_before_connect():
    """Handlers may be registered before connect(), which flushes their
    subscriptions to the daemon."""
    with Daemon("pysbc") as d, Node("sub") as sub, Node("pub") as pub:
        got, pat_hits = [], []
        sub.subscribe("t", got.append)
        sub.subscribe_pattern("cam/+", lambda t, p: pat_hits.append(t))
        sub.connect(d.sock)
        pub.connect(d.sock)
        sub.sync()

        pub.publish("t", b"x")
        pub.publish("cam/front", b"y")
        deadline = time.time() + 2
        while (not got or not pat_hits) and time.time() < deadline:
            time.sleep(0.01)
        assert got == [b"x"], got
        assert pat_hits == ["cam/front"], pat_hits
    print("python subscribe-before-connect self-check OK")


def test_unsubscribe():
    """unsubscribe()/unsubscribe_pattern() stop delivery daemon-side, and a
    reconnect must not resurrect what was dropped."""
    with Daemon("pyunsub") as d, Node("sub") as sub, Node("pub") as pub:
        sub.connect(d.sock)
        pub.connect(d.sock)
        got, pat_hits, kept = [], [], []
        sub.subscribe("t", got.append)
        sub.subscribe_pattern("cam/+", lambda t, p: pat_hits.append(t))
        sub.subscribe("keep", kept.append)  # control: never unsubscribed
        sub.sync()

        pub.publish("t", b"x")
        pub.publish("cam/front", b"y")
        deadline = time.time() + 2
        while (not got or not pat_hits) and time.time() < deadline:
            time.sleep(0.01)
        assert got == [b"x"], got
        assert pat_hits == ["cam/front"], pat_hits

        sub.unsubscribe("t")
        sub.unsubscribe_pattern("cam/+")
        sub.sync()
        # the daemon, not just the local handler table, must have forgotten them
        status = sub.info()
        assert "topics: 1 (1 subscriptions)" in status, status
        assert "patterns: 0" in status, status

        pub.publish("t", b"x2")
        pub.publish("cam/front", b"y2")
        pub.sync()
        time.sleep(0.05)  # let a (wrongly) routed publish arrive
        assert got == [b"x"], got
        assert pat_hits == ["cam/front"], pat_hits

        # restart the daemon so both nodes reconnect and _resubscribe() runs
        d.stop()
        deadline = time.time() + 2
        while (sub.connected() or pub.connected()) and time.time() < deadline:
            time.sleep(0.01)
        assert not sub.connected()
        d.start()

        deadline = time.time() + 6
        while not kept and time.time() < deadline:
            pub.publish("keep", b"z")  # dropped until pub reconnects
            time.sleep(0.02)
        assert kept, "no delivery after reconnect"

        status = sub.info()
        assert "topics: 1 (1 subscriptions)" in status, status
        assert "patterns: 0" in status, status
    print("python unsubscribe self-check OK")


def test_kv_delete():
    """delete() erases the key: watchers drop it without a round-trip, and a
    cold reader sees it gone."""
    with Daemon("pykvdel") as d, Node("a") as a, Node("b") as b:
        a.connect(d.sock)
        b.connect(d.sock)
        a.set("k", b"v")
        a.sync()
        assert b.get("k") == b"v"  # cold read; b now watches k

        a.delete("k")
        deadline = time.time() + 2
        while b.get("k") is not None and time.time() < deadline:
            time.sleep(0.01)
        assert b.get("k") is None  # warm read, served from the pushed deletion

        with Node("cold") as cold:
            cold.connect(d.sock)
            assert cold.get("k") is None  # round-trips: really gone from the store
    print("python kv delete self-check OK")


def test_kv_ttl():
    """set(ttl_ms=...) makes the daemon expire the key and push the deletion, so
    a watcher's warm read flips to None with no round-trip of its own."""
    with Daemon("pyttl") as d, Node("a") as a, Node("b") as b:
        a.connect(d.sock)
        b.connect(d.sock)
        a.set("k", b"v", ttl_ms=500)
        a.sync()
        assert b.get("k") == b"v"  # cold read, inside the ttl: b now watches k

        deadline = time.time() + 2
        while b.get("k") is not None and time.time() < deadline:
            time.sleep(0.01)
        assert b.get("k") is None
    print("python kv ttl self-check OK")


def test_kv_incr():
    """incr counts an absent key from 0 and stores the "=q" bytes a C++ node
    reads with get<int64_t>(). A key holding anything else is a type error, not
    something to coerce."""
    with Daemon("pyincr") as d, Node("a") as a:
        a.connect(d.sock)
        assert a.incr("c") == 1
        assert a.incr("c", 5) == 6
        assert a.get("c") == struct.pack("=q", 6)

        a.set("s", b"abc")
        a.sync()
        assert a.incr("s") is None
        assert a.get("s") == b"abc"  # unchanged
    print("python kv incr self-check OK")


def test_kv_setnx():
    """setnx hands the key to exactly one node, and its ttl releases it again if
    the holder never does. An empty value is a real value: winning with one must
    not read as losing."""
    with Daemon("pysetnx") as d, Node("a") as a, Node("b") as b:
        a.connect(d.sock)
        b.connect(d.sock)
        assert a.setnx("lock", b"a") is True
        assert b.setnx("lock", b"b") is False
        assert b.get("lock") == b"a"

        assert a.setnx("held", b"", ttl_ms=500) is True  # zero value bytes
        assert b.setnx("held", b"b") is False
        deadline = time.time() + 2
        while not b.setnx("held", b"b") and time.time() < deadline:
            time.sleep(0.01)
        assert b.get("held") == b"b"  # the ttl let the next node in
    print("python kv setnx self-check OK")


def test_retained_clear():
    """clear_retained() drops the topic's last value, so later subscribers are
    replayed nothing."""
    with Daemon("pyretain") as d, Node("pub") as pub:
        pub.connect(d.sock)
        pub.publish("status", b"ready", retain=True)
        pub.sync()

        with Node("late") as late:
            late.connect(d.sock)
            seen = []
            late.subscribe("status", seen.append)
            deadline = time.time() + 2
            while not seen and time.time() < deadline:
                time.sleep(0.01)
            assert seen == [b"ready"], seen

        pub.clear_retained("status")
        pub.sync()
        assert "retained: 0" in pub.info(), pub.info()

        with Node("later") as later:
            later.connect(d.sock)
            seen = []
            later.subscribe("status", seen.append)
            later.sync()
            time.sleep(0.05)  # let a (wrongly) replayed value arrive
            assert seen == [], seen
    print("python retained clear self-check OK")


def test_handler_pool():
    """threads>0 runs handlers on a pool. The barrier only clears if two
    invocations genuinely overlap — inline dispatch deadlocks it into a
    timeout."""
    with Daemon("pypool") as d, Node("sub", threads=2) as sub, Node("pub") as pub:
        sub.connect(d.sock)
        pub.connect(d.sock)
        barrier = threading.Barrier(2)
        got, overlapped = [], []

        def handler(payload):
            got.append(payload)
            try:
                barrier.wait(timeout=2)
                overlapped.append(payload)
            except threading.BrokenBarrierError:
                pass

        sub.subscribe("t", handler)
        sub.sync()
        pub.publish("t", b"1")
        pub.publish("t", b"2")
        deadline = time.time() + 3
        while len(overlapped) < 2 and time.time() < deadline:
            time.sleep(0.01)
        assert sorted(got) == [b"1", b"2"], got
        assert sorted(overlapped) == [b"1", b"2"], "handlers ran serially"
    print("python handler pool self-check OK")


def test_protocol_parity():
    """Cross-language parity: the Python frame kinds must be the same numbers as
    the C++ enum they mirror. Every behavioural check above already runs the
    Python wire encoding through the C++ daemon, so the one failure mode left is
    the numbering drifting apart — and this catches it without a build step."""
    header = os.path.join(ROOT, "include", "ipc", "transport.hpp")
    with open(header) as f:
        cpp = {m.group(1): int(m.group(2))
               for m in re.finditer(r"^\s*MSG_(\w+) = (\d+),", f.read(), re.M)}
    assert "KV_DEL" in cpp, cpp  # guards against the regex matching nothing
    for name, value in sorted(cpp.items()):
        assert getattr(chappe, "_" + name, None) == value, \
            f"MSG_{name}: C++ {value}, Python {getattr(chappe, '_' + name, None)}"

    # The magic in front of a serialized value is a contract between the two
    # clients: C++ strips it with json_payload(). Drift and a C++ node quietly
    # stops recognising Python values.
    with open(header) as f:
        m = re.search(r"JSON_MAGIC\[\] = \{([^}]*)\}", f.read())
    assert m, "JSON_MAGIC moved; update this check"
    cpp_magic = bytes(int(c.strip().strip("'").replace("\\x", ""), 16)
                      if "\\x" in c else ord(c.strip().strip("'"))
                      for c in m.group(1).split(","))
    assert cpp_magic == chappe._MAGIC, f"C++ {cpp_magic!r}, Python {chappe._MAGIC!r}"
    print("python protocol parity self-check OK")


def test_serialization():
    """publish/set take Python values, not just bytes — and bytes still go out
    byte for byte, which is what keeps C++ topics and frames working."""
    with Daemon("ser") as d:
        sock = d.sock
        with chappe.Node("pub") as pub, chappe.Node("sub") as sub:
            pub.connect(sock)
            sub.connect(sock)
            got = []
            sub.subscribe("v", got.append)
            sub.sync()

            values = [17, -3, 3.25, "hello", True, False, None,
                      [1, 2, "three"], {"mode": "race", "gear": 3}, {}, []]
            for v in values:
                pub.publish("v", v)
            pub.sync()
            sub.sync()
            assert got == values, f"{got} != {values}"

            # bytes are untouched: same object value, no magic, no JSON. This is
            # the C++ interop path — struct.pack must survive exactly.
            got.clear()
            raw = struct.pack("=i", 42)
            pub.publish("v", raw)
            pub.sync()
            sub.sync()
            assert got == [raw], got
            assert struct.unpack("=i", got[0])[0] == 42

            # A raw payload that happens to open with the magic must NOT be
            # mistaken for ours — the JSON behind it doesn't parse, so it comes
            # back as the bytes that were sent.
            got.clear()
            impostor = chappe._MAGIC + b"\xff\xfe not json"
            pub.publish("v", impostor)
            pub.sync()
            sub.sync()
            assert got == [impostor], got

            # A hostile payload must not kill the reader thread. This one wears
            # the magic and is nested deep enough that json.loads raises
            # RecursionError, which is not a ValueError — decoding it has to
            # fail soft, and the node has to keep working afterwards.
            got.clear()
            bomb = chappe._MAGIC + b"[" * 200_000 + b"]" * 200_000
            pub.publish("v", bomb)
            pub.sync()
            sub.sync()
            assert got == [bomb], "decode bomb was not passed through as bytes"
            got.clear()
            pub.publish("v", "still alive")
            pub.sync()
            sub.sync()
            assert got == ["still alive"], f"reader died on a bad payload: {got}"

            # kv takes the same values and gives them back as the same types.
            for i, v in enumerate(values):
                pub.set(f"k{i}", v)
            pub.sync()
            for i, v in enumerate(values):
                assert pub.get(f"k{i}") == v, f"k{i}: {pub.get(f'k{i}')} != {v}"
            pub.set("kraw", raw)
            pub.sync()
            assert pub.get("kraw") == raw

            # Unsupported values fail loudly rather than being mangled.
            for bad in ({1, 2}, {"blob": b"bytes don't nest"}, object()):
                try:
                    pub.publish("v", bad)
                    assert False, f"expected TypeError for {bad!r}"
                except TypeError as e:
                    assert "chappe cannot serialize" in str(e), e

            # incr's representation is fixed, so a serialized int is not a
            # counter — the documented sharp edge, asserted so it stays true.
            pub.set("count", struct.pack("=q", 5))
            pub.sync()
            assert pub.incr("count") == 6
            pub.set("notcount", 5)
            pub.sync()
            assert pub.incr("notcount") is None, "serialized int must not incr"
    print("python serialization self-check OK")


def test_version_parity():
    """The version is written out three times — chappe::VERSION, the Makefile's
    VERSION (which stamps chappe.pc and the CMake config) and __version__. That
    is fine as long as bumping one without the others can't go unnoticed."""
    with open(os.path.join(ROOT, "include", "chappe.hpp")) as f:
        cpp = re.search(r'VERSION = "([^"]+)"', f.read())
    with open(os.path.join(ROOT, "Makefile")) as f:
        mk = re.search(r"^VERSION := (\S+)", f.read(), re.M)
    assert cpp and mk, "version literal moved; update this check"
    assert cpp.group(1) == chappe.__version__ == mk.group(1), \
        f"C++ {cpp.group(1)}, python {chappe.__version__}, make {mk.group(1)}"
    print(f"python version parity self-check OK ({chappe.__version__})")


if __name__ == "__main__":
    main()
    test_patterns()
    test_reconnect()
    test_subscribe_before_connect()
    test_unsubscribe()
    test_kv_delete()
    test_kv_ttl()
    test_kv_incr()
    test_kv_setnx()
    test_retained_clear()
    test_handler_pool()
    test_protocol_parity()
    test_serialization()
    test_version_parity()
