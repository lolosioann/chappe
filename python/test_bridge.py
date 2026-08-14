#!/usr/bin/env python3
"""Self-check for the Redis bridge. Needs the daemon binary, redis-py and a
redis-server on PATH; skips (exit 0) if any is missing, so CI stays green on a
box without Redis:

    make daemon && python3 python/test_bridge.py
"""
import os
import shutil
import struct
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from chappe import Node

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DAEMON = os.path.join(ROOT, "bin", "chappe_daemon")


def skip(why):
    print(f"python redis bridge self-check SKIPPED ({why})")
    sys.exit(0)


def wait_for(pred, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        v = pred()
        if v:
            return v
        time.sleep(0.02)
    return pred()


def main():
    if not os.path.exists(DAEMON):
        sys.exit(f"missing {DAEMON} — run `make daemon` first")
    if shutil.which("redis-server") is None:
        skip("no redis-server on PATH")
    try:
        import redis
    except ImportError:
        skip("redis-py not installed")
    from chappe import redis_bridge

    tmp = tempfile.gettempdir()
    pid = os.getpid()
    sock = os.path.join(tmp, f"chappe_bridge_{pid}.sock")
    rsock = os.path.join(tmp, f"redis_bridge_{pid}.sock")
    port = 0

    # Redis on a unix socket, in-memory, so the check can't collide with a real
    # server on 6379 or leave a dump behind.
    rproc = subprocess.Popen(
        ["redis-server", "--port", str(port), "--unixsocket", rsock,
         "--save", "", "--appendonly", "no"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    dproc = subprocess.Popen([DAEMON, sock], stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL)
    bridge = None
    try:
        if not wait_for(lambda: os.path.exists(sock)):
            sys.exit("daemon socket never appeared")
        if not wait_for(lambda: os.path.exists(rsock)):
            skip("redis-server did not start")

        r = redis.Redis(unix_socket_path=rsock)
        wait_for(lambda: r.ping())

        with Node("app") as app:
            app.connect(sock)
            app.set("state/mode", b"idle")
            app.sync()

            bnode = Node("redis-bridge")
            bnode.connect(sock)
            bridge = redis_bridge.Bridge(bnode, r, "chappe:", 0.02,
                                         ["chappe:cmd/*"])
            threading.Thread(
                target=bridge.run,
                args=(["telemetry/*", "cmd/*"], ["state/mode", "state/gear"]),
                daemon=True).start()

            # kv ours -> Redis, including a value written after the bridge is up
            assert wait_for(lambda: r.get("chappe:state/mode") == b"idle")
            app.set("state/gear", struct.pack("=i", 3))
            assert wait_for(
                lambda: r.get("chappe:state/gear") == struct.pack("=i", 3))

            # a counter crosses as raw bytes, still readable as an int64
            app.set("state/mode", b"run")
            assert wait_for(lambda: r.get("chappe:state/mode") == b"run")

            # kv delete propagates as a Redis delete
            app.delete("state/gear")
            assert wait_for(lambda: r.get("chappe:state/gear") is None)

            # topics ours -> Redis
            ps = r.pubsub(ignore_subscribe_messages=True)
            ps.psubscribe("chappe:telemetry/*")
            time.sleep(0.2)  # let the subscription land before publishing
            app.publish("telemetry/temp", b"41")
            got = wait_for(lambda: ps.get_message(timeout=0.1))
            assert got and got["data"] == b"41", got
            ps.close()

            # topics Redis -> ours: the monitor's publish reaches a bus node
            with Node("mon") as mon:
                mon.connect(sock)
                seen = []
                mon.subscribe("cmd/stop", lambda p: seen.append(p))
                mon.sync()
                r.publish("chappe:cmd/stop", b"now")
                assert wait_for(lambda: len(seen) >= 1), "monitor publish lost"
                assert seen == [b"now"], seen

                # ...and a bus publish on a topic that is BOTH mirrored out and
                # subscribed back arrives exactly once. Redis echoes our own
                # publish to us; if the bridge failed to suppress it, it would
                # re-publish it onto the bus and this would land twice.
                twice = []
                mon.subscribe("cmd/go", lambda p: twice.append(p))
                mon.sync()
                app.publish("cmd/go", b"halt")
                assert wait_for(lambda: len(twice) >= 1), "bus publish lost"
                time.sleep(0.5)  # long enough for a duplicate to show up
                assert twice == [b"halt"], f"echo loop: {twice}"

            bridge._running = False
            bnode.close()
    finally:
        if bridge is not None:
            bridge._running = False
        dproc.terminate()
        dproc.wait()
        rproc.terminate()
        rproc.wait()
        for p in (sock, rsock):
            if os.path.exists(p):
                os.unlink(p)
    print("python redis bridge self-check OK")


if __name__ == "__main__":
    main()
