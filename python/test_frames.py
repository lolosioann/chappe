#!/usr/bin/env python3
"""Frame-transport self-check. Needs both binaries:
    make daemon libshm_ring && python3 python/test_frames.py
"""
import os
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from chappe import Node

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DAEMON = os.path.join(ROOT, "bin", "chappe_daemon")


def main():
    if not os.path.exists(DAEMON):
        sys.exit("missing bin/chappe_daemon — run `make daemon`")
    if not os.path.exists(os.path.join(ROOT, "bin", "libshm_ring.so")):
        sys.exit("missing bin/libshm_ring.so — run `make libshm_ring`")

    topic = "frametest.topic"
    try:
        os.unlink("/dev/shm/chappe_" + topic)  # clear a stale segment
    except OSError:
        pass

    sock = os.path.join(tempfile.gettempdir(), f"chappe_frametest_{os.getpid()}.sock")
    proc = subprocess.Popen([DAEMON, sock], stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)
    try:
        for _ in range(100):
            if os.path.exists(sock):
                break
            time.sleep(0.02)

        W = H = 4
        with Node("prod") as prod, Node("cons") as cons:
            prod.connect(sock)
            cons.connect(sock)
            prod.create_frame_ring(topic, W * H, 4)
            cons.attach_frame_ring(topic)

            got = []
            cons.subscribe_frame(topic, lambda m, v: got.append((m.timestamp_ns, m.width, v.cast("B")[0])))
            cons.sync()

            for f in range(3):
                assert prod.publish_frame(topic, 100 + f, W, H, W, bytes([10 + f]) * (W * H))
                time.sleep(0.02)  # let the consumer read each before the next

            deadline = time.time() + 2
            while len(got) < 3 and time.time() < deadline:
                time.sleep(0.01)
            assert len(got) == 3, got
            assert [g[0] for g in got] == [100, 101, 102], got  # ts, per-message
            assert got[0][1] == W
            assert got[-1][2] == 12, got  # last frame's first pixel

        # A consumer that starts before any producer must still work: the ring
        # only exists once a producer creates it, so attaching up front cannot
        # succeed. Nothing else on this bus cares about startup order.
        late = "frametest.late"
        try:
            os.unlink("/dev/shm/chappe_" + late)
        except OSError:
            pass
        with Node("late_cons") as cons, Node("late_prod") as prod:
            cons.connect(sock)
            seen = []
            cons.subscribe_frame(late, lambda meta, view: seen.append(bytes(view[:4])))
            cons.sync()

            prod.connect(sock)                      # producer arrives second
            prod.create_frame_ring(late, 16, 4)
            prod.publish_frame(late, 7, 4, 1, 4, b"\xa5" * 16)
            deadline = time.time() + 3
            while not seen and time.time() < deadline:
                time.sleep(0.01)
            assert seen and seen[0] == b"\xa5" * 4, (seen, cons.frame_drops())
        try:
            os.unlink("/dev/shm/chappe_" + late)
        except OSError:
            pass

        print("python frame self-check OK")
    finally:
        proc.terminate()
        proc.wait()
        try:
            os.unlink(sock)
        except OSError:
            pass


if __name__ == "__main__":
    main()
