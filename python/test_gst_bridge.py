#!/usr/bin/env python3
"""Self-check for the GStreamer frame bridge: a frame published on one topic has
to come out of a second topic, having actually been through H.264 and a UDP
socket. Skips cleanly (exit 0) when GStreamer or the codec plugins are missing —
they are system packages, not pip installs.

    python3 python/test_gst_bridge.py
"""
import os
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from chappe import Node

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
W, H, PORT, VALUE = 320, 240, 5623, 200

try:
    import gi

    gi.require_version("Gst", "1.0")
    gi.require_version("GstVideo", "1.0")
    from gi.repository import Gst

    Gst.init(None)
except (ImportError, ValueError) as e:
    print(f"python gst bridge self-check SKIPPED ({e})")
    sys.exit(0)

# Encode, decode, payload and transport each live in a different plugin package.
missing = [n for n in ("appsrc", "appsink", "x264enc", "avdec_h264",
                       "rtph264pay", "rtph264depay", "udpsrc", "udpsink",
                       "rtpjitterbuffer", "videoconvert")
           if Gst.ElementFactory.find(n) is None]
if missing:
    print(f"python gst bridge self-check SKIPPED (no {', '.join(missing)})")
    sys.exit(0)

daemon_bin = os.path.join(ROOT, "bin", "chappe_daemon")
if not os.path.exists(daemon_bin):
    print("python gst bridge self-check SKIPPED (run `make daemon` first)")
    sys.exit(0)


def wait_for(pred, timeout=20):
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        time.sleep(0.1)
    return False


def main():
    sock = os.path.join(tempfile.gettempdir(), f"chappe_gst_{os.getpid()}.sock")
    for ring in ("/dev/shm/chappe_gsttx", "/dev/shm/chappe_gstrx"):
        if os.path.exists(ring):
            os.unlink(ring)

    daemon = subprocess.Popen([daemon_bin, sock],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    procs, cam, sink = [], None, None
    try:
        assert wait_for(lambda: os.path.exists(sock), 5), "daemon never came up"

        # A synthetic camera. Every row is the same left-to-right ramp: a flat
        # grey would survive shearing or a wrong stride unchanged, where a ramp
        # shifts rows against each other and gives the check something to bite.
        cam = Node("cam")
        cam.connect(sock)
        cam.create_frame_ring("gsttx", W * H, 4)
        stop = threading.Event()
        row = bytes(x * 255 // W for x in range(W))

        def produce():
            frame = row * H
            t = 0
            while not stop.is_set():
                cam.publish_frame("gsttx", t, W, H, W, frame)
                t += 33_000_000
                time.sleep(1 / 30)

        threading.Thread(target=produce, daemon=True).start()

        env = dict(os.environ, PYTHONPATH=os.path.join(ROOT, "python"))

        def bridge(*extra):
            p = subprocess.Popen(
                [sys.executable, "-m", "chappe.gst_bridge", "--socket", sock,
                 "--width", str(W), "--height", str(H), "--port", str(PORT), *extra],
                cwd=ROOT, env=env,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            procs.append(p)
            return p

        # Receiver first: it owns the UDP port and creates the far-side ring.
        bridge("--recv", "gstrx")
        assert wait_for(lambda: os.path.exists("/dev/shm/chappe_gstrx"), 15), \
            "recv bridge never created its ring"
        bridge("--send", "gsttx", "--host", "127.0.0.1")

        got = []
        sink = Node("sink")
        sink.connect(sock)
        sink.attach_frame_ring("gstrx")
        sink.subscribe_frame(
            "gstrx", lambda meta, view: got.append((meta, bytes(view[:W * H]))))
        sink.sync()

        started = time.time()
        assert wait_for(lambda: len(got) >= 5, 25), \
            f"only {len(got)} frames crossed the bridge"
        stop.set()

        meta, data = got[-1]
        assert (meta.width, meta.height) == (W, H), \
            f"geometry lost: {meta.width}x{meta.height}"
        assert meta.stride == W, f"stride lost: {meta.stride}"
        assert len(data) == W * H, f"short frame: {len(data)} of {W * H}"

        # H.264 is lossy, so judge the shape of the image, not exact bytes.
        top, mid = data[:W], data[100 * W:101 * W]
        assert top[0] < 25, f"left edge should be dark, got {top[0]}"
        assert top[-1] > 225, f"right edge should be bright, got {top[-1]}"
        assert abs(top[W // 2] - 127) < 20, f"ramp midpoint off: {top[W // 2]}"
        # Every row was identical going in. If the rows disagree coming out, the
        # frame was re-packed against the wrong stride and is sheared.
        skew = sum(abs(a - b) for a, b in zip(top, mid)) / W
        assert skew < 8, f"rows drifted apart by {skew:.1f} — sheared frame"
        print(f"python gst bridge self-check OK "
              f"({len(got)} frames in {time.time() - started:.1f}s)")
    finally:
        for p in procs:
            p.terminate()
            try:
                p.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
        for n in (cam, sink):
            if n is not None:
                n.close()
        daemon.terminate()
        daemon.wait(timeout=5)
        if os.path.exists(sock):
            os.unlink(sock)


if __name__ == "__main__":
    main()
