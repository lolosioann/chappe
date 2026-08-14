#!/usr/bin/env python3
"""Python frame-transport demo. Build the ring lib and start a daemon first:
    make daemon libshm_ring
    ./bin/chappe_daemon
Then:  python3 python/example_frames.py

A camera node publishes frames into shared memory; a vision node reads them
zero-copy. Only the FrameHandle metadata crosses the broker.
"""
import time

from chappe import Node

W, H = 8, 8  # tiny 8x8 grayscale frames


def main():
    with Node("camera") as camera, Node("vision") as vision:
        camera.connect()
        vision.connect()
        camera.create_frame_ring("cam/demo", W * H, 4)
        vision.attach_frame_ring("cam/demo")

        def on_frame(meta, view):
            px = view.cast("B")  # bytes as ints, still zero-copy into shm
            avg = sum(px) // len(px)
            print(f"[vision] ts={meta.timestamp_ns} {meta.width}x{meta.height} avg={avg}")

        vision.subscribe_frame("cam/demo", on_frame)
        vision.sync()

        print("-- camera publishing 5 frames --")
        for f in range(5):
            camera.publish_frame("cam/demo", 1000 + f, W, H, W,
                                 bytes([40 + f * 20]) * (W * H))
            time.sleep(0.02)
        time.sleep(0.2)


if __name__ == "__main__":
    main()
