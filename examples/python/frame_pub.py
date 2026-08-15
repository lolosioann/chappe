import time

from chappe import Node

W, H = 16, 16


def make_frame(frame_num):
    """Build a 16x16 grayscale frame: a bright bar that walks down."""
    data = bytearray(W * H)
    bar_y = frame_num % H
    for y in range(H):
        brightness = 255 if y == bar_y else (40 + y * 10)
        for x in range(W):
            data[y * W + x] = brightness & 0xFF
    return bytes(data)


def main():
    """
    Camera node: publishes 16x16 grayscale frames into a shared-memory
    ring.  Run frame_sub.py in another terminal to see them.
    """
    # `with` matters for a producer: closing the node destroys the ring, which
    # is what unlinks the shared-memory segment. Leaving it behind is survivable
    # (the next run reclaims it) but it lingers until reboot.
    with Node("camera") as camera:
        try:
            camera.connect()
        except Exception:
            print("Could not connect to daemon. Make sure chappe is installed and running")
            return

        camera.create_frame_ring("cam/front", W * H, 4)

        print(f"-- camera publishing {W}x{H} frames --")
        try:
            for f in range(30):
                ok = camera.publish_frame("cam/front", 1000 + f, W, H, W,
                                          make_frame(f))
                if not ok:
                    print("[camera] frame dropped (all slots held)")
                time.sleep(0.1)
        except KeyboardInterrupt:
            pass
        print("-- camera done --")


if __name__ == "__main__":
    main()
