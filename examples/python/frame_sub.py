import time

from chappe import Node

W, H = 16, 16

# ASCII ramp for grayscale -> terminal art.
RAMP = " .:-=+*#%@"


def show_frame(meta, view):
    """Render a 16x16 grayscale frame as ASCII art in the terminal."""
    px = view.cast("B")
    print(f"\n[vision] ts={meta.timestamp_ns} {meta.width}x{meta.height}")
    for y in range(meta.height):
        row = ""
        for x in range(meta.width):
            v = px[y * meta.stride + x]
            row += RAMP[v * (len(RAMP) - 1) // 255]
        print("  " + row)


def main():
    """
    Vision node: attaches to the camera's shared-memory ring and renders
    each incoming frame as ASCII art in the terminal.
    """
    vision = Node("vision")
    try:
        vision.connect()
    except Exception:
        print("Could not connect to daemon. Make sure chappe is installed and running")
        return

    # No need to wait for the publisher: the ring is attached on the first
    # frame, so this node can start first.
    vision.subscribe_frame("cam/front", show_frame)
    vision.sync()

    print("-- vision subscribed, waiting for frames (start frame_pub.py) --")
    while True:
        time.sleep(10)


if __name__ == "__main__":
    main()
