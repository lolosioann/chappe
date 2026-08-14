#!/usr/bin/env python3
"""Carry a frame topic between devices as H.264 video.

    camera --shm--> ring --> gst_bridge --rtp/udp--> gst_bridge --> ring --shm--> vision
    |------------ device A ------------|            |------------ device B ------------|

Frames are the one thing chappe_link deliberately refuses to forward: a
FrameHandle names shared memory on the host that published it, so relaying one
only points the far side at a segment that is missing — or worse, at a different
local ring that happens to share the name. This bridge moves the pixels instead,
terminating the frame topic on each device: the sender attaches to the local
ring and encodes, the receiver decodes and publishes into a ring of its own.
Nodes on either side just see an ordinary local frame topic, and the zero-copy
path inside each device is untouched.

What you give up: this is not the zero-copy path. H.264 is lossy, and
encode/network/decode adds latency. Right for an operator view or a monitoring
app; wrong if the far side needs the real sensor bytes to compute on.

FrameHandle carries no pixel format — only timestamp/width/height/stride — so
the format is configured here and both ends must agree, exactly as they already
must to share a local ring.

    # device A, where the camera publishes
    python3 -m chappe.gst_bridge --send cam/front --host b.local --port 5000 \\
                                 --width 640 --height 480
    # device B
    python3 -m chappe.gst_bridge --recv cam/front --port 5000 \\
                                 --width 640 --height 480

Needs GStreamer and its Python bindings. Those are system packages, not pip
installs — Debian/Ubuntu: python3-gi gir1.2-gst-plugins-base-1.0
gstreamer1.0-plugins-{base,good,bad,ugly} gstreamer1.0-libav; Arch:
python-gobject gst-plugins-{base,good,bad,ugly} gst-libav.
"""
import argparse
import os
import signal
import sys

# Lets this run as a plain script as well as `python3 -m chappe.gst_bridge`.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from chappe import Node, default_addr

try:
    import gi

    gi.require_version("Gst", "1.0")
    gi.require_version("GstVideo", "1.0")
    from gi.repository import GLib, Gst, GstVideo
except (ImportError, ValueError) as e:
    sys.exit(f"gst_bridge needs GStreamer and PyGObject: {e}")

# GStreamer format name -> bytes per pixel, used only to default --stride.
# Packed formats only: a planar one (NV12, I420) has no single stride, so it
# cannot describe the ring's flat slot.
_BPP = {"GRAY8": 1, "GRAY16_LE": 2, "GRAY16_BE": 2, "RGB": 3, "BGR": 3,
        "RGBA": 4, "BGRA": 4, "RGBx": 4, "BGRx": 4}


def build_send(args):
    """Local ring -> H.264 -> the network."""
    caps = (f"video/x-raw,format={args.format},width={args.width},"
            f"height={args.height},framerate={args.fps}/1")
    tail = args.pipeline or (
        f"x264enc tune=zerolatency speed-preset=ultrafast bitrate={args.bitrate} "
        # A receiver that joins late has to be told how to decode: without a
        # periodic keyframe and inline SPS/PPS it shows nothing until one
        # happens along by chance.
        f"key-int-max={args.fps} ! rtph264pay config-interval=1 pt=96 "
        f"! udpsink host={args.host} port={args.port}")
    # leaky=downstream: if the encoder falls behind, drop old frames rather than
    # queue them. A live view wants to be current, not complete.
    pipeline = Gst.parse_launch(
        f"appsrc name=src is-live=true format=time caps={caps} "
        f"! queue max-size-buffers=4 leaky=downstream ! videoconvert ! {tail}")
    src = pipeline.get_by_name("src")

    node = Node("gst_send")
    node.connect(args.socket)
    node.attach_frame_ring(args.topic)
    frame_bytes = args.height * args.stride
    base = {}

    def on_frame(meta, view):
        # retain_latest() hands back the whole slot; this frame is only the
        # first height*stride bytes of it. The copy is deliberate — the view is
        # released when this returns, and the encoder outlives that.
        buf = Gst.Buffer.new_wrapped(bytes(view[:frame_bytes]))
        # PTS is running time from the first frame, not the camera's epoch.
        base.setdefault("t0", meta.timestamp_ns)
        buf.pts = max(0, meta.timestamp_ns - base["t0"])
        buf.duration = Gst.SECOND // args.fps
        src.emit("push-buffer", buf)

    node.subscribe_frame(args.topic, on_frame)
    node.sync()
    return pipeline, node


def build_recv(args):
    """The network -> decoded frames -> a local ring we own."""
    head = args.pipeline or (
        f"udpsrc port={args.port} "
        f"caps=application/x-rtp,media=video,encoding-name=H264,payload=96 "
        f"! rtpjitterbuffer latency={args.latency} ! rtph264depay ! avdec_h264")
    pipeline = Gst.parse_launch(
        f"{head} ! videoconvert ! video/x-raw,format={args.format} "
        f"! appsink name=sink emit-signals=true sync=false max-buffers=2 drop=true")
    sink = pipeline.get_by_name("sink")

    node = Node("gst_recv")
    node.connect(args.socket)
    node.create_frame_ring(args.topic, args.height * args.stride, args.slots)
    row = args.stride

    def on_sample(appsink):
        sample = appsink.emit("pull-sample")
        if sample is None:
            return Gst.FlowReturn.OK
        buf = sample.get_buffer()
        ok, info = buf.map(Gst.MapFlags.READ)
        if not ok:
            return Gst.FlowReturn.OK
        try:
            # GStreamer pads rows to its own alignment, so its stride need not
            # be ours. Repack row by row when they differ — shipping the padded
            # buffer as if it were flat shears the image by a few pixels per
            # row, which looks like a decoder bug rather than a layout one.
            vi = GstVideo.VideoInfo.new_from_caps(sample.get_caps())
            gst_stride = vi.stride[0] if vi else row
            data = bytes(info.data)
            if gst_stride != row:
                data = b"".join(data[y * gst_stride: y * gst_stride + row]
                                for y in range(args.height))
            else:
                data = data[:args.height * row]
            ts = 0 if buf.pts == Gst.CLOCK_TIME_NONE else buf.pts
            node.publish_frame(args.topic, ts, args.width, args.height, row, data)
        finally:
            buf.unmap(info)
        return Gst.FlowReturn.OK

    sink.connect("new-sample", on_sample)
    return pipeline, node


def main(argv=None):
    p = argparse.ArgumentParser(
        prog="python -m chappe.gst_bridge",
        description="Carry a frame topic between devices as H.264 video.")
    p.add_argument("--send", metavar="TOPIC", help="encode this local frame topic out")
    p.add_argument("--recv", metavar="TOPIC", help="decode into this local frame topic")
    p.add_argument("--socket", default=default_addr(), help="local broker socket")
    p.add_argument("--host", default="127.0.0.1", help="peer host (--send)")
    p.add_argument("--port", type=int, default=5000)
    p.add_argument("--width", type=int, required=True)
    p.add_argument("--height", type=int, required=True)
    p.add_argument("--format", default="GRAY8",
                   help=f"pixel format, one of: {', '.join(sorted(_BPP))}")
    p.add_argument("--stride", type=int, help="row bytes (default width*bpp)")
    p.add_argument("--fps", type=int, default=30)
    p.add_argument("--bitrate", type=int, default=2000, help="kbit/s (--send)")
    p.add_argument("--latency", type=int, default=50,
                   help="jitter buffer ms (--recv); trades lag for loss")
    p.add_argument("--slots", type=int, default=4, help="ring slots (--recv)")
    p.add_argument("--pipeline", help="replace the codec/transport half of the "
                                      "pipeline; the ring end stays ours")
    args = p.parse_args(argv)

    if (args.send is None) == (args.recv is None):
        p.error("pass exactly one of --send or --recv")
    if args.format not in _BPP:
        p.error(f"--format must be one of: {', '.join(sorted(_BPP))}")
    args.topic = args.send or args.recv
    if args.stride is None:
        args.stride = args.width * _BPP[args.format]

    Gst.init(None)

    # GStreamer pads rows to its own alignment. A chappe ring is flat
    # height*stride, so when the two disagree the pipeline does not error — it
    # simply produces nothing, which is the worst way to find out. Refuse up
    # front and say which number to change. Padding both directions in Python
    # was the alternative and it is not worth it: real sensors are aligned.
    info = GstVideo.VideoInfo.new_from_caps(Gst.Caps.from_string(
        f"video/x-raw,format={args.format},width={args.width},"
        f"height={args.height},framerate={args.fps}/1"))
    if info and (info.stride[0] != args.stride or info.size != args.height * args.stride):
        sys.exit(f"gst_bridge: GStreamer wants {info.stride[0]}-byte rows for "
                 f"{args.width}x{args.height} {args.format} ({info.size} bytes a "
                 f"frame); this ring is flat {args.stride}-byte rows "
                 f"({args.height * args.stride} bytes). Rows have to be a "
                 f"multiple of 4 bytes, so pick a width where width*{_BPP[args.format]} "
                 f"is — or pass --stride {info.stride[0]} if the producer already "
                 f"pads its rows to that.")
    pipeline, node = (build_send if args.send else build_recv)(args)

    loop = GLib.MainLoop()
    failure = {}

    def on_message(_bus, msg):
        if msg.type == Gst.MessageType.ERROR:
            err, _ = msg.parse_error()
            failure["err"] = str(err)
            loop.quit()
        elif msg.type == Gst.MessageType.EOS:
            loop.quit()

    bus = pipeline.get_bus()
    bus.add_signal_watch()
    bus.connect("message", on_message)

    # A Python signal handler cannot interrupt GLib's main loop while it sits in
    # C, so raise a flag and let a timer notice it. GLib's own unix_signal_add
    # would do this natively but has moved between PyGObject versions, and this
    # needs no branch on which one is installed.
    stopping = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stopping.append(True))
    GLib.timeout_add(200, lambda: loop.quit() if stopping else True)

    pipeline.set_state(Gst.State.PLAYING)
    print(f"gst_bridge {'sending' if args.send else 'receiving'} {args.topic} "
          f"{args.width}x{args.height} {args.format} on port {args.port}",
          flush=True)
    try:
        loop.run()
    finally:
        pipeline.set_state(Gst.State.NULL)
        node.close()
    if failure:
        sys.exit(f"gstreamer: {failure['err']}")


if __name__ == "__main__":
    main()
