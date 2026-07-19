"""Pure-Python client for the broker daemon.

Speaks the same wire protocol as the C++ Node (see include/ipc/transport.hpp),
so Python nodes interoperate with C++ nodes over the same broker_daemon — no
compilation, no dependencies, stdlib only.

Covers pub/sub, the get/set store, and the zero-copy shared-memory frame
transport (the last via python/shm_ring.py, a ctypes binding to the same C ring
the C++ side uses — so it needs `make libshm_ring`).

Messages are bytes — bring your own serialization (struct, json, msgpack, ...).
The daemon routes opaque payloads by string topic, so to interoperate with a
C++ topic your bytes must match whatever that topic's wire_codec<T> produces
(e.g. a C++ `struct Tick { int seq; }` is `struct.pack("=i", seq)`).
"""
import itertools
import os
import socket
import struct
import threading
import time
from collections import namedtuple
from concurrent.futures import Future

# Frame kinds — must match the enum in include/ipc/transport.hpp.
(_SUBSCRIBE, _UNSUBSCRIBE, _PUBLISH, _KV_SET, _KV_GET, _KV_REPLY, _KV_UPDATE,
 _PING, _PONG, _PUBLISH_RETAIN) = range(10)

_U32 = struct.Struct("=I")  # native-endian u32, matching the C++ memcpy

# The FrameHandle POD the C++ side puts on the broker for a frame topic:
# { u64 timestamp_ns; u32 width; u32 height; u32 stride; }. The "4x" is the
# struct's trailing padding, so this is exactly sizeof(FrameHandle) == 24.
_FRAME = struct.Struct("=QIII4x")
FrameMeta = namedtuple("FrameMeta", "timestamp_ns width height stride")


def _has_wildcard(s):
    return "+" in s or "*" in s


def _topic_matches(pattern, topic):
    """Bash-path-like match over '/'-separated levels: '+' one level, '*' the
    rest. Mirrors ipc::topic_matches in include/ipc/transport.hpp."""
    p, t = pattern.split("/"), topic.split("/")
    for i, level in enumerate(p):
        if level == "*":
            return True
        if i >= len(t):
            return False
        if level == "+":
            continue
        if level != t[i]:
            return False
    return len(p) == len(t)


def default_broker_addr():
    return os.environ.get("BROKER_SOCKET", "/tmp/broker.sock")


class Node:
    """A broker client. One connection; a background reader thread delivers
    incoming publishes to handlers and services kv replies."""

    def __init__(self, name):
        self.name = name
        self._sock = None               # current socket; None during reconnect
        self._addr = None               # address to (re)connect to
        self._started = False           # connect() succeeded at least once
        self._reader = None
        self._running = False
        self._send_lock = threading.Lock()  # guards _sock, serializes sends
        self._subs = {}                 # topic -> list of handler(bytes)
        self._pattern_subs = {}         # pattern -> list of handler(topic, bytes)
        self._subs_lock = threading.Lock()
        self._cache = {}                # key -> bytes
        self._watched = set()           # keys we receive pushes for
        self._kv_lock = threading.Lock()
        self._pending = {}              # req_id -> Future
        self._pending_lock = threading.Lock()
        self._ids = itertools.count(1)  # next(...) is atomic under the GIL
        self._rings = {}                # topic -> shm_ring.Ring
        self._frame_drops = 0

    # ---- lifecycle ---------------------------------------------------------

    def connect(self, addr=None):
        """Connect to the daemon and start the reader. The initial connect must
        succeed; if the link later drops, the reader reconnects to the same
        address and resubscribes transparently."""
        if self._started:
            raise RuntimeError("node already connected")
        addr = addr or default_broker_addr()
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect(addr)  # initial connect must succeed
        self._addr = addr
        self._sock = s
        self._started = True
        self._running = True
        self._reader = threading.Thread(target=self._run, daemon=True)
        self._reader.start()

    def connected(self):
        """True while a live connection exists (False during a reconnect wait)."""
        return self._sock is not None

    def close(self):
        self._running = False
        with self._send_lock:
            if self._sock is not None:
                try:
                    self._sock.shutdown(socket.SHUT_RDWR)  # unblock the reader
                except OSError:
                    pass
        if self._reader is not None:
            self._reader.join()
        with self._send_lock:
            if self._sock is not None:
                self._sock.close()
                self._sock = None
        for ring in self._rings.values():  # reader stopped -> safe to tear down
            ring.destroy()
        self._rings.clear()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ---- pub/sub -----------------------------------------------------------

    def publish(self, topic, payload, retain=False):
        """Publish `payload` (bytes) to `topic`. With retain=True the daemon
        keeps it as the topic's last value and replays it to future subscribers;
        the default is classic pub/sub (late subscribers miss it)."""
        self._require_connected()
        self._send(_PUBLISH_RETAIN if retain else _PUBLISH, topic.encode(), payload)

    def subscribe(self, topic, handler):
        """Register handler(payload: bytes) for `topic`. Handlers run on the
        reader thread, so keep them quick (or hand off to your own queue)."""
        self._require_connected()
        with self._subs_lock:
            handlers = self._subs.setdefault(topic, [])
            first = not handlers
            handlers.append(handler)
        if first:
            self._send(_SUBSCRIBE, topic.encode(), b"")

    def subscribe_pattern(self, pattern, handler):
        """Subscribe to a wildcard pattern ('/'-separated levels; '+' one level,
        '*' the rest). The handler gets (topic, payload) since a pattern spans
        topics."""
        self._require_connected()
        with self._subs_lock:
            handlers = self._pattern_subs.setdefault(pattern, [])
            first = not handlers
            handlers.append(handler)
        if first:
            self._send(_SUBSCRIBE, pattern.encode(), b"")

    def sync(self, timeout=5.0):
        """Round-trip barrier: returns once the daemon has processed every frame
        sent so far (e.g. call after subscribe() before a peer publishes)."""
        self._require_connected()
        rid = self._next_id()
        fut = self._register(rid)
        if not self._send(_PING, b"", _U32.pack(rid)):
            self._fulfill(rid, None)  # disconnected: nothing to flush
        fut.result(timeout=timeout)

    # ---- get/set -----------------------------------------------------------

    def set(self, key, value):
        self._require_connected()
        self._send(_KV_SET, key.encode(), value)

    def get(self, key, timeout=5.0):
        """Return the value bytes, or None if unset. First read of a key
        round-trips to the daemon and starts watching it; later reads are
        served from the local cache, kept fresh by the daemon's pushes."""
        self._require_connected()
        with self._kv_lock:
            if key in self._watched:
                return self._cache.get(key)
        rid = self._next_id()
        fut = self._register(rid)
        if not self._send(_KV_GET, key.encode(), _U32.pack(rid)):
            self._fulfill(rid, None)  # disconnected: don't block on a dropped req
        return fut.result(timeout=timeout)

    # ---- frames (shared memory) -------------------------------------------
    # The pixels live in a shm ring keyed by the topic; only the FrameHandle
    # rides the broker. Needs the ring lib (make libshm_ring). Interoperates
    # with C++ frame nodes on the same topic.

    def create_frame_ring(self, topic, slot_size, num_slots):
        """Producer: own the ring for `topic`."""
        from shm_ring import Ring
        self._rings[topic] = Ring.create("/broker_" + topic, slot_size, num_slots)

    def attach_frame_ring(self, topic):
        """Consumer: attach to the ring another node created for `topic`."""
        from shm_ring import Ring
        self._rings[topic] = Ring.attach("/broker_" + topic)

    def publish_frame(self, topic, timestamp_ns, width, height, stride, data):
        """Write `data` into the ring zero-copy and announce it on the broker.
        Returns False if every slot is held by a consumer (frame dropped)."""
        self._require_connected()
        ring = self._rings.get(topic)
        if ring is None:
            raise RuntimeError(f"publish_frame: no ring for topic {topic!r}")
        if not ring.write(data):
            return False
        self.publish(topic, _FRAME.pack(timestamp_ns, width, height, stride))
        return True

    def subscribe_frame(self, topic, handler):
        """Register handler(meta: FrameMeta, view: memoryview). The view is a
        zero-copy read into shm, released after the handler returns — copy it out
        to keep it. Attach the ring first, or frames count as drops."""
        def wrapper(payload):
            ring = self._rings.get(topic)
            if ring is None:
                self._frame_drops += 1
                return
            retained = ring.retain_latest()
            if retained is None:
                self._frame_drops += 1  # nothing ready / producer reclaimed it
                return
            idx, view = retained
            try:
                handler(FrameMeta(*_FRAME.unpack(payload)), view)
            finally:
                ring.release(idx)
        self.subscribe(topic, wrapper)

    def frame_drops(self):
        return self._frame_drops

    # ---- internals ---------------------------------------------------------

    def _require_connected(self):
        if not self._started:
            raise RuntimeError("node is not connected to a broker")

    def _next_id(self):
        return next(self._ids) & 0xFFFFFFFF

    def _register(self, rid):
        fut = Future()
        with self._pending_lock:
            self._pending[rid] = fut
        return fut

    def _fulfill(self, rid, value):
        with self._pending_lock:
            fut = self._pending.pop(rid, None)
        if fut is not None and not fut.done():
            fut.set_result(value)

    def _send(self, kind, name, payload):
        """Send a frame. Returns True if written; False during a reconnect
        window (so request/reply callers don't block on a lost frame)."""
        frame = (bytes((kind,)) + _U32.pack(len(name)) + name +
                 _U32.pack(len(payload)) + payload)
        with self._send_lock:
            if self._sock is None:
                return False
            try:
                self._sock.sendall(frame)
                return True
            except OSError:
                return False

    def _recv_exact(self, sock, n):
        buf = bytearray()
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                return None  # peer closed
            buf += chunk
        return bytes(buf)

    def _read_frame(self, sock):
        hdr = self._recv_exact(sock, 1)
        if hdr is None:
            return None
        nlen = self._recv_exact(sock, 4)
        if nlen is None:
            return None
        name = self._recv_exact(sock, _U32.unpack(nlen)[0])
        if name is None:
            return None
        plen = self._recv_exact(sock, 4)
        if plen is None:
            return None
        payload = self._recv_exact(sock, _U32.unpack(plen)[0])
        if payload is None:
            return None
        return hdr[0], name.decode(), payload

    # Connection manager: read until the link drops, then reconnect to the same
    # address and resubscribe, until the node is closed.
    def _run(self):
        sock = self._sock  # from connect()
        while self._running:
            self._read_until_closed(sock)
            with self._send_lock:  # link down: tear down and unblock in-flight
                if self._sock is not None:
                    try:
                        self._sock.close()
                    except OSError:
                        pass
                    self._sock = None
            self._fail_pending()
            if not self._running:
                return
            sock = self._reconnect_backoff()
            if sock is None:
                return  # closed while backing off
            with self._send_lock:
                self._sock = sock
            self._resubscribe()

    def _read_until_closed(self, sock):
        try:
            while self._running:
                frame = self._read_frame(sock)
                if frame is None:
                    break
                kind, name, payload = frame
                if kind == _PUBLISH:
                    self._dispatch(name, payload)
                elif kind == _KV_REPLY:
                    self._on_kv_reply(name, payload)
                elif kind == _KV_UPDATE:
                    with self._kv_lock:
                        self._watched.add(name)
                        self._cache[name] = payload
                elif kind == _PONG and len(payload) >= 4:
                    self._fulfill(_U32.unpack(payload[:4])[0], None)
        except OSError:
            pass  # socket error -> treat as disconnect

    def _reconnect_backoff(self):
        delay = 0.01
        while self._running:
            try:
                s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                s.connect(self._addr)
                return s
            except OSError:
                try:
                    s.close()
                except OSError:
                    pass
                slept = 0.0
                while slept < delay and self._running:
                    time.sleep(min(0.02, delay - slept))
                    slept += 0.02
                delay = min(delay * 2, 1.0)
        return None

    # Re-establish server-side state after a reconnect: re-send SUBSCRIBE for
    # every topic. The daemon's KV watches are gone, so drop the local cache —
    # the next get() cold-fetches and re-registers the watch.
    def _resubscribe(self):
        with self._subs_lock:
            topics = list(self._subs.keys()) + list(self._pattern_subs.keys())
        for t in topics:
            self._send(_SUBSCRIBE, t.encode(), b"")
        with self._kv_lock:
            self._cache.clear()
            self._watched.clear()

    def _dispatch(self, topic, payload):
        with self._subs_lock:
            handlers = list(self._subs.get(topic, ()))
            pmatched = [h for pat, hs in self._pattern_subs.items()
                        if _topic_matches(pat, topic) for h in hs]
        for handler in handlers:
            try:
                handler(payload)
            except Exception:
                pass  # a bad handler must not kill the reader thread
        for handler in pmatched:
            try:
                handler(topic, payload)  # pattern handlers also get the topic
            except Exception:
                pass

    def _on_kv_reply(self, key, payload):
        if len(payload) < 5:
            return
        rid = _U32.unpack(payload[:4])[0]
        found = payload[4] != 0
        value = payload[5:] if found else None
        with self._kv_lock:  # cache here, in receive order, like the C++ client
            self._watched.add(key)
            if found:
                self._cache[key] = value
            else:
                self._cache.pop(key, None)
        self._fulfill(rid, value)

    def _fail_pending(self):
        with self._pending_lock:
            pending = list(self._pending.values())
            self._pending.clear()
        for fut in pending:
            if not fut.done():
                fut.set_result(None)
