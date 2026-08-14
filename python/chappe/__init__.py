"""Pure-Python client for the broker daemon.

Speaks the same wire protocol as the C++ Node (see include/ipc/transport.hpp),
so Python nodes interoperate with C++ nodes over the same chappe_daemon — no
compilation, no dependencies, stdlib only.

Covers pub/sub, the get/set store, and the zero-copy shared-memory frame
transport (the last via chappe/shm_ring.py, a ctypes binding to the same C ring
the C++ side uses — a wheel builds it in, otherwise `make libshm_ring`).

The daemon routes opaque payloads by string topic. str/int/float/bool/None/
list/dict are serialized for you and arrive as the same type on another Python
node; bytes are sent untouched. To interoperate with a C++ topic, send bytes
matching whatever that topic's wire_codec<T> produces (a C++
`struct Tick { int seq; }` is `struct.pack("=i", seq)`) — the serialized form is
Python-to-Python only.
"""
import itertools
import json
import os
import socket
import struct
import threading
import time
from collections import namedtuple
from concurrent.futures import Future, ThreadPoolExecutor

# Kept in step with chappe::VERSION and the Makefile by test_chappe.py.
__version__ = "1.0.0"

# Frame kinds — must match the enum in include/ipc/transport.hpp.
(_SUBSCRIBE, _UNSUBSCRIBE, _PUBLISH, _KV_SET, _KV_GET, _KV_REPLY, _KV_UPDATE,
 _PING, _PONG, _PUBLISH_RETAIN, _INFO, _KV_DEL, _KV_SETEX, _KV_INCR, _KV_SETNX,
 _KV_RESULT) = range(16)

_U32 = struct.Struct("=I")  # native-endian u32, matching the C++ memcpy
_I64 = struct.Struct("=q")  # the counter representation incr fixes

# The FrameHandle POD the C++ side puts on the broker for a frame topic:
# { u64 timestamp_ns; u32 width; u32 height; u32 stride; }. The "4x" is the
# struct's trailing padding, so this is exactly sizeof(FrameHandle) == 24.
_FRAME = struct.Struct("=QIII4x")
FrameMeta = namedtuple("FrameMeta", "timestamp_ns width height stride")

# ---- convenience serialization ---------------------------------------------
# publish/set take bytes, because the bus is payload-agnostic and a C++ node on
# the same topic reads whatever wire_codec<T> produces. Requiring every Python
# caller to pack an int by hand is a poor trade for the common Python-only case,
# so anything that isn't already bytes is encoded as JSON behind a short magic.
#
# Bytes still go out untouched, byte for byte. That is what keeps this additive:
# every existing topic, the frame handles and the C++ POD codecs are unaffected,
# and only values that used to raise ("object of type 'int' has no len()") gain
# a representation.
#
# The magic is what makes decoding safe to do unconditionally: a C++ POD payload
# almost never starts with these four bytes, and if one does, the JSON parse
# behind it fails and the raw bytes are handed back. Values encoded this way are
# Python-to-Python — a C++ subscriber sees the JSON as opaque bytes.
_MAGIC = b"\xc7ch\x01"


def _encode(value):
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    try:
        return _MAGIC + json.dumps(value).encode()
    except TypeError as e:
        raise TypeError(
            f"chappe cannot serialize {type(value).__name__}: {e}. bytes, str, "
            "int, float, bool, None, list and dict are supported; bytes nested "
            "inside a list or dict are not — pack those yourself and publish "
            "the result as bytes.") from e


def _decode(payload):
    if not payload.startswith(_MAGIC):
        return payload  # raw bytes: a C++ POD, a frame handle, or bytes we sent
    try:
        return json.loads(payload[len(_MAGIC):])
    except Exception:
        # Deliberately broad. This runs on the reader thread, outside the guard
        # that stops a bad handler killing it, and the payload comes off the
        # wire from any process on the box — or across a link, which has no auth
        # at all. A malformed body is a wrong guess about the magic, not a
        # reason to go deaf: hand back the raw bytes. Nesting deep enough to
        # raise RecursionError is the case a ValueError-only catch misses.
        return payload


def _has_wildcard(s):
    return "+" in s or "*" in s


def _topic_matches(pattern, topic):
    """Bash-path-like match over '/'-separated levels: '+' one level, '*' the
    rest. Mirrors chappe::topic_matches in include/ipc/transport.hpp."""
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


def default_addr():
    return os.environ.get("CHAPPE_SOCKET", "/tmp/chappe.sock")


class Node:
    """A broker client. One connection; a background reader thread delivers
    incoming publishes to handlers and services kv replies. With threads > 0
    handlers run on a pool of that many workers instead of the reader thread,
    so they run concurrently with each other and with further receives."""

    def __init__(self, name, threads=0, decode=True):
        self.name = name
        # decode=False hands handlers and get() the exact bytes off the wire.
        # For anything that forwards payloads on verbatim — the Redis bridge —
        # decoding and re-encoding is the wrong shape: it would have to turn the
        # value back into bytes to pass it along, and a dict is not something
        # redis-py can store.
        self._decode = decode
        self._pool = ThreadPoolExecutor(max_workers=threads) if threads else None
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
        """Connect to the daemon and start the reader, flushing any subscription
        registered beforehand. The initial connect must succeed; if the link
        later drops, the reader reconnects to the same address and resubscribes
        transparently."""
        if self._started:
            raise RuntimeError("node already connected")
        addr = addr or default_addr()
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect(addr)  # initial connect must succeed
        self._addr = addr
        self._sock = s
        self._started = True
        self._running = True
        self._reader = threading.Thread(target=self._run, daemon=True)
        self._reader.start()
        self._resubscribe()  # subscriptions registered before connect()

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
        if self._pool is not None:  # after the join (nothing new is submitted),
            self._pool.shutdown(wait=True)  # before the rings handlers may hold
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
        """Publish `payload` to `topic`. bytes go out untouched; str, int, float,
        bool, None, list and dict are serialized for you and come back as the
        same type on any Python subscriber. With retain=True the daemon keeps it
        as the topic's last value and replays it to future subscribers; the
        default is classic pub/sub (late subscribers miss it).

        Send bytes when a C++ node is on the other end — it decodes with
        wire_codec<T>, which knows nothing about the serialized form."""
        self._require_connected()
        self._send(_PUBLISH_RETAIN if retain else _PUBLISH, topic.encode(),
                   _encode(payload))

    def clear_retained(self, topic):
        """Drop `topic`'s retained value, so late subscribers get nothing. Wire
        form is a retained publish with an empty payload (the MQTT convention);
        current subscribers still see that empty publish."""
        self._require_connected()
        self._send(_PUBLISH_RETAIN, topic.encode(), b"")

    def subscribe(self, topic, handler):
        """Register handler(payload) for `topic`; payload is whatever the
        publisher sent — a str/int/list/dict if it sent one, else bytes. Legal
        before connect(),
        which flushes the subscription to the daemon. Without a handler pool
        handlers run on the reader thread, so keep them quick (or hand off to
        your own queue)."""
        with self._subs_lock:
            handlers = self._subs.setdefault(topic, [])
            first = not handlers
            handlers.append(handler)
        if first:
            self._send(_SUBSCRIBE, topic.encode(), b"")

    def subscribe_pattern(self, pattern, handler):
        """Subscribe to a wildcard pattern ('/'-separated levels; '+' one level,
        '*' the rest). The handler gets (topic, payload) since a pattern spans
        topics. Legal before connect(), like subscribe()."""
        with self._subs_lock:
            handlers = self._pattern_subs.setdefault(pattern, [])
            first = not handlers
            handlers.append(handler)
        if first:
            self._send(_SUBSCRIBE, pattern.encode(), b"")

    def unsubscribe(self, topic):
        """Drop every handler for `topic` and stop the daemon sending it. No-op
        if not subscribed."""
        # Delete the entry, don't just empty it: a lingering empty list would
        # make _resubscribe() re-subscribe after a reconnect.
        with self._subs_lock:
            gone = self._subs.pop(topic, None)
        if gone is None:
            return
        self._send(_UNSUBSCRIBE, topic.encode(), b"")

    def unsubscribe_pattern(self, pattern):
        """unsubscribe() for a wildcard pattern."""
        with self._subs_lock:
            gone = self._pattern_subs.pop(pattern, None)
        if gone is None:
            return
        self._send(_UNSUBSCRIBE, pattern.encode(), b"")

    def _request(self, kind, name, extra=b"", timeout=5.0):
        """Every daemon round-trip has the same shape: take a request id, park a
        future under it, send [u32 id][extra], and wait. None means the link was
        down or the daemon never answered."""
        rid = self._next_id()
        fut = self._register(rid)
        if not self._send(kind, name, _U32.pack(rid) + extra):
            self._fulfill(rid, None)  # disconnected: don't block on a dropped req
        return fut.result(timeout=timeout)

    def sync(self, timeout=5.0):
        """Round-trip barrier: returns once the daemon has processed every frame
        sent so far (e.g. call after subscribe() before a peer publishes)."""
        self._require_connected()
        self._request(_PING, b"", timeout=timeout)

    def info(self, timeout=5.0):
        """Human-readable daemon status (clients, subscription counts, retained/
        kv totals). Empty string if not connected."""
        self._require_connected()
        reply = self._request(_INFO, b"", timeout=timeout)
        return reply.decode() if reply else ""

    # ---- get/set -----------------------------------------------------------

    def set(self, key, value, ttl_ms=0):
        """Store `value` under `key`, serialized like publish(): bytes as-is,
        other types encoded and returned as that type by get(). With ttl_ms > 0
        the daemon deletes the key that many milliseconds later and pushes the
        deletion to watchers; a set without a ttl clears any the key had.

        A counter you intend to incr() is the one case to pack yourself —
        see incr()."""
        self._require_connected()
        value = _encode(value)
        if ttl_ms:
            self._send(_KV_SETEX, key.encode(), _U32.pack(ttl_ms) + value)
        else:
            self._send(_KV_SET, key.encode(), value)

    def incr(self, key, by=1, timeout=5.0):
        """Add `by` to the counter at `key` in the daemon and return the new
        total; an absent key counts as 0. It happens under the daemon's lock, so
        concurrent nodes can't lose an increment the way a get/set pair would.
        The stored value is struct.pack("=q") bytes, so C++ nodes read it with
        get<int64_t>(). None means the key holds something else (nothing was
        changed) or the link is down.

        That representation is fixed, so seed a counter with
        set(key, struct.pack("=q", n)) — set(key, n) stores the serialized form
        instead and incr() will reject it as the wrong type."""
        self._require_connected()
        reply = self._request(_KV_INCR, key.encode(), _I64.pack(by), timeout)
        # Length-checked like the C++ side's wire_codec decode: a reply that
        # isn't exactly 8 bytes is a bad answer, not an exception to raise.
        if reply is None or len(reply) != _I64.size:
            return None
        return _I64.unpack(reply)[0]

    def setnx(self, key, value, ttl_ms=0, timeout=5.0):
        """Set `key` only if it is unset, returning whether this node won it —
        the lock / leader-election primitive. Pass a ttl_ms: it is what stops a
        holder that dies without delete()ing the key from deadlocking every
        other node forever."""
        self._require_connected()
        extra = _U32.pack(ttl_ms) + _encode(value)
        # `is not None`, not a truth test: a win carries zero value bytes.
        return self._request(_KV_SETNX, key.encode(), extra, timeout) is not None

    def delete(self, key):
        """Erase `key` from the store; the daemon pushes the deletion to every
        watcher. Named delete because `del` is a keyword (C++ is Node::del)."""
        self._require_connected()
        self._send(_KV_DEL, key.encode(), b"")

    def get(self, key, timeout=5.0):
        """Return the value, or None if unset. Whatever set() serialized comes
        back as that type; anything else — a C++ node's value, a counter — comes
        back as bytes. First read of a key round-trips to the daemon and starts
        watching it; later reads are served from the local cache, kept fresh by
        the daemon's pushes."""
        self._require_connected()
        with self._kv_lock:
            if key in self._watched:
                raw = self._cache.get(key)
                return raw if raw is None or not self._decode else _decode(raw)
        raw = self._request(_KV_GET, key.encode(), timeout=timeout)
        return raw if raw is None or not self._decode else _decode(raw)

    # ---- frames (shared memory) -------------------------------------------
    # The pixels live in a shm ring keyed by the topic; only the FrameHandle
    # rides the broker. Needs the ring lib (make libshm_ring). Interoperates
    # with C++ frame nodes on the same topic.

    @staticmethod
    def _ring_shm_name(topic):
        # Match ring_shm_name() in node.hpp: leading '/', inner '/' -> '_'.
        return "/chappe_" + topic.replace("/", "_")

    def create_frame_ring(self, topic, slot_size, num_slots):
        """Producer: own the ring for `topic`."""
        from .shm_ring import Ring
        self._rings[topic] = Ring.create(self._ring_shm_name(topic), slot_size, num_slots)

    def attach_frame_ring(self, topic):
        """Consumer: attach to the ring another node created for `topic`."""
        from .shm_ring import Ring
        self._rings[topic] = Ring.attach(self._ring_shm_name(topic))

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
        rejected = 0.01
        while self._running:
            opened = time.monotonic()
            self._read_until_closed(sock)
            with self._send_lock:  # link down: tear down and unblock in-flight
                if self._sock is not None:
                    try:
                        self._sock.close()
                    except OSError:
                        pass
                    self._sock = None
            with self._kv_lock:
                # The daemon's watches died with the link, so the cache stops
                # being authoritative here rather than after the reconnect: for
                # a TTL'd key "still cached" would otherwise mean "still alive"
                # for the whole outage.
                self._cache.clear()
                self._watched.clear()
            self._fail_pending()
            if not self._running:
                return
            # A link that dropped as soon as it opened means the daemon hung up
            # on us — an access-control denial looks exactly like this — and
            # connect() keeps succeeding, so nothing else here would ever sleep.
            if time.monotonic() - opened < 1.0:
                self._backoff_sleep(rejected)
                rejected = min(rejected * 2, 1.0)
            else:
                rejected = 0.01
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
                elif kind == _KV_RESULT and len(payload) >= 5:
                    # Unlike _KV_REPLY this must not cache: the daemon holds no
                    # watcher for an incr/setnx, so the entry would go stale
                    # unnoticed and every later get() would read it.
                    self._fulfill(_U32.unpack(payload[:4])[0],
                                  payload[5:] if payload[4] else None)
                elif kind == _KV_DEL:
                    with self._kv_lock:  # stays watched, so a warm get() reads
                        self._cache.pop(name, None)  # None without a round-trip
                elif kind == _PONG and len(payload) >= 4:
                    self._fulfill(_U32.unpack(payload[:4])[0], None)
                elif kind == _INFO and len(payload) >= 4:
                    self._fulfill(_U32.unpack(payload[:4])[0], payload[4:])
        except OSError:
            pass  # socket error -> treat as disconnect

    def _backoff_sleep(self, delay):
        """Slept in short slices so close() isn't held up for the whole delay."""
        slept = 0.0
        while slept < delay and self._running:
            time.sleep(min(0.02, delay - slept))
            slept += 0.02

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
                self._backoff_sleep(delay)
                delay = min(delay * 2, 1.0)
        return None

    # Re-establish server-side state after a reconnect: re-send SUBSCRIBE for
    # every topic. The KV cache was already dropped when the link went down, so
    # the next get() cold-fetches and re-registers its watch.
    def _resubscribe(self):
        with self._subs_lock:
            topics = list(self._subs.keys()) + list(self._pattern_subs.keys())
        for t in topics:
            self._send(_SUBSCRIBE, t.encode(), b"")

    def _dispatch(self, topic, payload):
        # Decode once for every handler on this message. A frame handle and any
        # C++ POD lack the magic and pass straight through as bytes, so the
        # frame path below is unaffected.
        payload = _decode(payload) if self._decode else payload
        with self._subs_lock:
            handlers = list(self._subs.get(topic, ()))
            pmatched = [h for pat, hs in self._pattern_subs.items()
                        if _topic_matches(pat, topic) for h in hs]
        for handler in handlers:
            self._invoke(handler, payload)
        for handler in pmatched:
            self._invoke(handler, topic, payload)  # patterns also get the topic

    def _invoke(self, handler, *args):
        """Run a handler on the pool if there is one, else inline on the reader
        thread. Never called under _subs_lock."""
        def call():
            try:
                handler(*args)
            except Exception:
                pass  # a bad handler must not kill the reader thread or a worker
        if self._pool is not None:
            self._pool.submit(call)
        else:
            call()

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
