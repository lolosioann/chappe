# chappe

A small central message broker for C++17, IPC-first — think MQTT pub/sub +
redis get/set + zero-copy shared-memory frames, unified under one client
interface:

- **central daemon** — a `chappe_daemon` process routes between many client
  nodes. Publishers and subscribers don't know each other; they only share a
  topic name.
- **pub/sub** — publish to a topic, and every node subscribed to it receives the
  message (the publisher itself does not — noLocal).
- **get/set** — an authoritative key/value store in the daemon, with a
  read-through cache on each client so warm reads are local.
- **zero-copy frames** — big sensor/video payloads move through POSIX shared
  memory directly producer→consumer; only tiny metadata rides the broker.

Topics are C++ *types* on the client side, so the API is compile-time
type-safe, while the daemon stays a dumb, payload-agnostic string router.
Header-only except for the C shared-memory ring.

## Build

```sh
make daemon     # build the chappe_daemon binary
make test       # build + run all suites
make examples   # build + run examples/basic.cpp (runs an in-process daemon)
```

Requires a C++17 compiler and `-lrt` (POSIX shm).

## Install

```sh
sudo make install                 # -> /usr/local (PREFIX to override)
make install PREFIX=~/.local      # or a user prefix, no sudo
make uninstall                    # same PREFIX/DESTDIR removes it
```

Installs the `chappe_daemon` and `chappe_link` binaries, `libshm_ring.so`, the
C++ headers under `include/chappe/`, a pkg-config file, a CMake package config,
and the Python package. `DESTDIR` is supported for staged/distro packaging.

### Using it from C++

Either integration works; both point at the same headers and library.

```sh
g++ -std=c++17 app.cpp $(pkg-config --cflags --libs chappe) -o app
```

```cmake
find_package(chappe 2.0 REQUIRED)
target_link_libraries(myapp PRIVATE chappe::chappe)   # headers, shm ring, pthread, rt
```

If you installed to a non-standard prefix, point the tools at it with
`PKG_CONFIG_PATH=$PREFIX/lib/pkgconfig` or `-DCMAKE_PREFIX_PATH=$PREFIX`.

### Using it from Python

```sh
pip install chappe            # client + the shm ring, compiled for your interpreter
pip install 'chappe[redis]'   # also pulls redis-py, for the bridge below
```

The wheel builds `src/shm_ring.c` into the package, so frames work off a `pip
install` alone — no `make` step and no `$CHAPPE_LIB`. Releases attach an sdist
and a wheel; the sdist is the portable one, since a wheel is tied to the exact
interpreter and glibc that built it.

`make install` also drops the package under `$PREFIX/lib/chappe/python` for
anyone who would rather set `PYTHONPATH` than use pip — that copy has no
compiled ring beside it, so it falls back to the installed `libshm_ring.so`
(override with `$CHAPPE_LIB`).

## Run the daemon

```sh
./bin/chappe_daemon                      # listens on the default address
./bin/chappe_daemon /tmp/mychappe.sock   # or an explicit path
```

Both the daemon and its clients default to the well-known address — `$CHAPPE_SOCKET`
if set, otherwise `/tmp/chappe.sock` — so the common case never names a path.
Ctrl-C / SIGTERM stops the daemon.

The socket is created `0600`, so only the uid running the daemon can connect —
worth knowing before you put it in a shared `/tmp`. To serve other users, pass
`Server` an allow-list of uids (`Server(path, {1000, 1001})`); the
socket then opens up and `SO_PEERCRED` becomes the gate instead. The list is the
complete set, not an addition, so the daemon's own uid needs to be in it too.

## Usage

Declare messages as plain types and give each a topic name:

```cpp
struct IMUReading { float ax, ay, az; };
MAKE_TOPIC(IMUReading, "imu/reading");   // '/'-separated for wildcard matching
```

Then drive everything through `chappe::Node`.

### Pub/sub

```cpp
chappe::Node sensor("sensor");
chappe::Node vision("vision", 2);   // 2 worker threads => handlers run on a pool
sensor.connect();           // default address; or connect("/tmp/mychappe.sock")
vision.connect();

vision.subscribe([](const IMUReading &m) { /* ... */ });  // type deduced
vision.sync();              // ensure the subscription is live at the daemon
sensor.publish(IMUReading{.ax = 0.1f});
```

Handlers may also be registered *before* `connect()` — the subscriptions are
flushed to the daemon when the connection comes up. To stop receiving a topic,
`unsubscribe`; it drops every local handler for that topic, since the wire
subscription is per-topic and not per-handler.

```cpp
vision.unsubscribe<IMUReading>();
vision.unsubscribe_pattern("cam/*");
```

**Retained messages.** By default a subscriber only receives messages published
while it was subscribed — a publish sent before it registers is lost. For
state/status topics where a late joiner needs the current value, publish with
`retain=true`: the daemon keeps the last value and replays it on subscribe.

```cpp
sensor.publish(SensorState{.calibrated = true}, /*retain=*/true);
// ...a node that subscribes later still gets that last value immediately.
sensor.clear_retained<SensorState>();   // later subscribers get nothing again
```

`clear_retained` goes out as a zero-length retained publish (MQTT convention),
so *current* subscribers do see an empty publish: the default POD codec rejects
0 bytes and those handlers skip it, but codecs that accept 0 bytes
(`std::string`, `std::vector<T>`) fire with a default-constructed value.

`sync()` (a round-trip barrier) is still useful to order setup deterministically
in tests, but retained publishes are the real fix for the publish-before-subscribe
race on state topics. Plain event/data streams stay non-retained.

**Reconnect.** If the daemon restarts or a connection drops, a node's reader
thread reconnects to the same address (exponential backoff) and re-sends its
subscriptions automatically — `connected()` reports the live state. Publishes
during the gap are dropped and the KV cache is invalidated the moment the link
goes down, so a `get()` during the outage reports the key absent rather than a
value that may already be gone (the first one after reconnecting re-fetches); a
node does not yet replay messages it *missed* while disconnected, only resumes
live delivery. Same behavior in the C++ and Python clients.

**Wildcard subscriptions.** For hierarchical topics named with `/`
(e.g. `cam/front`, `sensor/imu/accel`), `subscribe_pattern` matches a bash-path-
like pattern: `+` matches one level, `*` matches the rest.

```cpp
// C++ — handler is untyped (a pattern spans message types): topic + raw bytes
cams.subscribe_pattern("cam/*", [](const std::string &topic, const void *d, size_t n) {
  /* decode based on `topic` */
});
```

```python
# Python — handler gets (topic, payload)
node.subscribe_pattern("cam/+", lambda topic, payload: ...)  # one level under cam/
```

`cam/+` matches `cam/front` but not `cam/front/left`; `cam/*` matches both (and
`cam` itself). Exact `subscribe`/`publish` are unchanged — patterns are matched
by the daemon for routing and by the client for dispatch. Frame topics and
retained replay don't participate in pattern matching (yet).

### Frames (shared memory)

Only the lightweight metadata (`FrameHandle`: timestamp, width, height, stride)
goes through the broker; the bytes live in a shared-memory ring keyed by the
topic name, so producer and consumer agree on the segment with no shared config.
A frame topic derives from `chappe::FrameHandle`:

```cpp
struct FrontCam : chappe::FrameHandle {};
MAKE_TOPIC(FrontCam, "cam/front");

// producer
producer.create_frame_ring<FrontCam>(/*slot_size=*/w*h, /*num_slots=*/4);
producer.publish_frame<FrontCam>(ts, w, h, w, [&](void *dst, size_t n) {
  memcpy(dst, pixels, n);            // decode/render straight into the slot
});

// consumer
consumer.attach_frame_ring<FrontCam>();
consumer.subscribe_frame<FrontCam>([](const FrontCam &meta, chappe::ShmSlotView &slot) {
  process(slot.data(), slot.size()); // slot auto-released after handler returns
});
```

`publish_frame` returns `false` if every slot is held by a consumer (frame
dropped); `frame_drops()` counts frames a subscriber couldn't retain.

### get/set store

```cpp
a.set<int>("gear", 3);              // writes the authoritative value in the daemon
int g = b.get<int>("gear").value(); // first get round-trips + starts watching
int h = b.get<int>("gear").value(); // subsequent gets read the local cache
a.del("gear");                      // erases it; watchers drop their cached copy

using namespace std::chrono_literals;
a.set<int>("alive", 1, 2s);         // the daemon deletes it 2 s from now
a.incr("seq", 1);                   // atomic counter -> the new total
a.setnx<std::string>("lock", "a", 5s);  // won it? -> bool
```

`get<T>` reads the local cache once warm. The first read of a key round-trips to
the daemon and subscribes to future updates, so later `set`s by any node are
pushed into the cache — subsequent reads are local with no round-trip. Unknown
keys return `std::nullopt`. The cache is dropped the moment the link goes down,
not when it comes back: nothing can tell a disconnected node that a key expired
or was deleted, so a `get` during an outage reports the key absent rather than
handing back a value that may be long gone.

`del` erases the key in the daemon and pushes the deletion to every watcher. A
watching node keeps watching, so its next `get` reports the key absent straight
from the cache, with no round-trip.

A `set` with a TTL makes the daemon delete the key that long afterwards and push
the deletion out exactly like a `del`, so "key present" means "the writer was
alive within the last TTL" — a heartbeat with no extra topic. A plain `set`
clears any TTL the key had. Expiry is swept every 100 ms, so a key can be read
for up to that long past its deadline; a client that never round-trips again
still sees it go, which is why the sweep exists at all.

`incr` and `setnx` run inside the daemon's store lock, so concurrent nodes can't
lose an increment or both win a lock the way a get/modify/set pair would.
`incr` fixes one representation — a native-endian `int64` in exactly 8 bytes, so
`get<int64_t>` reads a counter as an ordinary value and an absent key counts as
0; a key holding anything else is a type error (`std::nullopt`, nothing
changed), not a coercion. Give `setnx` a TTL: it is what stops a holder that
dies without `del`ing the key from locking every other node out forever.

### Introspection

`node.info()` returns a human-readable daemon status snapshot — connected
clients, per-topic subscriber counts, pattern/retained/kv totals:

```
clients: 2
topics: 2 (2 subscriptions)
    motor/command=1
    imu/reading=1
patterns: 1
retained: 0
kv_keys: 2
kv_watchers: 1
kv_expiring: 0
```

Counts are of live entries only: a topic, pattern or watched key disappears from
the snapshot once its last subscriber/watcher disconnects.

## Python

`python/chappe/` is a Python client that speaks the same wire protocol, so
Python nodes interoperate with C++ nodes over the same `chappe_daemon`. Messages
are `bytes` — bring your own serialization:

```python
from chappe import Node
import struct

with Node("py") as node:
    node.connect()                                   # $CHAPPE_SOCKET or /tmp/chappe.sock
    node.subscribe("tick", lambda p: print(struct.unpack("=i", p)[0]))
    node.sync()
    node.publish("tick", struct.pack("=i", 42))
    node.set("mode", b"race")
    print(node.get("mode"))                          # b'race'
```

Because it's the same wire format, a Python subscriber decodes a C++ node's
messages directly — a C++ `struct Tick { int seq; }` is `struct.pack("=i", seq)`.

- **pub/sub and get/set** are pure stdlib — no build, no dependencies.
- **Same surface as C++** — `unsubscribe`/`unsubscribe_pattern`,
  `clear_retained(topic)`, and `delete(key)` (spelled out, since `del` is a
  keyword; the C++ name is `del`). The KV extras too: `set(key, value,
  ttl_ms=...)`, `incr(key, by=1)` and `setnx(key, value, ttl_ms=...)`. `incr`
  stores `struct.pack("=q", n)`, the same bytes a C++ node reads with
  `get<int64_t>()`, so a counter is shared across both languages.
- **Handler pool** — `Node("py", threads=4)` runs handlers on 4 workers instead
  of the reader thread. The default (`threads=0`) runs them inline on the
  reader, so keep them quick.
- **Frames** work too, via `python/shm_ring.py`, a `ctypes` binding to the same
  C ring the C++ side uses (so `make libshm_ring` first). Same layout both ways,
  so a Python node can read frames a C++ node produced and vice versa:

  ```python
  cam.create_frame_ring("cam/front", w*h, 4)
  cam.publish_frame("cam/front", ts, w, h, w, pixels)          # producer
  vision.attach_frame_ring("cam/front")
  vision.subscribe_frame("cam/front", lambda meta, view: ...)  # zero-copy view
  ```

Self-checks: `make daemon libshm_ring` then `python3 python/test_chappe.py` and
`python3 python/test_frames.py`. Demos (with a daemon running): `example.py`,
`example_frames.py`.

## Redis bridge

`chappe.redis_bridge` mirrors the bus into Redis, so a monitoring app that
already speaks Redis can watch the system's state and publish into it without
learning our wire protocol:

```sh
python3 -m chappe.redis_bridge --key state/mode --key state/gear \
                               --out 'telemetry/*' --in 'cmd/*'
```

It is an ordinary client — a separate process, no daemon changes, and nothing in
the broker depends on Redis. Only the bridge needs `redis-py`.

- **kv goes out only.** Mirrored keys are copied to `chappe:<key>` byte for byte;
  a delete or an expired TTL removes the Redis key. Nothing writes back, so
  there is no loop to break and no conflict to resolve. A counter written by
  `incr` stays 8 native-endian bytes, so it still reads back through
  `get<int64_t>()` — `redis-cli` will show it as binary and Redis `INCR` won't
  work on it; unpack it instead.
- **Topics go both ways.** `--out` mirrors bus topics to `chappe:<topic>`
  channels, `--in` mirrors Redis channels back onto the bus. A topic that is
  both still arrives exactly once: Redis echoes our own publish back to us, and
  the bridge drops that copy. The other direction needs no such care because
  `route_publish` is noLocal.
- **Keys must be named explicitly.** The store has no enumeration and no
  prefix-watch — a client starts watching a key by getting it — so the bridge
  mirrors only the keys you pass with `--key`.
- **Frames never cross.** A `FrameHandle` names a shared-memory segment on the
  local host, so it is meaningless to a remote monitor. Don't put a frame topic
  in `--out`.

Losing Redis stops the bridge with a non-zero exit rather than reconnecting —
run it under a supervisor. The bus side reconnects on its own, as any node does.

Self-check: `python3 python/test_bridge.py` (skips cleanly without `redis-py` or
a `redis-server` on PATH).

## Cross-device links

Every device runs its own daemon on its own unix socket, and `chappe_link` joins
two of them over TCP. One side listens, the other connects; they are otherwise
symmetric:

```sh
# device A
./bin/chappe_link --listen 0.0.0.0:7000 --topic 'cmd/*' --key state/mode
# device B
./bin/chappe_link --peer a.local:7000   --topic 'cmd/*' --key state/mode
```

A publish on either device reaches subscribers on the other; a `set` or `del` on
a forwarded key crosses too, and a key that already had a value is seeded across
when the link comes up. Nothing else crosses — a link carries exactly what both
its ends were told to carry, and a frame arriving from a peer configured
differently is dropped rather than trusted.

Why a daemon per device instead of one daemon with remote clients: local traffic
never touches the network, frames stay where they can be mapped, and a partition
leaves each device's own bus working. The daemon itself never learns TCP, so it
keeps its unix socket and its `SO_PEERCRED` uid gate.

- **No auth on the wire.** There is no `SO_PEERCRED` equivalent for TCP and no
  token scheme here, because a plaintext token looks like security without being
  it. Put links on a private network — WireGuard, an SSH tunnel, a VLAN — and
  bind `--listen` to the interface you mean.
- **Both ends must share an ABI.** The default codec puts a struct's raw bytes
  on the wire, so if the two devices disagree about byte order, padding, type
  sizes or whether `char` is signed, the bytes still arrive and still decode —
  into plausible garbage. Links therefore open with an ABI fingerprint and
  refuse a peer that disagrees, before any data is applied. Pass
  `--allow-abi-mismatch` if everything you forward is strings or bytes, which
  survive the difference; for genuinely mixed fleets, serialize explicitly
  rather than relying on struct layout.
- **Keys are exact names, topics may be wildcards.** Same reason as the Redis
  bridge: a client starts watching a key by getting it, so there is nothing to
  match a key pattern against.
- **Federated kv is eventually consistent.** There is no cross-device ordering,
  so if two devices write the same key they will disagree about which write was
  last. Keep one owning device per key.
- **Don't forward a frame topic.** A `FrameHandle` names shared memory on the
  host that published it. Forwarding one points the far side at a segment that
  is missing — or worse, at a *different* ring that happens to share the topic
  name. The link cannot detect this for you: on the wire a handle is just bytes.
  Use the frame bridge below, which moves the pixels instead.
- **A dropped link exits** with a non-zero status rather than reconnecting, so a
  supervisor restarts it. Same posture as the Redis bridge.

## Frames across devices

`chappe.gst_bridge` carries a frame topic between devices as H.264 video, since
the handle itself is meaningless off-host. It terminates the frame topic on each
side: the sender attaches to the local ring and encodes, the receiver decodes and
publishes into a ring of its own, so nodes on either device just see an ordinary
local frame topic and the zero-copy path inside each device is untouched.

```sh
# device A, where the camera publishes
python3 -m chappe.gst_bridge --send cam/front --host b.local --port 5000 \
                             --width 640 --height 480
# device B
python3 -m chappe.gst_bridge --recv cam/front --port 5000 \
                             --width 640 --height 480
```

- **This is not the zero-copy path.** H.264 is lossy and encode/network/decode
  adds latency. Right for an operator view or a monitoring app; wrong if the far
  side needs the real sensor bytes to compute on.
- **Both ends must agree on geometry and format.** `FrameHandle` carries only
  timestamp, width, height and stride — no pixel format — so `--format` is
  configured here (`GRAY8` by default).
- **Rows must be a multiple of 4 bytes**, because GStreamer pads to that and a
  chappe ring is flat. The bridge refuses at startup rather than starting up and
  silently forwarding nothing, which is what a mismatch actually does.
- **Bring your own pipeline** with `--pipeline` to change codec or transport;
  the ring end stays ours. The default is H.264 over RTP/UDP with
  `tune=zerolatency`, dropping frames rather than lagging.
- Needs GStreamer and PyGObject, which are system packages rather than pip
  installs — Debian/Ubuntu `python3-gi gstreamer1.0-plugins-{base,good,bad,ugly}
  gstreamer1.0-libav`, Arch `python-gobject gst-plugins-{base,good,bad,ugly}
  gst-libav`.

## Benchmarks

```sh
make bench_shm_ring      # raw shm ring: throughput / latency / contended
make bench_chappe        # C++ broker layer: pub/sub, kv, frames
make daemon libshm_ring && python3 python/bench.py   # same, Python client
```

`bench_chappe` and `bench.py` measure the same things (pub/sub RTT + throughput,
kv cold/warm/set, frame throughput) so C++ and Python are directly comparable.
All are loopback on one host — real numbers depend on the machine; treat them as
relative, not absolute. A captured snapshot lives in [BENCHMARKS.md](BENCHMARKS.md).

## Implementation notes

- **Daemon** (`include/server.hpp`) — thread-per-client, a
  `topic → {subscribers}` routing table, and the authoritative KV store with a
  `key → {watchers}` set. It only ever sees `[kind][topic/key][opaque bytes]`;
  all typing (`MAKE_TOPIC`, `wire_codec`) stays on the clients. Fine for tens of
  long-lived nodes; a single global lock guards the store (see the ponytail
  notes in the source for the ceilings). Client sockets carry a 2 s
  `SO_SNDTIMEO` and a failed write drops that client, so a consumer that stops
  reading is disconnected instead of stalling the store indefinitely with the KV
  lock held. That bounds the stall, it doesn't remove it: a slow-but-draining
  consumer keeps every send making progress and still delays a `set` fan-out.
  One extra thread sweeps expired keys every 100 ms under the same lock. The
  listen socket is `0600` and every accepted connection is checked against
  `SO_PEERCRED` before it is read from — credentials come from the kernel, so a
  client can't forge them, and a `getsockopt` failure denies.

- **Wire protocol** (`include/ipc/transport.hpp`) — one frame shape,
  `[u8 kind][u32 name_len][name][u32 payload_len][payload]`, native-endian
  (same-host assumption). Kinds cover subscribe/publish, kv
  set/setex/del/get/reply/update/incr/setnx/result, and a ping/pong barrier.
  `KV_RESULT` answers `incr`/`setnx` instead of `KV_REPLY` precisely because it
  must *not* populate the client cache: the daemon registers no watcher for
  those, so a cached entry would go stale unnoticed. `wire_codec<T>` handles
  trivially-copyable types as raw bytes, with specializations for `std::string`
  and `std::vector<T>`.

- **KV coherence** — the daemon serializes each store write and its pushes under
  one lock, and clients cache only from daemon-originated frames (reply + push),
  in receive order. So a `get` reply can never overwrite a newer pushed value,
  and every client's cache converges to the daemon's write order.

- **Shared-memory ring** (`src/shm_ring.c`) — single-producer / multi-consumer
  **latest-frame broadcast** (not a FIFO): consumers only ever see the newest
  published frame, held zero-copy via a refcount. Slots cycle through
  `FREE → WRITING → READY`; a seq_cst handshake between the producer's acquire
  and the consumer's retain prevents a slot being overwritten while it's read.
  Geometry lives in the segment, gated by a magic value written last, so a
  consumer can attach knowing only the name.

## Layout

```
include/chappe.hpp        Topic/MAKE_TOPIC + handler-type deduction
include/server.hpp        central broker daemon (chappe::Server)
include/node.hpp          chappe::Node client (pub/sub + frames + get/set)
include/link.hpp          cross-device link (chappe::Link)
include/threadpool.hpp    fixed-size worker pool
include/ipc/              FrameHandle, shm ring C++ wrappers, wire protocol
include/shm_ring.h        C shm ring interface
src/shm_ring.c            C shm ring implementation
src/chappe_daemon.cpp     the daemon main()
src/chappe_link.cpp       the link main()
examples/                 runnable demos, one per transport (see below)
python/chappe/            Python client: __init__.py + shm_ring.py (ctypes) + redis_bridge.py
python/                   demos + self-checks
tests/                    per-layer suites + shm ring stress/bench
```

## Examples

`make examples` builds them all and runs the self-contained ones.

| file | transport | shows |
|------|-----------|-------|
| `basic.cpp`    | pub/sub    | sensor→vision→control→actuator pipeline, sync + async nodes |
| `keyvalue.cpp` | get/set    | daemon-backed store, read-through cache, live update push |
| `frames.cpp`   | shm frames | zero-copy camera→vision, only FrameHandle on the broker |
| `producer.cpp` / `consumer.cpp` | pub/sub | **real cross-process** pub/sub — separate programs over the daemon |

The first three embed the daemon in-process to stay trivially runnable. The
producer/consumer pair is the genuine multi-process path — run each in its own
terminal against a `chappe_daemon`, sharing the message contract in `tick.hpp`:

```sh
./bin/chappe_daemon      # terminal 1
./bin/consumer           # terminal 2 — subscribes, prints ticks
./bin/producer           # terminal 3 — publishes 10 ticks
```
