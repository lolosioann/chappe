# broker

A small central message broker for C++17, IPC-first — think MQTT pub/sub +
redis get/set + zero-copy shared-memory frames, unified under one client
interface:

- **central daemon** — a `broker_daemon` process routes between many client
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
make daemon     # build the broker_daemon binary
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

Installs the `broker_daemon` binary, `libshm_ring.so`, the C++ headers under
`include/broker/`, and the Python modules. It prints the exact flags; in short:

- **C++:** `-I$PREFIX/include/broker`, link `$PREFIX/lib/libshm_ring.so -lrt`.
- **Python:** `export PYTHONPATH=$PREFIX/lib/broker/python` (set `$BROKER_LIB`
  if `libshm_ring.so` isn't on a standard path). Supports `DESTDIR` for staged
  packaging.

## Run the daemon

```sh
./bin/broker_daemon                      # listens on the default address
./bin/broker_daemon /tmp/mybroker.sock   # or an explicit path
```

Both the daemon and its clients default to the well-known address — `$BROKER_SOCKET`
if set, otherwise `/tmp/broker.sock` — so the common case never names a path.
Ctrl-C / SIGTERM stops the daemon.

## Usage

Declare messages as plain types and give each a topic name:

```cpp
struct IMUReading { float ax, ay, az; };
MAKE_TOPIC(IMUReading, "imu/reading");   // '/'-separated for wildcard matching
```

Then drive everything through `Node`.

### Pub/sub

```cpp
Node sensor("sensor");
Node vision("vision", 2);   // 2 worker threads => handlers run on a pool
sensor.connect();           // default address; or connect("/tmp/mybroker.sock")
vision.connect();

vision.subscribe([](const IMUReading &m) { /* ... */ });  // type deduced
vision.sync();              // ensure the subscription is live at the daemon
sensor.publish(IMUReading{.ax = 0.1f});
```

**Retained messages.** By default a subscriber only receives messages published
while it was subscribed — a publish sent before it registers is lost. For
state/status topics where a late joiner needs the current value, publish with
`retain=true`: the daemon keeps the last value and replays it on subscribe.

```cpp
sensor.publish(SensorState{.calibrated = true}, /*retain=*/true);
// ...a node that subscribes later still gets that last value immediately.
```

`sync()` (a round-trip barrier) is still useful to order setup deterministically
in tests, but retained publishes are the real fix for the publish-before-subscribe
race on state topics. Plain event/data streams stay non-retained.

**Reconnect.** If the daemon restarts or a connection drops, a node's reader
thread reconnects to the same address (exponential backoff) and re-sends its
subscriptions automatically — `connected()` reports the live state. Publishes
during the gap are dropped and the KV cache is invalidated (the next `get()`
re-fetches); a node does not yet replay messages it *missed* while disconnected,
only resumes live delivery. Same behavior in the C++ and Python clients.

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
A frame topic derives from `ipc::FrameHandle`:

```cpp
struct FrontCam : ipc::FrameHandle {};
MAKE_TOPIC(FrontCam, "cam/front");

// producer
producer.create_frame_ring<FrontCam>(/*slot_size=*/w*h, /*num_slots=*/4);
producer.publish_frame<FrontCam>(ts, w, h, w, [&](void *dst, size_t n) {
  memcpy(dst, pixels, n);            // decode/render straight into the slot
});

// consumer
consumer.attach_frame_ring<FrontCam>();
consumer.subscribe_frame<FrontCam>([](const FrontCam &meta, ipc::ShmSlotView &slot) {
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
```

`get<T>` reads the local cache once warm. The first read of a key round-trips to
the daemon and subscribes to future updates, so later `set`s by any node are
pushed into the cache — subsequent reads are local with no round-trip. Unknown
keys return `std::nullopt`.

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
```

## Python

`python/broker.py` is a Python client that speaks the same wire protocol, so
Python nodes interoperate with C++ nodes over the same `broker_daemon`. Messages
are `bytes` — bring your own serialization:

```python
from broker import Node
import struct

with Node("py") as node:
    node.connect()                                   # $BROKER_SOCKET or /tmp/broker.sock
    node.subscribe("tick", lambda p: print(struct.unpack("=i", p)[0]))
    node.sync()
    node.publish("tick", struct.pack("=i", 42))
    node.set("mode", b"race")
    print(node.get("mode"))                          # b'race'
```

Because it's the same wire format, a Python subscriber decodes a C++ node's
messages directly — a C++ `struct Tick { int seq; }` is `struct.pack("=i", seq)`.

- **pub/sub and get/set** are pure stdlib — no build, no dependencies.
- **Frames** work too, via `python/shm_ring.py`, a `ctypes` binding to the same
  C ring the C++ side uses (so `make libshm_ring` first). Same layout both ways,
  so a Python node can read frames a C++ node produced and vice versa:

  ```python
  cam.create_frame_ring("cam/front", w*h, 4)
  cam.publish_frame("cam/front", ts, w, h, w, pixels)          # producer
  vision.attach_frame_ring("cam/front")
  vision.subscribe_frame("cam/front", lambda meta, view: ...)  # zero-copy view
  ```

Self-checks: `make daemon libshm_ring` then `python3 python/test_broker.py` and
`python3 python/test_frames.py`. Demos (with a daemon running): `example.py`,
`example_frames.py`.

## Benchmarks

```sh
make bench_shm_ring      # raw shm ring: throughput / latency / contended
make bench_broker        # C++ broker layer: pub/sub, kv, frames
make daemon libshm_ring && python3 python/bench.py   # same, Python client
```

`bench_broker` and `bench.py` measure the same things (pub/sub RTT + throughput,
kv cold/warm/set, frame throughput) so C++ and Python are directly comparable.
All are loopback on one host — real numbers depend on the machine; treat them as
relative, not absolute. A captured snapshot lives in [BENCHMARKS.md](BENCHMARKS.md).

## Implementation notes

- **Daemon** (`include/broker_server.hpp`) — thread-per-client, a
  `topic → {subscribers}` routing table, and the authoritative KV store with a
  `key → {watchers}` set. It only ever sees `[kind][topic/key][opaque bytes]`;
  all typing (`MAKE_TOPIC`, `wire_codec`) stays on the clients. Fine for tens of
  long-lived nodes; a single global lock guards the store (see the ponytail
  notes in the source for the ceilings).

- **Wire protocol** (`include/ipc/transport.hpp`) — one frame shape,
  `[u8 kind][u32 name_len][name][u32 payload_len][payload]`, native-endian
  (same-host assumption). Kinds cover subscribe/publish, kv set/get/reply/update,
  and a ping/pong barrier. `wire_codec<T>` handles trivially-copyable types as
  raw bytes, with specializations for `std::string` and `std::vector<T>`.

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
include/broker.hpp        Topic/MAKE_TOPIC + handler-type deduction
include/broker_server.hpp central broker daemon (BrokerServer)
include/node.hpp          Node client (pub/sub + frames + get/set)
include/threadpool.hpp    fixed-size worker pool
include/ipc/              FrameHandle, shm ring C++ wrappers, wire protocol
include/shm_ring.h        C shm ring interface
src/shm_ring.c            C shm ring implementation
src/broker_daemon.cpp     the daemon main()
examples/                 runnable demos, one per transport (see below)
python/                   Python client: broker.py + shm_ring.py (ctypes) + demos + checks
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
terminal against a `broker_daemon`, sharing the message contract in `tick.hpp`:

```sh
./bin/broker_daemon      # terminal 1
./bin/consumer           # terminal 2 — subscribes, prints ticks
./bin/producer           # terminal 3 — publishes 10 ticks
```
