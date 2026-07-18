# broker

A typed, in-process publish/subscribe message bus for C++17 that scales out to
two things the same API can't normally reach:

- **zero-copy frames** — big sensor/video payloads move through POSIX shared
  memory, not the broker.
- **other processes** — a socket bridge mirrors selected topics across a
  connection, and carries a small replicated key/value store on the side.

Topics are C++ *types*, so dispatch is resolved at compile time — no string
lookups on the publish path. Header-only except for the C shared-memory ring.

## Build

```sh
make test       # build + run all suites
make examples   # build + run examples/basic.cpp
```

Requires a C++17 compiler and `-lrt` (POSIX shm). See the `Makefile` for
individual targets (`test_broker`, `stress_shm_ring`, `bench_shm_ring`, ...).

## Usage

Everything is driven through `Node` — a per-participant facade over the broker.
First declare messages as plain types and give each a topic name:

```cpp
struct IMUReading { float ax, ay, az; };
MAKE_TOPIC(IMUReading, "imu.reading");
```

### Pub/sub

```cpp
Broker<IMUReading, /* ...other topics... */> broker;

// 0 threads => handlers run synchronously on the publisher's thread.
// N threads => handlers run on an N-worker pool.
Node<IMUReading> sensor("sensor", broker);
Node<IMUReading> vision("vision", broker, 2);

vision.subscribe([](const IMUReading &m) { /* ... */ });   // type deduced
sensor.publish(IMUReading{.ax = 0.1f});

vision.drain();   // wait for async handlers (tests/shutdown)
```

Subscriptions live as long as the `Node`; destroying it unsubscribes.

### Frames (shared memory)

For large payloads, only lightweight metadata (`FrameHandle`: timestamp, width,
height, stride) goes through the broker — the bytes live in a shared-memory ring
keyed by the topic name, so producer and consumer agree on the segment with no
shared config. A frame topic derives from `ipc::FrameHandle`:

```cpp
struct FrontCam : ipc::FrameHandle {};
MAKE_TOPIC(FrontCam, "cam.front");

// producer process
producer.create_frame_ring<FrontCam>(/*slot_size=*/w*h, /*num_slots=*/4);
producer.publish_frame<FrontCam>(ts, w, h, w, [&](void *dst, size_t n) {
  memcpy(dst, pixels, n);           // decode/render straight into the slot
});

// consumer process
consumer.attach_frame_ring<FrontCam>();
consumer.subscribe_frame<FrontCam>([](const FrontCam &meta, ipc::ShmSlotView &slot) {
  process(slot.data(), slot.size()); // slot auto-released after handler returns
});
```

`publish_frame` returns `false` if every slot is held by a consumer (frame
dropped); `frame_drops()` counts frames a subscriber couldn't retain.

### Bridging to another process

```cpp
// process A
a.bridge_listen("/tmp/link.sock");   // blocks until B connects
a.bridge_forward<IMUReading>();      // local publishes of this topic go to B

// process B
b.bridge_connect("/tmp/link.sock");
b.subscribe([](const IMUReading &m) { /* receives A's publishes */ });
```

Only topics you `bridge_forward<T>()` are sent; any forwarded topic arriving from
the peer is republished into the local broker.

### Replicated key/value store

Layered on the same bridge — a last-writer-wins latest-value store:

```cpp
a.set<int>("gear", 3);                       // writes locally, pushes to peer
std::optional<int> g = b.get<int>("gear");   // reads B's replica
```

Eventually consistent; values set before a peer connects are not back-filled.

## Implementation notes

- **Broker** — `Broker<Topics...>` holds one `TopicState<T>` per topic in a
  tuple. Each state guards its handler list with a `shared_mutex` and snapshots
  it before dispatch, so publishing never holds the lock across a handler.

- **Shared-memory ring** (`src/shm_ring.c`) — single-producer / multi-consumer
  **latest-frame broadcast** (not a FIFO): consumers only ever see the newest
  published frame, held zero-copy via a refcount. Slots cycle through
  `FREE → WRITING → READY`; a seq_cst handshake between the producer's acquire
  and the consumer's retain prevents a slot being overwritten while it's being
  read. Geometry lives in the segment and is gated by a magic value written last,
  so a consumer can attach knowing only the name.

- **Socket bridge** (`include/ipc/transport.hpp`) — mirrors topics over any
  connected byte stream (Unix-socket helpers provided; TCP-swappable). Wire
  frame: `[u8 kind][u32 name_len][name][u32 payload_len][payload]`, native-endian
  (same-host assumption). `wire_codec<T>` handles trivially-copyable types as raw
  bytes, with specializations for `std::string` and `std::vector<T>`. A
  thread-local marker suppresses echoing a topic straight back to the peer it
  came from.

## Layout

```
include/broker.hpp        typed pub/sub core
include/threadpool.hpp    fixed-size worker pool
include/node.hpp          Node facade (pub/sub + frames + bridge + kv)
include/ipc/              FrameHandle, shm ring C++ wrappers, socket bridge
include/shm_ring.h        C shm ring interface
src/shm_ring.c            C shm ring implementation
examples/basic.cpp        sensor -> vision -> control -> actuator pipeline
tests/                    per-layer suites + shm ring stress/bench
```
