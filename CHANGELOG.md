# Changelog

## v1.0.0

First tagged release. A typed, same-host IPC message bus: a central daemon
routes between many client nodes, with pub/sub, a get/set store, and zero-copy
shared-memory frames — all under one `Node` interface, in C++ and Python.

### Messaging
- **Central broker daemon** (`broker_daemon`) — payload-agnostic string-topic
  router; clients connect over a Unix socket (`$BROKER_SOCKET` or
  `/tmp/broker.sock`).
- **Typed pub/sub** — topics are C++ types (`MAKE_TOPIC`); dispatch resolved at
  compile time. Publisher noLocal (no self-echo).
- **Retained messages** — `publish(msg, retain=true)` stores a last-value the
  daemon replays to late subscribers; plain publishes stay fire-and-forget.
- **Wildcard subscriptions** — `subscribe_pattern()` over `/`-separated levels,
  `+` (one level) and `*` (the rest); matched daemon-side for routing and
  client-side for dispatch.

### Key/value store
- Authoritative store in the daemon with a **read-through cache** on each client:
  the first `get` round-trips and starts watching; later reads are local, kept
  fresh by daemon pushes.

### Frames (zero-copy)
- Single-producer / multi-consumer **latest-frame** shared-memory ring
  (`shm_ring`, C). Pixels move producer→consumer directly; only the small
  `FrameHandle` rides the broker. `create/attach_frame_ring`, `publish_frame`,
  `subscribe_frame`, `frame_drops()`.

### Resilience & hardening
- **Reconnect + resubscribe** — a dropped client reconnects (exponential
  backoff) and re-sends its subscriptions; get/sync don't block on a lost
  request; KV cache invalidated on reconnect.
- **Frame-size cap** (`MAX_FRAME_BYTES`, 64 MB) — an oversized/garbage length
  drops that one connection instead of crashing the daemon.
- **Frame abandon path** — if a `publish_frame` writer throws, the slot is
  returned to the free pool instead of leaking as `WRITING`.
- **Buffered frame reader** — ~1 recv/frame instead of 5; ~2.4× pub/sub
  throughput.
- **Introspection** — `info()` returns a daemon status snapshot (clients,
  per-topic subscriber counts, retained/kv totals).

### Clients & tooling
- **Python client** (`python/broker.py`) — stdlib-only for pub/sub + get/set;
  frames via a `ctypes` binding to the same C ring (`make libshm_ring`).
  Interoperates with C++ nodes over the identical wire protocol.
- Examples (`make examples`), per-layer test suites, shm-ring stress/bench, and
  broker-layer + cross-broker benchmarks (`BENCHMARKS.md`).

### Known limitations (see `TODO.md`)
- Same-host only (Unix socket, native-endian); no TCP/cross-host yet.
- No auth on the socket — any local process can connect.
- A reconnected node resumes live delivery but does not replay messages missed
  while disconnected.
