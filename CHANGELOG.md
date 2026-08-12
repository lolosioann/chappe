# Changelog

## Unreleased

### Messaging
- **Subscribe before connect** — handlers may be registered before `connect()`,
  which flushes their subscriptions to the daemon. Both clients.
- **Unsubscribe** — `unsubscribe<T>()` / `unsubscribe_pattern()` in C++,
  `unsubscribe()` / `unsubscribe_pattern()` in Python. Drops every local handler
  for the topic and the daemon-side subscription with it (the wire subscription
  is per-topic, so there's no per-handler granularity).
- **Clear a retained value** — `clear_retained<T>()` / `clear_retained(topic)`,
  sent as a zero-length retained publish (MQTT convention). Current subscribers
  still see that empty publish; the default POD codec rejects it, so typed
  handlers skip it.

### Key/value store
- **Delete** — `Node::del(key)` in C++, `Node.delete(key)` in Python. The daemon
  erases the key and pushes the deletion to every watcher; a watcher drops the
  cached value but keeps watching, so its next `get` reads "absent" locally.
- **Key TTL** — `set(key, val, ttl)` / `set(key, value, ttl_ms=...)`. The daemon
  deletes the key once it is due and pushes the deletion like a `del`, so "key
  present" means "the writer was alive within the last TTL". A plain `set`
  clears an existing TTL (Redis semantics). A 100 ms sweep thread does the
  expiring — required, not an optimisation: a warm client answers `get` from its
  cache, so a key that expired lazily would never be noticed. `info()` gained a
  `kv_expiring` count.
- **Atomic ops** — `incr(key, by)` and `setnx(key, val, ttl)` in both clients,
  executed inside the daemon's store lock, so concurrent nodes can't lose an
  increment or both win a lock the way a get/modify/set pair would. `incr` fixes
  the counter representation at a native-endian `int64` in exactly 8 bytes —
  readable as an ordinary value with `get<int64_t>()` / `struct.unpack("=q")` —
  counts an absent key as 0, and reports a key holding anything else as a type
  error rather than coercing it. `setnx` takes a TTL because that is what stops
  a holder that dies without releasing from locking every other node out. A TTL
  outside what the wire's u32 milliseconds can hold is clamped rather than cast:
  truncating to 0 would have read as "no TTL" and stored the key permanently.

### Resilience & hardening
- **Slow-consumer bound** — accepted client sockets get a 2 s `SO_SNDTIMEO` and
  a failed write drops that client. A consumer that stops reading no longer
  stalls the whole daemon with the KV lock held. Bounded, not solved: a
  slow-but-draining consumer still delays a `set` fan-out — the real fix is a
  per-client outbox (see `TODO.md`).
- **Reader threads reaped on accept** — finished client readers no longer
  accumulate until shutdown, so a flapping client doesn't leak one per
  reconnect.
- **Empty routing entries erased** — subscriber, pattern and watcher map entries
  are dropped when their last client leaves, instead of lingering for the
  daemon's life and being walked on every publish. `info()` gained a
  `kv_watchers` count, which makes the watcher half of that observable.
- **Access control** — the listen socket is bound `0600` (umask around the bind,
  so it never exists world-connectable for a window), and every accepted
  connection is checked against `SO_PEERCRED` before it is read from; the
  credentials come from the kernel, so a client can't forge them, and a
  `getsockopt` failure denies. `BrokerServer(path, {uids...})` replaces the
  default same-uid rule with an explicit allow-list — the complete set, not an
  addition — and opens the socket mode so those uids can reach the check.
- **KV cache dropped on disconnect, not on reconnect** — a client used to keep
  serving cached values for the whole reconnect window, so a TTL'd key read
  "still there" long after the daemon had expired it. The cache is cleared when
  the link drops; a `get` in the window reports the key absent, which is the
  safe answer for a heartbeat or a lock.
- **Reconnect backs off after an instant hang-up** — a link that dropped as soon
  as it opened (what an access-control denial looks like) used to spin
  connect/EOF as fast as the kernel allowed: ~2.3 CPU-seconds per 3 s wall in
  C++, 1.8 in Python, all of it also hitting the daemon's accept thread. Both
  clients now back off 10 ms doubling to 1 s; measured at 0.01 s / 0.05 s.
- **Bounded wait on every round-trip** — `get`, `sync`, `info`, `incr` and
  `setnx` all time out after 5 s instead of blocking the caller forever against
  a daemon that stops answering (or never knew the verb). Each used to hand-roll
  its own request/reply sequence, and only two of the five bounded the wait;
  they now share one `request()` / `_request()` helper in each client, so the
  policy is set in one place.
- **`drain()` actually drains** — `ThreadPool::drain()` (and so `Node::drain()`)
  queued a sentinel task and returned when it ran, which with more than one
  worker only proved *a* worker had reached the back of the queue, not that the
  others had finished what they were running. It now waits on the in-flight
  count as well. Both existing callers happened to guard with their own wait
  first, so nothing was failing because of it.
- **Socket file removed on shutdown** — the daemon unlinks its listen address
  when it stops, instead of leaving it in `/tmp` (test runs accumulated one per
  daemon).

### Clients & tooling
- **Redis bridge** — `python/redis_bridge.py` mirrors the bus into Redis for
  monitoring apps that already speak it: named kv keys go out to `broker:<key>`
  byte for byte (deletes and TTL expiry included), topics go both ways. A
  separate process built on the existing Python client, so the daemon gains no
  dependency and no code. Values are not translated — an `incr` counter stays 8
  native-endian bytes rather than becoming a Redis decimal integer. Frames never
  cross: a `FrameHandle` names local shared memory.
- **Python handler pool** — `Node(name, threads=N)` runs handlers on N workers
  instead of the reader thread; the default (`threads=0`) keeps them inline.
- **CI** — GitHub Actions workflow running the C++ suites and both Python
  self-checks.

### Transport
- **Cross-device links** — `broker_link` joins two devices' buses over TCP,
  forwarding chosen topics (wildcards) and keys (exact names) both ways, and
  seeding a key that already had a value when the link comes up. Every device
  keeps its own daemon on its own unix socket, so local traffic never crosses
  the network, frames stay where they can be mapped, a partition leaves each
  device working, and the daemon keeps its `SO_PEERCRED` uid gate. The link is a
  protocol relay rather than a client: it speaks frames straight to the local
  daemon, which is how it acts on kv pushes as they arrive instead of polling.
  No auth on the wire by design — put links on a private network.
- **TCP primitives** — `tcp_connect` / `tcp_listen` / `tcp_accept` / `tcp_tune`
  in `ipc/transport.hpp`, carrying the same frames as the unix path. Nothing in
  the daemon or `Node` calls them: they exist for the cross-device link, so the
  daemon keeps its unix socket and its `SO_PEERCRED` uid gate. `tcp_listen`
  requires an explicit bind address — there is no auth over TCP, so that choice
  is the access control, and links belong on a private network.

### Wire protocol
- Adds `MSG_KV_DEL = 11`, `MSG_KV_SETEX = 12`, `MSG_KV_INCR = 13`,
  `MSG_KV_SETNX = 14` and `MSG_KV_RESULT = 15`; nothing existing changed. Old
  clients are unaffected, but an old daemon can't serve a new client's `del()`,
  TTL or atomic ops.
- `MSG_KV_RESULT` answers `incr`/`setnx` rather than reusing `MSG_KV_REPLY`,
  because a `KV_REPLY` makes the client cache the value and mark the key
  watched. The daemon registers no watcher for these, so that entry would never
  be refreshed and every later `get` would serve it.

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
