# Changelog

## v1.0.0

First release. A typed, same-host IPC message bus: a central daemon routes
between many client nodes, with pub/sub, a get/set store, and zero-copy
shared-memory frames — all under one `Node` interface, in C++ and Python, plus
bridges out to other devices, to Redis and to video.

### Messaging

- **Central daemon** (`chappe_daemon`) — a payload-agnostic string-topic router.
  Clients connect over a unix socket (`$CHAPPE_SOCKET`, else `/tmp/chappe.sock`).
- **Typed pub/sub** — topics are C++ *types* (`MAKE_TOPIC`), so dispatch is
  resolved at compile time while the daemon stays a dumb string router.
  Publisher noLocal: a node never receives its own publish.
- **Retained messages** — `publish(msg, retain=true)` keeps a last-value the
  daemon replays to late subscribers, which is the real fix for the
  publish-before-subscribe race on state topics. `clear_retained<T>()` drops it,
  sent as a zero-length retained publish (the MQTT convention).
- **Wildcard subscriptions** — `subscribe_pattern()` over `/`-separated levels:
  `+` matches one level, `*` the rest. Matched daemon-side for routing and
  client-side for dispatch. Pattern handlers are untyped, since a wildcard spans
  message types.
- **Subscribe before connect** — handlers may be registered before `connect()`,
  which flushes them. `unsubscribe<T>()` / `unsubscribe_pattern()` drop every
  local handler for a topic and the daemon-side subscription with it; the wire
  subscription is per-topic, so there is no per-handler granularity.
- **`sync()`** — a round-trip barrier, for ordering setup deterministically.

### Key/value store

- Authoritative store in the daemon with a **read-through cache** on each
  client: the first `get` round-trips and starts watching the key, later reads
  are local and kept fresh by daemon pushes.
- **Delete** — `del(key)` in C++, `delete(key)` in Python. The daemon erases it
  and pushes the deletion to every watcher, which drops the cached value but
  keeps watching.
- **Key TTL** — `set(key, val, ttl)`. The daemon deletes the key when due and
  pushes the deletion like a `del`, so "key present" means "the writer was alive
  within the last TTL". A plain `set` clears an existing TTL (Redis semantics).
  A 100 ms sweep thread does the expiring — required, not an optimisation: a
  warm client answers `get` from its cache, so a key expired lazily would never
  be noticed.
- **Atomic ops** — `incr(key, by)` and `setnx(key, val, ttl)` run inside the
  daemon's store lock, so concurrent nodes can't lose an increment or both win a
  lock the way a get/modify/set pair would. `incr` fixes the counter at a
  native-endian `int64` in exactly 8 bytes — readable as an ordinary value with
  `get<int64_t>()` / `struct.unpack("=q")` — counts an absent key as 0, and
  reports a key holding anything else as a type error rather than coercing it.
  `setnx` takes a TTL because that is what stops a holder that dies without
  releasing from locking every other node out.
- **Introspection** — `info()` returns a daemon status snapshot: connected
  clients, per-topic subscriber counts, pattern/retained/kv/watcher totals.

### Frames (zero-copy)

- Single-producer / multi-consumer **latest-frame** shared-memory ring
  (`shm_ring`, C). Pixels move producer→consumer directly through POSIX shared
  memory; only the small `FrameHandle` rides the broker.
- `create/attach_frame_ring`, `publish_frame`, `subscribe_frame`,
  `frame_drops()`. The ring is keyed by topic name, so producer and consumer
  agree on the segment with no shared configuration.
- **Frame-size cap** (`MAX_FRAME_BYTES`, 64 MB) — an oversized or garbage length
  drops that one connection instead of letting a reader `resize()` to gigabytes
  and take the daemon down.
- **Producer abandon path** — `shm_ring_abandon_slot` returns an
  acquired-but-unpublished slot to the free pool, so a writer that throws does
  not leak a slot as `WRITING`.

### Cross-device

- **`chappe_link`** joins two devices' buses over TCP, forwarding chosen topics
  (wildcards) and keys (exact names) both ways, and seeding a key that already
  had a value. Every device keeps its own daemon on its own unix socket, so
  local traffic never crosses the network, frames stay where they can be mapped,
  a partition leaves each device working, and the daemon keeps its unix socket
  and its `SO_PEERCRED` gate. The link is a protocol relay rather than a client:
  it speaks frames straight to the local daemon, which is how it acts on kv
  pushes as they arrive instead of polling.
- **Links refuse a peer with a different ABI.** `wire_codec<T>` puts a struct's
  raw bytes on the wire; between two devices that assumes matching byte order,
  padding, type sizes and char signedness, and when it is wrong nothing fails —
  the bytes arrive, the length matches, and they decode into plausible garbage.
  So a link opens with an `abi_fingerprint()` and refuses a peer that disagrees,
  before any data reaches the daemon. `--allow-abi-mismatch` overrides it for
  links carrying only strings and bytes.
- **No auth on the wire**, deliberately: there is no `SO_PEERCRED` equivalent
  for TCP and a plaintext token would look like security without being it. So
  `tcp_listen` requires an explicit bind address, and links belong on a private
  network — WireGuard, an SSH tunnel, a VLAN.
- Federated kv is eventually consistent with no cross-device ordering, so keep
  one owning device per key.

### Resilience & hardening

- **Reconnect + resubscribe** — a dropped client reconnects with exponential
  backoff and re-sends its subscriptions. Publishes during the gap are dropped,
  and the kv cache is invalidated the moment the link drops, so a `get` in the
  window reports the key absent rather than a value that may already be gone —
  the safe answer for a heartbeat or a lock. A reconnected node resumes live
  delivery and does not replay what it missed.
- **Access control** — the listen socket is bound `0600` (umask around the bind,
  so it never exists world-connectable) and every accepted connection is checked
  against `SO_PEERCRED` before it is read from; the credentials come from the
  kernel, so a client cannot forge them, and a `getsockopt` failure denies.
  `Server(path, {uids...})` swaps the default same-uid rule for an explicit
  allow-list — the complete set, not an addition.
- **Slow-consumer bound** — client sockets get a 2 s `SO_SNDTIMEO` and a failed
  write drops that client, so a consumer that stops reading cannot stall the
  daemon with the kv lock held. Bounded, not solved: a slow-but-draining
  consumer still delays a fan-out.
- **Reader threads reaped on accept**, so a flapping client does not leak one
  per reconnect; **empty routing entries erased**, so subscriber, pattern and
  watcher maps do not accumulate dead entries walked on every publish.
- **Bounded wait on every round-trip** — `get`, `sync`, `info`, `incr` and
  `setnx` time out rather than blocking forever against a daemon that stops
  answering (or never knew the verb).
- **Backoff after an instant hang-up** — a link that drops as soon as it opens
  (what an access-control denial looks like) backs off instead of spinning
  connect/EOF as fast as the kernel allows.
- **Socket file removed on shutdown**, rather than left behind in `/tmp`.

### Clients & tooling

- **C++ client** — header-only apart from the C shared-memory ring.
- **Python client** (`chappe`) — stdlib-only for pub/sub and get/set; frames via
  a `ctypes` binding to the same C ring, so a Python node and a C++ node can
  share one. Interoperates over the identical wire protocol.
  - `publish`/`set` take ordinary values — str, int, float, bool, None, list,
    dict — serialized as JSON behind a short magic and returned as the same type
    to another Python node. `bytes` are sent untouched, which is what keeps C++
    topics, frame handles and the POD codecs working unchanged. JSON rather than
    pickle because a link forwards payloads between devices with no auth, and
    unpickling that would be remote code execution.
  - `chappe::json_payload()` lets a C++ node recognise one of those payloads and
    get the JSON body, to hand to whatever parser it already uses.
  - `Node(name, threads=N)` runs handlers on a pool; `Node(name, decode=False)`
    hands back raw bytes, for code that forwards payloads verbatim.
- **Redis bridge** (`chappe.redis_bridge`) — mirrors named kv keys out to Redis
  and topics both ways, for monitoring apps that already speak Redis. A separate
  process built on the ordinary client, so the daemon gains no dependency.
- **GStreamer frame bridge** (`chappe.gst_bridge`) — carries a frame topic
  between devices as H.264 over RTP, terminating the topic on each device so
  each keeps its own zero-copy ring. Not the zero-copy path across the wire:
  lossy and higher latency, for an operator view rather than remote vision work.
- **Examples**, per-layer test suites, shm-ring stress/bench, and broker-layer
  and cross-broker benchmarks (`BENCHMARKS.md`).

### Install & packaging

- **One-line install** — `scripts/install.sh` builds the latest release and
  installs the binaries, headers, `libshm_ring.so`, a pkg-config file and a
  CMake package config, then registers a systemd unit so the daemon comes up at
  boot. `User=` defaults to whoever ran the script rather than root, because the
  socket is `0600` and `SO_PEERCRED`-gated; `PrivateTmp=no` is explicit, because
  a private `/tmp` would hide the socket from every client.
- **C++ consumers** get `pkg-config --cflags --libs chappe` or
  `find_package(chappe)` exposing `chappe::chappe`.
- **Python** installs with pip; the wheel compiles the C ring into the package,
  so frames work off a pip install alone.
- **CI** builds and exercises all of it on every push — the C++ suites, the
  Python self-checks, the packages, a consumer built against an installed
  prefix, and the installer under `/bin/sh`.

### Known limitations (see `TODO.md`)

- Thread-per-client with global locks in the daemon, and one lock held across kv
  pushes. Fine for tens of nodes; the daemon is wakeup-bound rather than
  lock-bound (see `BENCHMARKS.md`), so a per-client outbox and update conflation
  are the levers if that ever matters.
- A reconnected node resumes live delivery but does not replay what it missed.
- Raw struct payloads assume a matching ABI. Same host that is free; across a
  link it is checked and refused rather than assumed.
