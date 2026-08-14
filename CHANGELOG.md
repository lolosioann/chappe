# Changelog

## Unreleased

### One-line install, and a systemd unit

```sh
curl -sSL https://raw.githubusercontent.com/lolosioann/chappe/main/scripts/install.sh | sh
```

`scripts/install.sh` resolves the latest release, builds it, installs the
binaries and the whole C++ side, and registers a systemd unit so the daemon
comes up at boot. `PREFIX`, `CHAPPE_REF`, `CHAPPE_USER` and `CHAPPE_SOCKET`
override the defaults; `--no-systemd` installs the binaries and registers
nothing. sudo is used only when the prefix actually needs it.

Two details in the unit that are easy to get wrong and expensive to debug:

- **`User=` defaults to whoever ran the script** (`$SUDO_USER` under sudo), not
  root. The socket is `0600` and gated on `SO_PEERCRED`, so a daemon running as
  root is one no ordinary node can talk to.
- **`PrivateTmp=no`**, explicitly. A private `/tmp` would put the daemon's
  socket in a namespace no client can see, and the socket is the entire
  interface.

The Python client stays on pip and is not touched by the script.

### Python publish/set take values, not just bytes

`publish("number", 17)` used to fail with "object of type 'int' has no len()".
Now str, int, float, bool, None, list and dict are serialized as JSON behind a
four-byte magic, and arrive as the same type on any Python subscriber or `get()`.

This is additive by construction: **bytes still go out byte for byte**, so every
C++ topic, every `wire_codec<T>`, the frame handles and the shm path are exactly
as they were. Only values that previously raised gained a representation. The
magic is what makes decoding safe to do unconditionally — a C++ POD almost never
starts with those four bytes, and if one does, the JSON behind it fails to parse
and the raw bytes are handed back untouched.

A C++ node can still read these. `wire_codec<T>` is a compile-time POD mapping
with nowhere to put a dict, so it cannot decode one into a type — but the daemon
never inspects a payload, so the bytes arrive intact. `chappe::json_payload()`
(new, in `ipc/transport.hpp`) says whether a payload carries the envelope and
hands back the JSON body as a `string_view`, to feed to whatever parser you
already use; chappe does not ship one, and recognising an envelope is not
parsing. It exists so callers stop hardcoding the magic and a `substr(4)`, which
is how that silently breaks later. Cross-language topics are still better served
by agreeing on a POD and having Python `struct.pack` it.

JSON, not pickle, and deliberately: `chappe_link` forwards payloads between
devices over TCP with no auth, so a pickle payload would be remote code
execution on the far device. JSON's edges are the price — tuples come back as
lists, dict keys come back as strings, and `bytes` cannot nest inside a list or
dict. Unsupported values raise `TypeError` naming the type rather than being
quietly mangled.

Two things fell out of building it:

- **`Node(name, decode=False)`** hands handlers and `get()` the exact bytes off
  the wire. Anything that forwards payloads onward wants this, and the Redis
  bridge now uses it — it had started receiving dicts from `get()`, which
  redis-py cannot store (`DataError`), so mirrored dict/list values would have
  silently stopped reaching Redis.
- Decoding runs on the reader thread, **outside** the guard that keeps a bad
  handler from killing it, and payloads arrive from any local process or across
  a link. So a failed decode falls back to raw bytes on *any* exception, not
  just `ValueError` — JSON nested deeply enough raises `RecursionError`, which
  would otherwise have made the node silently go deaf.

Counters are the one sharp edge: `incr` fixes its representation at a
native-endian int64, so seed one with `set(k, struct.pack("=q", n))`. `set(k, n)`
stores the serialized form and `incr` correctly rejects it.

## v3.0.0

Major for one reason: links now open with a handshake, so a v2 link and a v3
link will not talk to each other. Upgrade both ends of a link together. Nothing
else breaks — the client/daemon protocol, the C++ API and the Python API are all
unchanged from v2, so a node only needs a rebuild.

### Frames across devices, as video

`chappe.gst_bridge` carries a frame topic between devices as H.264 over RTP. It
is a separate process built on the existing Python client, so the daemon gains
no dependency and no code — the same shape as the Redis bridge.

The point is what it *doesn't* do: it never forwards a `FrameHandle`. A handle
names shared memory on the host that published it, so relaying one only points
the far side at a segment that is missing, or at a different local ring with the
same name. The bridge terminates the frame topic on each device instead —
encode from the local ring, decode into a ring the far side owns — so nodes on
both devices see an ordinary local frame topic and the zero-copy path inside
each device is untouched. `chappe_link` still refuses frame topics; the docs now
point here.

This is deliberately not the zero-copy path across the wire: H.264 is lossy and
encode/network/decode costs latency. It suits an operator view or a monitoring
app, not a far side that needs real sensor bytes.

Two things it refuses rather than fudges. `FrameHandle` carries no pixel format,
so `--format` is configured and both ends must agree. And GStreamer pads rows to
4 bytes where a ring is flat, so a width whose rows aren't a multiple of 4 is
rejected at startup — left alone that combination doesn't error, it just
forwards nothing, which is a far worse way to learn about it. `--pipeline`
replaces the codec/transport half for anything else; the ring end stays ours.

Needs GStreamer and PyGObject, which are system packages — so there is no
`chappe[gstreamer]` extra, which would install something that still couldn't
encode. `chappe-gst-bridge` is on PATH after a pip install; the self-check skips
cleanly when the plugins are absent, and CI installs them so it doesn't.

### Links refuse a peer with a different ABI — breaking between link versions

`wire_codec<T>` puts a struct's raw bytes on the wire. Same host, that is free.
Between two devices it is an assumption about byte order, padding, type sizes
and char signedness — and when it is wrong nothing fails: the bytes arrive, the
length matches, and they decode into plausible garbage. A control system acting
on quietly wrong numbers is worse than one with a dead link.

So a link now opens by sending an `abi_fingerprint()` — byte order read from a
known value rather than a macro, `sizeof` of pointer/long/long double, the
padding of a probe struct, and whether `char` is signed (the x86-vs-ARM trap:
same bytes, different values). It describes the *ABI*, not the build; matching
compiler versions are neither necessary nor sufficient.

The hello is the first frame on the wire and the peer checks it before its loop
body, so there is no window in which an incompatible peer's data reaches the
daemon. A peer that opens with anything else is refused the same way, which is
also what an older link looks like. `--allow-abi-mismatch` overrides it for
links carrying only strings and bytes, which survive the difference.

Breaking for links specifically: a 2.0.0 link never sends a hello, so a link at
this version will refuse it. Upgrade both ends of a link together. Nothing about
the client/daemon protocol changed — this frame only ever travels link-to-link,
which is why it sits at kind 200, clear of the `MSG_` range.

`Link::error()` now reports why a link stopped, and `chappe_link` prints it
instead of a bare "peer link lost".

## v2.0.0

Major because the rename below breaks every include, import and environment
variable at once. The wire protocol itself is untouched — frame kinds and
framing are identical to v1, so a v1 and a v2 process still understand each
other *if you point them at the same socket*. The default address moved, so
during a mixed rollout name the path explicitly rather than relying on it.

### Renamed to chappe — breaking

Named for Claude Chappe, whose optical telegraph (1792) was a chain of relay
towers each reading its neighbour and passing the message on — which is what a
`chappe_daemon` per device joined by `chappe_link` actually is.

- **Everything public moved into `namespace chappe`.** `ipc::` is gone, and so
  are the global `Node`, `Topic`, `msg_t` and `ThreadPool` — a header installed
  into a shared prefix has no business claiming names that generic. The classes
  shed their now-redundant prefix: `ipc::BrokerServer` → `chappe::Server`,
  `ipc::BrokerLink` → `chappe::Link`.
- **Files** — `broker.hpp` → `chappe.hpp`, `broker_server.hpp` → `server.hpp`,
  binaries `broker_daemon`/`broker_link` → `chappe_daemon`/`chappe_link`,
  headers install to `$PREFIX/include/chappe/`.
- **Python is a package** — `python/broker.py` → `python/chappe/__init__.py`,
  with `shm_ring` and `redis_bridge` inside it. `import broker` → `import
  chappe`; the bridge runs as `python3 -m chappe.redis_bridge`. PyPI already has
  a `broker` project that installs a top-level `broker` module, so the old name
  was a collision waiting to happen.
- **Environment and defaults** — `$BROKER_SOCKET` → `$CHAPPE_SOCKET`,
  `$BROKER_LIB` → `$CHAPPE_LIB`, `/tmp/broker.sock` → `/tmp/chappe.sock`, and
  the Redis bridge mirrors to `chappe:<key>` instead of `broker:<key>`. No
  fallback to the old names: a silent fallback would leave two daemons on two
  sockets looking like one working system. Update `--prefix` on any monitoring
  app reading the old keys.
- `default_broker_addr()` → `default_addr()` in both clients.

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
  `getsockopt` failure denies. `Server(path, {uids...})` replaces the
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
- **Cross-device links** — `chappe_link` joins two devices' buses over TCP,
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

### Packaging & distribution
- **Installable with pip** — `pyproject.toml` makes the Python client a real
  package. The wheel compiles `src/shm_ring.c` into it (as `chappe._shm_ring`,
  loaded by `ctypes`, never imported — it has no `PyInit`), so frames work off a
  pip install alone, with no `make` step and no `$CHAPPE_LIB`. `chappe[redis]`
  pulls `redis-py`; the client itself stays stdlib-only. The bridge installs as
  a `chappe-redis-bridge` command.
- **pkg-config and CMake** — `make install` now also writes `chappe.pc` and a
  `chappeConfig.cmake` exposing `chappe::chappe` (headers, `libshm_ring`,
  threads, `rt`, C++17), so consumers stop hand-writing `-I` and `-l` flags.
  Both are generated during the install recipe rather than as build targets:
  their contents depend on `$(PREFIX)`, which make cannot take a dependency on,
  so a file target would quietly go stale the moment you installed to a second
  prefix.
- **One version, three literals** — `chappe::VERSION`, the Makefile's `VERSION`
  (which stamps both config files) and Python's `__version__`. Generating them
  from one source would put a build step in front of a header-only library, so
  instead `test_chappe.py` fails if they ever disagree, and the release workflow
  refuses a tag that doesn't match the tree.
- **CI builds the packages on every push**, installs the sdist with
  `--no-binary` so the ring is compiled the way a source install compiles it,
  and builds a C++ consumer against an installed prefix through *both* the
  pkg-config and `find_package` paths — a release should never be the first time
  any of that runs. Tagging `v*` attaches the sdist and wheel to a GitHub
  release.

## v1.0.0

First tagged release. A typed, same-host IPC message bus: a central daemon
routes between many client nodes, with pub/sub, a get/set store, and zero-copy
shared-memory frames — all under one `Node` interface, in C++ and Python.

### Messaging
- **Central broker daemon** (`chappe_daemon`) — payload-agnostic string-topic
  router; clients connect over a Unix socket (`$CHAPPE_SOCKET` or
  `/tmp/chappe.sock`).
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
