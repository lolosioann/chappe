# TODO

Deferred work, roughly by priority. These are v1 scope cuts, not bugs — the
current build is coherent and tested. Add each only when real use asks for it.

## Resilience (do this first if anything stays running)

- [x] **Client reconnect + resubscribe.** Done: on a dropped connection the
      reader thread (C++ `Node` and Python `Node`) reconnects to the same
      address with exponential backoff and re-sends every `SUBSCRIBE`; publishes
      during the gap are dropped, and get/sync don't block on a lost request.
      The KV cache is invalidated the moment the link drops, so a get() during
      the outage reports the key absent instead of a value that may already be
      gone; the first get() after reconnecting cold-fetches and re-watches.
      A reconnected node resumes live delivery and does not replay what it
      missed — deliberately, see *Retained delivery* under "Borrow from Redis".
- [x] **Startup ordering.** ~~A publish sent before its subscriber registers is
      dropped.~~ Done: opt-in retained messages — `publish(msg, retain=true)`
      stores a last-value the daemon replays on subscribe (MQTT-style). State
      topics use it; event/data streams stay non-retained. `sync()` remains for
      deterministic test ordering.

## Transport reach

Cross-device is a *daemon per device*: each keeps its local unix socket (so
frames stay zero-copy, latency stays local, and a partition leaves each device's
own bus working), and devices are joined by out-of-process links. The daemon
itself never learns TCP — which is also why its `SO_PEERCRED` uid gate survives
intact.

- [x] **Redis bridge** — done: `python/redis_bridge.py`, an ordinary client that
      mirrors named kv keys out to Redis and topics both ways, for monitoring
      apps that speak Redis. No daemon changes, no dependency added to the core.
- [x] **TCP primitives** — done: `tcp_connect`/`tcp_listen`/`tcp_accept`/
      `tcp_tune` in `transport.hpp`, with NODELAY (Nagle would sit on frames this
      small), REUSEADDR, and keepalive tuned to ~19 s so a peer that loses power
      is noticed. `tcp_listen` takes a bind address with no all-interfaces
      default — with no auth on the wire, that choice *is* the access control.
      Used only by the link below; `Node` and `Server` still call the unix
      ones.
- [x] **`chappe_link`** — done: `include/link.hpp` + `src/chappe_link.cpp`, one
      process per peer, forwarding topics (wildcards) and keys (exact names)
      both ways. It speaks frames straight to the local daemon rather than going
      through `Node`, which is what lets it act on KV_UPDATE/KV_DEL as they
      arrive instead of polling a cache. Publishes need no loop guard —
      route_publish is noLocal — but kv does, since the daemon pushes an update
      back to whoever wrote it. Federated kv is eventually consistent with no
      cross-device ordering, so keep one owning device per key.
- [x] **Security for the link.** Settled: no auth over TCP, deliberately. There
      is no `SO_PEERCRED` equivalent and a plaintext token would look like
      security without being it, so `tcp_listen` requires an explicit bind
      address and the docs say to run links over WireGuard/SSH/a private VLAN.
- [ ] **Payload portability.** Bigger than the endianness note it replaces:
      `wire_codec<T>` ships raw struct bytes, so layout, padding *and* byte order
      are all assumed identical across the link. Fine between identical builds;
      document it, and serialize explicitly for anything else.
- [ ] **Frames across devices** — a separate feature, not a link setting.
      Forwarding pixels over TCP defeats the zero-copy design, so decide what it
      should actually be before building it. Until then, note the sharp edge: a
      forwarded FrameHandle points the far side at a segment that is missing, or
      at a *different* local ring of the same name. The link cannot detect a
      frame topic — on the wire a handle is just bytes — so this is a
      configuration hazard the docs warn about rather than something it blocks.

## Routing features

- [x] **Topic wildcards.** Done: bash-path-like over '/'-separated levels —
      `cam/+` (one level), `cam/*` (the rest). `subscribe_pattern()` in both
      clients; the daemon matches patterns on publish, clients on dispatch.
      Pattern handlers are untyped (topic + raw bytes). No retained replay for
      patterns, and no wildcard frame subscriptions — noted as follow-ups if
      needed. (For hierarchy, name topics with '/', e.g. `cam/front`.)
- [x] **Access control.** Done: the listen socket is bound `0600` (umask around
      the bind, so it never exists world-connectable), and every accepted
      connection is checked against `SO_PEERCRED` before it is read from.
      `Server(path, {uids...})` swaps the default same-uid rule for an
      explicit allow-list (complete set, not additive) and opens the socket mode
      so those uids can reach the check. Same-uid is the boundary: a process
      running as you can ptrace you anyway. No per-topic/per-key ACLs — add if a
      real multi-tenant case turns up.

## Borrow from Redis

Features a mature store has that we don't — but only the ones that fit a
same-host real-time bus. Ordered by value. The rest (persistence, replication,
Sentinel/Cluster, the data-type zoo, ACL modules, search/JSON/timeseries) we
deliberately **don't** rebuild: at that point, run Redis alongside for durable/
structured/cross-host state and keep this on the hot path.

- [x] **Retained delivery.** Done, and deliberately stopping here: opt-in
      last-value retention (`publish(msg, retain=true)`) replays the current
      value on subscribe. **Replay-N-back is explicitly out of scope** — this bus
      is last-value everywhere by design (KV holds the current value, retained
      pub/sub the current message, the frame ring the current frame), and a
      reconnecting node wants current state, not a backlog of stale ones. If you
      ever need the missed history, that is a log, and Redis Streams or a file is
      the right thing to run alongside.
- [x] **Atomic KV ops** — done: `incr` and `setnx` in both clients, executed
      inside the daemon's store lock so concurrent nodes can't lose an increment
      or both win a lock. `incr` fixes one representation (native-endian `int64`,
      exactly 8 bytes — what `get<int64_t>` and `struct "=q"` read); anything
      else under the key is a type error, not a coercion. `setnx` takes a TTL so
      a holder that dies can't lock everyone out. No general compare-and-swap
      yet — `setnx` covers the lock case, add CAS if a read-modify-write on an
      arbitrary value turns up.
- [x] **Key TTL / expiry** (Redis `EXPIRE`). Done: `set(key, val, ttl)` /
      `set(key, value, ttl_ms=)`, and a plain `set` clears the TTL. A 100 ms
      sweep thread on the daemon expires keys and pushes the deletion to
      watchers. The sweep is not an optimisation here: clients serve `get` from
      their cache once warm, so a lazily-expired key nothing round-trips for
      would never be noticed. No `EXPIRE`/`TTL`/`PERSIST` on an existing key —
      the TTL rides the write; add them if resetting one without rewriting the
      value turns up.
- [x] **Introspection** (Redis `INFO`). Done: `node.info()` returns a daemon
      status snapshot — connected clients, per-topic subscriber counts, pattern/
      retained/kv totals. C++ and Python. (Message/drop counters not tracked
      yet — add if useful.)

Already tracked elsewhere as Redis analogues: pattern pub/sub = `PSUBSCRIBE`
(see *Topic wildcards*), socket auth = Redis `ACL` (see *Access control*),
durability/HA (see *Resilience*, but mostly out of scope per above).

## Hardening

- [x] **Frame-size cap.** Done: `MAX_FRAME_BYTES` (64 MB) in `transport.hpp`;
      an oversized/garbage length drops that one connection instead of letting
      the reader `resize()` to gigabytes and take the daemon down with a
      `bad_alloc`.
- [x] **Access control** — done; see *Routing features* above.

## Daemon ceilings (`server.hpp`, marked `ponytail:`)

Known limits, not planned work — throughput is deliberately off this list. Each
is marked `ponytail:` at the line where it bites, which is the version that
stays true; this is just the index.

- **Thread-per-client + global locks.** Fine for tens of nodes. Epoll + sharded
  locks is the upgrade path if client count ever demands it.
- **Slow consumers are bounded, not solved.** A 2 s `SO_SNDTIMEO` plus
  drop-on-write-failure stops one wedged consumer from stalling the daemon, but
  a slow-but-draining one still delays a `set` fan-out. The upgrade path is a
  per-client outbox with a bounded queue and an explicit drop policy — and the
  measurement saying *why* (wakeup-bound, not lock-bound) is in `BENCHMARKS.md`.
- **KV holds one global lock across pushes.** Per-key versioning if it ever
  stalls under contention.

- [x] **Reader-thread reaping.** Done: readers are `std::future<void>`s and the
      accept loop drops the finished ones before starting a new one, so a
      flapping client no longer leaks a thread per reconnect. Shutdown clears the
      vector (a future's destructor is the join).

## Frames

- [x] Producer abandon path. Done: `shm_ring_abandon_slot` returns an
      acquired-but-unpublished slot to the free pool; `publish_frame` calls it if
      the writer throws (C++), and the Python `Ring.write` checks size before
      acquiring — so a failed write no longer leaks a slot as `WRITING`.

## Python

- [x] **`pip`-installable package.** Done: `pyproject.toml` + a `setup.py` that
      exists only to declare the C ring as an extension, so a wheel carries a
      compiled `chappe._shm_ring` and frames work without a `make` step. Not on
      PyPI — releases attach an sdist and a wheel to a GitHub tag, and the wheel
      is tagged for the exact interpreter and glibc that built it, so the sdist
      is the portable artifact. Publishing to PyPI would need a manylinux build
      (`cibuildwheel`); add that if anyone outside the fleet wants it.

An `asyncio` client variant is not planned; the threaded/blocking one is what
this needs.

## Housekeeping

- [x] LICENSE (MIT) + CHANGELOG for v1.0.0. Tag `v1.0.0` on the release commit.
- [x] **v2.0.0 released.** Tagged on the rename/packaging commit; the `release`
      workflow checked the tag against the version in the tree and attached the
      sdist and wheel. Repo renamed to `chappe` — the old URL still redirects,
      but `git remote set-url` is worth doing on each existing clone.
