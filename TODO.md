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
      Remaining: a bounded replay buffer so a reconnected node also
      gets messages it *missed* while gone, not just resumed live delivery (see
      the Streams item under "Borrow from Redis").
- [x] **Startup ordering.** ~~A publish sent before its subscriber registers is
      dropped.~~ Done: opt-in retained messages — `publish(msg, retain=true)`
      stores a last-value the daemon replays on subscribe (MQTT-style). State
      topics use it; event/data streams stay non-retained. `sync()` remains for
      deterministic test ordering.

## Transport reach

- [ ] **TCP transport** for cross-host. Only `unix_connect`/`unix_listen` in
      `include/ipc/transport.hpp` are socket-family-specific — swap those.
- [ ] **Endianness.** Wire u32s and POD payloads are native-endian
      (same-host/same-arch assumption). Move to network order before spanning
      architectures.

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
      `BrokerServer(path, {uids...})` swaps the default same-uid rule for an
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

- [~] **Streams-style retained / replayable delivery** (highest value). Redis
      Streams keep an append-only log with replay-from-ID and consumer-group
      acks; classic pub/sub (ours) is fire-and-forget. **Partly done:** opt-in
      last-value retention (`publish(msg, retain=true)`) replays the current
      value on subscribe. Remaining: a bounded per-topic ring of *recent*
      messages (replay-N-back, not just last), which covers the reconnect
      replay case (a reattached node gets what it missed, not only the latest).
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

## Daemon ceilings (`broker_server.hpp`, marked `ponytail:`)

- [ ] Thread-per-client + global locks. Fine for tens of nodes; move to epoll +
      sharded locks only if client count / fan-out throughput demands it.
- [x] **Reader-thread reaping.** Done: readers are `std::future<void>`s and the
      accept loop drops the finished ones before starting a new one, so a
      flapping client no longer leaks a thread per reconnect. Shutdown clears the
      vector (a future's destructor is the join).
- [ ] Slow-consumer handling is a *bound*, not a design: a 2 s `SO_SNDTIMEO` on
      client sockets plus drop-on-write-failure keeps one wedged consumer from
      stalling the daemon forever, but a slow-but-draining one still holds the KV
      lock for the length of its writes. The upgrade path is a per-client outbox
      thread with a bounded queue and an explicit drop policy — then no consumer
      can delay a `set` fan-out at all.
- [ ] KV: single global lock held across pushes. Add per-key versioning if it
      ever stalls under contention.

## Frames

- [x] Producer abandon path. Done: `shm_ring_abandon_slot` returns an
      acquired-but-unpublished slot to the free pool; `publish_frame` calls it if
      the writer throws (C++), and the Python `Ring.write` checks size before
      acquiring — so a failed write no longer leaks a slot as `WRITING`.

## Python

- [ ] `pip`-installable package (`pyproject.toml` / wheel).
- [ ] `asyncio` client variant (current one is threaded/blocking).

## Housekeeping

- [x] LICENSE (MIT) + CHANGELOG for v1.0.0. Tag `v1.0.0` on the release commit.
