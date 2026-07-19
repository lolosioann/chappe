# TODO

Deferred work, roughly by priority. These are v1 scope cuts, not bugs — the
current build is coherent and tested. Add each only when real use asks for it.

## Resilience (do this first if anything stays running)

- [x] **Client reconnect + resubscribe.** Done: on a dropped connection the
      reader thread (C++ `Node` and Python `Node`) reconnects to the same
      address with exponential backoff and re-sends every `SUBSCRIBE`; publishes
      during the gap are dropped, and get/sync don't block on a lost request.
      The KV cache is invalidated on reconnect (next get() cold-fetches and
      re-watches). Remaining: a bounded replay buffer so a reconnected node also
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
- [ ] **Access control.** No auth on the socket — any local process can connect.

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
- [ ] **Atomic KV ops** — `incr`, `setnx`/compare-and-swap. Our `set` is a blind
      last-writer-wins overwrite, so nodes can't do counters, locks, or barriers.
      The daemon already serializes the KV under one lock, so these are a few
      verbs, not a redesign. Unlocks leader election / sequencing / presence.
- [ ] **Key TTL / expiry** (Redis `EXPIRE`). Turns the KV into a presence/
      heartbeat mechanism (write a key with a 2 s TTL; its absence = node gone)
      and bounds unbounded growth. Lazy + periodic sweep on the daemon.
- [ ] **Introspection** (Redis `INFO`/`CLIENT LIST`). A read-only status frame:
      connected clients, per-topic subscriber counts, message/drop counters.
      A handful of lines; turns the daemon from a black box into something
      debuggable.

Already tracked elsewhere as Redis analogues: pattern pub/sub = `PSUBSCRIBE`
(see *Topic wildcards*), socket auth = Redis `ACL` (see *Access control*),
durability/HA (see *Resilience*, but mostly out of scope per above).

## Hardening

- [x] **Frame-size cap.** Done: `MAX_FRAME_BYTES` (64 MB) in `transport.hpp`;
      an oversized/garbage length drops that one connection instead of letting
      the reader `resize()` to gigabytes and take the daemon down with a
      `bad_alloc`.
- [ ] **Access control** — no auth on the socket (also under *Routing*); any
      local process can connect, subscribe to anything, and overwrite any key.

## Daemon ceilings (`broker_server.hpp`, marked `ponytail:`)

- [ ] Thread-per-client + global locks. Fine for tens of nodes; move to epoll +
      sharded locks only if client count / fan-out throughput demands it.
- [ ] Finished client reader threads linger in `reader_threads_` until daemon
      shutdown. Now that clients reconnect, a node that flaps accumulates dead
      threads on the daemon — add reaping (join finished ones on accept) before
      this runs long-lived with unstable clients.
- [ ] KV: single global lock held across pushes. Add per-key versioning if it
      ever stalls under contention.

## Frames

- [ ] Producer has no abandon path: if a `publish_frame` writer throws mid-write,
      the slot stays `WRITING` and is permanently skipped (`node.hpp`,
      `src/shm_ring.c`). Add an abandon/reset for the write slot.

## Python

- [ ] `pip`-installable package (`pyproject.toml` / wheel).
- [ ] `asyncio` client variant (current one is threaded/blocking).

## Housekeeping

- [ ] CHANGELOG + tag a v1 release.
