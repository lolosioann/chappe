# Benchmarks

Snapshot taken 2026-07-19. Numbers are **relative, not absolute** — they depend
on the machine, and everything here is loopback on a single host (best case; a
real cross-process/cross-host path adds scheduling and copies). Use them to
catch regressions and to compare transports/languages, not as headline specs.

### Environment

| | |
|---|---|
| CPU | AMD Ryzen 7 4700U (8 logical cores) |
| RAM | 14 GiB |
| Kernel | Linux 7.1.2 |
| Compiler | g++ 16.1.1, `-O2 -std=c++17` |
| Python | 3.14.6 |

### Reproduce

```sh
make bench_shm_ring                                  # raw shm ring
make bench_chappe                                    # C++ broker layer
make daemon libshm_ring && python3 python/bench.py   # Python client
```

---

## Raw shm ring (`bench_shm_ring`)

Single-producer/multi-consumer broadcast buffer, the layer under frames.

**Throughput** — single producer, no consumers (per-frame overhead → memcpy-bound):

| payload | Mframes/s | GB/s | ns/frame |
|--------:|----------:|-----:|---------:|
| 64 B    | 88.9 | 5.7  | 11.3 |
| 256 B   | 64.3 | 16.5 | 15.6 |
| 1 KB    | 26.8 | 27.4 | 37.4 |
| 4 KB    | 15.3 | 62.8 | 65.3 |
| 16 KB   | 2.38 | 39.1 | 419 |
| 64 KB   | 0.57 | 37.1 | 1767 |

**Latency** — one-way publish→observe, busy-poll consumer (cross-process, CLOCK_MONOTONIC):

| min | p50 | p90 | p99 | p99.9 | max | mean |
|----:|----:|----:|----:|------:|----:|-----:|
| 320 ns | 571 ns | 581 ns | 721 ns | 922 ns | 15.1 µs | 572 ns |

**Contended** — 1 producer flooding, 4 consumers: producer 3.17 Mframes/s
(13.0 GB/s), consumers 19.7 Mreads/s aggregate.

---

## Broker layer — pub/sub + kv

Same metrics for C++ and the Python client, over a real unix socket through the
daemon. C++ iterates more (lower variance); both measure the same paths. C++
figures are medians over several runs (this is a laptop — noisy).

| metric | C++ | Python |
|---|---:|---:|
| pub/sub RTT — p50 | 27 µs | 70.4 µs |
| pub/sub throughput (32 B msgs) | 752 k msg/s | 72 k msg/s |
| kv get **cold** (round-trip) — p50 | 18 µs | 53.3 µs |
| kv get **warm** (cache) — p50 | 0.12 µs | 0.95 µs |
| kv set throughput | 536 k/s | 73 k/s |

The warm/cold gap (~150× C++, ~55× Python) is the read-through cache doing its
job: a cached read never touches the socket. Python runs ~2–4× slower on these
paths — per-message interpreter overhead.

**Buffered frame reader (2026-07-19).** The daemon originally did one `recv()`
per frame field (5 syscalls/frame); a `FrameReader` now recv()s into a chunk and
parses frames out of it (~1 syscall/frame). C++ pub/sub throughput went 317 k →
752 k msg/s (~2.4×), kv set 290 k → 536 k (~1.8×), pub/sub RTT p50 36 → 27 µs.
Python barely moved — it's interpreter-bound, not syscall-bound — but the daemon
serving it is cheaper. Frame throughput is unchanged (pixels never crossed the
socket). Numbers above are post-change.

---

## Frames

Pixels move producer→consumer through the shm ring; only the 24-byte FrameHandle
crosses the broker. So throughput tracks shm memcpy bandwidth, not the protocol.

| frame | size | C++ | Python |
|---|---:|---:|---:|
| 640×480 gray | 300 KB | 69.9 k fps · 21.5 GB/s | 24.1 k fps · 7.4 GB/s |
| 1920×1080 gray | 2025 KB | 3.6 k fps · 7.4 GB/s | 3.4 k fps · 7.2 GB/s |

The cache cliff is visible: 300 KB working sets stay in L3 (~21 GB/s), 2 MB ones
go to RAM (~7 GB/s). Python closes to ~1.05× of C++ at 1080p because the copy is
a C `memmove` either way; the gap at 300 KB is per-frame ctypes call overhead.

> Note: an early Python frame result was ~200× slower — `ring.write` used ctypes
> slice-assignment (element-by-element). Fixed to `ctypes.memmove`; the numbers
> above are post-fix.

---

## Comparison to other brokers

These systems aren't the same shape, so most raw-number comparisons mislead.
Only **Redis** is a close enough analog (central daemon, unix socket, SET/GET ≈
our KV, PUBLISH/SUBSCRIBE ≈ our pub/sub) to measure head-to-head — the rest are
positioned by architecture.

### Redis 7.2 — measured on this machine

Same box, unix socket, single client, no pipelining, 4-byte values, no
persistence — matched to our KV bench (`redis-server --unixsocket … --save '' --appendonly no`,
`redis-benchmark -c 1 -P 1 -d 4`).

| path | ours | Redis 7.2 |
|---|---:|---:|
| single in-memory round trip, p50 | 18 µs (kv get cold) | 15 µs (GET) |
| sequential read throughput (1 client) | ~55 k/s | 63 k/s |
| write, single client | 536 k/s ¹ | 60 k/s (SET, ack'd) |
| write, batched/pipelined | — | 379 k/s (SET `-P 16`) |
| repeated read of the same key | 0.12 µs ² | 15 µs |

¹ Our `set` is **fire-and-forget** — no ack, no durability. So it's a *send*
rate: comparable to Redis's *pipelined* 379 k/s (it now exceeds it), **not** its
synchronous 60 k/s. Add an ack and it drops toward Redis's synchronous number.
Not a like-for-like win.

² Client-side read-through cache. Redis's equivalent is client-side caching
(RESP3 tracking) — the same watch-and-invalidate idea — which would land in the
same range. Without it, Redis always round-trips.

**Takeaway:** on the one genuinely comparable path — a single in-memory round
trip — we're within ~25 % of Redis (19 vs 15 µs). That's a fair place to sit
next to a mature single-threaded C server (ours is thread-per-client C++ with
more per-message allocation). Redis wins on rich types, durability, replication,
and years of ops hardening; we're leaner and add typed messages + zero-copy shm
frames it doesn't have.

### Same-methodology Python shootout (measured on this machine)

One harness (`python/bench_compare.py`) drives our broker, Redis (`redis-py` +
`hiredis`), and Mosquitto (`paho-mqtt`) through identical Python timing code —
each launched locally, in-memory. So every broker is measured end-to-end
**through its own Python client**.

| metric | this broker | Redis (redis-py) | MQTT (paho) |
|---|---:|---:|---:|
| pub/sub RTT p50 | 79.6 µs | 765.8 µs | 722.3 µs |
| pub/sub throughput | 70.8 k msg/s | 4.5 k msg/s | 18.8 k msg/s |
| kv get RTT p50 | 52.8 µs | 180.9 µs | — |
| kv set throughput | 158.9 k/s ¹ | 5.1 k/s | — |

**Read this carefully — it is not "our broker beats Redis."** It measures the
*whole Python stack*, and our client is ~250 lines of hand-rolled protocol while
`redis-py`/`paho` are full-featured libraries. The gap is mostly **client
overhead**, not broker speed. The proof is right above: the C `redis-benchmark`
does GET in **15 µs**, but `redis-py` does the same GET in **181 µs** — ~12×
client overhead, unchanged by `hiredis`. Redis's pub/sub number is worst-hit
because `redis-py`'s `listen()` path isn't its optimized route.

So the two lenses:

- **Server ceiling (C client → server):** Redis GET 15 µs, ours (C++) 18 µs —
  **Redis's server is still a touch faster**, and far more capable.
- **End-to-end in Python (each project's own client):** ours is lighter, so it
  comes out ahead — a statement about *client libraries*, not brokers.

Honest conclusion: our broker sits in the same performance class as a mature
in-memory store; it isn't faster than Redis at the server level. What this repo
has that Redis doesn't is typed messages and zero-copy shm frames; what Redis
has that this doesn't is durability, rich types, replication, and a decade of
ops hardening.

> Reproduce: `pip install redis "paho-mqtt<2" hiredis` in a venv, then
> `<venv>/bin/python python/bench_compare.py` (needs `redis-server` + `mosquitto`
> on PATH). Not wired into `make` — it depends on external brokers.

### Where each system sits (architecture, not measured)

| system | model | transport | typed msgs | zero-copy frames | store | closest to us |
|---|---|---|---|---|---|---|
| **this broker** | central daemon | unix sock + shm | yes (C++ compile-time) | yes (shm ring) | last-value KV | — |
| Redis | central daemon | TCP / unix | no | no | rich data types | pub/sub + KV |
| MQTT (Mosquitto) | central broker | TCP | no | no | retained msgs | pub/sub + QoS |
| ZeroMQ / NNG | brokerless lib | tcp/ipc/inproc | no | no | none | the transport itself |
| DDS / ROS 2 | brokerless (discovery) | UDP / shm | yes (IDL) | yes (shm xport) | none | robotics pub/sub |
| iceoryx | shm daemon (RouDi) | shm | yes | yes (true zero-copy) | none | our frame transport |

Latency *class* (order-of-magnitude, published / hardware-dependent — **not**
measured here except Redis and ours):

- **shm zero-copy** (ours 0.6 µs raw ring; iceoryx ~1 µs; DDS-shm single-digit µs) — same regime.
- **local socket round trip** (ours 18 µs one-hop / 27 µs pub/sub two-hop; Redis 15 µs; ZeroMQ ipc/inproc ~5–30 µs) — same regime.
- **TCP broker with QoS** (MQTT ~100 µs–1 ms) — a different, heavier class; buys network reach + delivery guarantees we don't offer.

Two honest gaps this makes obvious: we have **no durability/QoS** (MQTT/Redis do)
and **no cross-host transport** (everything is same-host) — see `TODO.md`.
