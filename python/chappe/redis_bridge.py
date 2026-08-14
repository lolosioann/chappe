#!/usr/bin/env python3
"""Mirror a broker bus into Redis, so a monitoring app that speaks Redis can see
the system's state and publish into it.

    nodes <--unix--> chappe_daemon <--unix--> redis_bridge <--tcp--> redis <--> monitor

What crosses, and why:

  kv      ours -> Redis only. The monitor views state, it does not write keys, so
          nothing mirrors back and there is no loop to break. Values are copied
          byte for byte — a counter written by incr stays 8 native-endian bytes
          (struct "=q"), not a decimal string, so it still reads back through
          get<int64_t>(). Redis INCR will not work on it; unpack it instead.
  topics  both ways.
  frames  never. A FrameHandle names a shared-memory segment on this host, so it
          means nothing anywhere else. Don't put a frame topic in --out.

Keys have to be named explicitly with --key: the store has no enumeration and no
prefix-watch — a client starts watching a key by getting it — so the bridge can
only mirror keys it is told about.

    python3 python/redis_bridge.py --key state/mode --key state/gear \\
                                   --out 'telemetry/*' --in 'cmd/*'

Needs redis-py (`pip install redis`); redis-server itself is not our dependency.
"""
import argparse
import os
import sys
import threading
import time
from collections import Counter
from fnmatch import fnmatchcase

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from chappe import Node, default_addr

try:
    import redis
except ImportError:
    sys.exit("redis_bridge needs redis-py: pip install redis")

_UNSEEN = object()  # distinct from None, which is a real "key is absent"


class Bridge:
    def __init__(self, node, r, prefix, poll_s, inbound):
        self.node, self.r, self.prefix, self.poll_s = node, r, prefix, poll_s
        self._inbound = inbound         # redis glob patterns, already prefixed
        self._echo = Counter()          # (channel, payload) we published to Redis
        self._echo_lock = threading.Lock()
        self._running = True
        self.failure = None             # set if we stopped because Redis died

    # ---- ours -> Redis -----------------------------------------------------

    def _to_redis(self, topic, payload):
        ch = self.prefix + topic
        # Redis delivers a publish back to us as well, so remember it — but only
        # when some --in pattern covers the channel, or nothing would ever
        # consume the entry and the counter would grow forever. Our own bus
        # needs no such care: route_publish is noLocal, so a publish by this
        # node never comes back to it.
        if any(fnmatchcase(ch, p) for p in self._inbound):
            with self._echo_lock:
                self._echo[(ch, payload)] += 1
        self.r.publish(ch, payload)

    # ---- Redis -> ours -----------------------------------------------------

    def _from_redis(self, ch, payload):
        with self._echo_lock:
            if self._echo[(ch, payload)]:
                self._echo[(ch, payload)] -= 1  # our own message, round it off
                if not self._echo[(ch, payload)]:
                    del self._echo[(ch, payload)]
                return
        if ch.startswith(self.prefix):
            self.node.publish(ch[len(self.prefix):], payload)

    def _redis_reader(self, ps):
        try:
            for msg in ps.listen():
                if not self._running:
                    return
                if msg.get("type") == "pmessage":
                    self._from_redis(msg["channel"].decode(), msg["data"])
        except Exception as e:  # Redis went away, or ps was closed under us
            if self._running:
                self.failure = f"redis link lost: {e}"
        finally:
            # ponytail: no reconnect to Redis. Losing it stops the bridge and a
            # supervisor (systemd Restart=always) brings it back — unlike the
            # bus link, which the Node client reconnects on its own. Add a retry
            # loop here if running unsupervised ever becomes a requirement.
            self._running = False

    # ---- kv ----------------------------------------------------------------

    def _mirror_keys(self, keys):
        """Poll rather than hook the client: the first get() starts the watch and
        every later one reads the local cache the daemon keeps fresh, so this
        costs no round-trip. Latency is one poll interval, which is what a
        monitor wants and not worth a callback API for."""
        last = {}
        for k in keys:
            self.node.get(k)  # cold read: registers the watch
        while self._running:
            for k in keys:
                v = self.node.get(k)
                if v == last.get(k, _UNSEEN):
                    continue
                last[k] = v
                if v is None:  # deleted, or a ttl expired and was pushed to us
                    self.r.delete(self.prefix + k)
                else:
                    self.r.set(self.prefix + k, v)
            time.sleep(self.poll_s)

    # ---- lifecycle ---------------------------------------------------------

    def run(self, out_patterns, keys):
        for pat in out_patterns:
            self.node.subscribe_pattern(pat, self._to_redis)
        self.node.sync()
        ps = None
        if self._inbound:
            ps = self.r.pubsub(ignore_subscribe_messages=True)
            ps.psubscribe(*self._inbound)
            threading.Thread(target=self._redis_reader, args=(ps,),
                             daemon=True).start()
        try:
            self._mirror_keys(keys)
        finally:
            self._running = False
            if ps is not None:
                ps.close()


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--socket", default=default_addr())
    p.add_argument("--redis-host", default="127.0.0.1")
    p.add_argument("--redis-port", type=int, default=6379)
    p.add_argument("--redis-db", type=int, default=0)
    p.add_argument("--prefix", default="chappe:",
                   help="namespace for mirrored keys and channels in Redis")
    p.add_argument("--key", action="append", default=[],
                   help="kv key mirrored out to Redis (repeatable)")
    p.add_argument("--out", action="append", default=[],
                   help="topic pattern mirrored ours->Redis (repeatable)")
    p.add_argument("--in", dest="inbound", action="append", default=[],
                   help="channel pattern mirrored Redis->ours, redis glob "
                        "syntax, unprefixed (repeatable)")
    p.add_argument("--poll-ms", type=int, default=50,
                   help="how often mirrored keys are checked (local reads)")
    args = p.parse_args(argv)

    if not (args.key or args.out or args.inbound):
        p.error("nothing to mirror: pass at least one of --key / --out / --in")

    r = redis.Redis(host=args.redis_host, port=args.redis_port, db=args.redis_db)
    r.ping()  # fail here with a clear error, not later inside a handler
    with Node("redis-bridge") as node:
        node.connect(args.socket)
        bridge = Bridge(node, r, args.prefix, args.poll_ms / 1000.0,
                        [args.prefix + p for p in args.inbound])
        print("bridging %d key(s), out=%s in=%s -> redis %s:%d prefix %r"
              % (len(args.key), args.out, args.inbound, args.redis_host,
                 args.redis_port, args.prefix), flush=True)
        try:
            bridge.run(args.out, args.key)
        except KeyboardInterrupt:
            return 0
    if bridge.failure:  # non-zero so a supervisor restarts us
        print(bridge.failure, file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
